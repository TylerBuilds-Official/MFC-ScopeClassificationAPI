"""Generates annotated scope letter .docx files from session match results."""

import io
import logging
import re
from collections import defaultdict
from pathlib import Path

from docx import Document
from docx.enum.text import WD_COLOR_INDEX

from scope_classification import ConnectionFactory

from ._dataclasses import HighlightRegion
from .run_splitter import RunSplitter


log = logging.getLogger(__name__)

# ── Production colors (by match type) ────────────────────────────────
ALIGNED_COLOR = WD_COLOR_INDEX.BRIGHT_GREEN
PARTIAL_COLOR = WD_COLOR_INDEX.YELLOW

# FIXME: dev-only — low confidence mapping indicator, remove before release
LOW_CONFIDENCE_COLOR    = WD_COLOR_INDEX.TURQUOISE
LOW_CONFIDENCE_THRESHOLD = 0.60

# ── Verification colors (by match method) ────────────────────────────
VERIFY_EXACT_COLOR  = WD_COLOR_INDEX.BRIGHT_GREEN
VERIFY_FUZZY_COLOR  = WD_COLOR_INDEX.YELLOW
VERIFY_MANUAL_COLOR = WD_COLOR_INDEX.PINK


class ScopeDocGenerator:
    """Generates highlighted scope letter documents from analysis results."""

    def __init__(self, db: ConnectionFactory, template_path: str) -> None:
        """Initialize with DB connection and path to template .docx."""

        self._db            = db
        self._schema        = db.schema
        self._template_path = template_path
        self._splitter      = RunSplitter()


    def generate_session_doc(self, session_id: int) -> tuple[bytes, str]:
        """Generate highlighted scope letter for a completed session. Returns (docx_bytes, filename)."""

        session  = self._load_session(session_id)
        matches  = self._load_session_matches(session_id)
        mappings = self._load_template_mappings()

        # Build MfcExclusionId -> match type lookup
        match_lookup = {}
        for m in matches:
            mfc_id     = m['MfcExclusionId']
            match_type = m['MatchType']
            if mfc_id and match_type in ('Aligned', 'Partial'):
                # Conservative: Partial wins if same MFC item matched multiple ways
                existing = match_lookup.get(mfc_id)
                if existing == 'Partial' or match_type == 'Partial':
                    match_lookup[mfc_id] = 'Partial'
                else:
                    match_lookup[mfc_id] = 'Aligned'

        # Build mapping lookup: MfcExclusionId -> mapping row
        mapping_lookup = {m['MfcExclusionId']: m for m in mappings}

        # Build highlight regions grouped by paragraph index
        regions_by_para = defaultdict(list)

        for mfc_id, match_type in match_lookup.items():
            mapping = mapping_lookup.get(mfc_id)
            if not mapping:
                log.warning(f"  MFC exclusion {mfc_id}: matched as {match_type} but no template mapping exists")
                continue

            # Determine color
            if match_type == 'Partial':
                color = PARTIAL_COLOR
            elif float(mapping['MatchConfidence']) < LOW_CONFIDENCE_THRESHOLD:
                # FIXME: dev-only blue highlight for low-confidence mappings
                color = LOW_CONFIDENCE_COLOR
            else:
                color = ALIGNED_COLOR

            region = HighlightRegion(
                start_char       = mapping['StartChar'],
                end_char         = mapping['EndChar'],
                color            = color,
                mfc_exclusion_id = mfc_id,
            )

            regions_by_para[mapping['ParaIndex']].append(region)

        # Load template and apply highlights
        doc       = Document(self._template_path)
        total_hl  = 0

        for para_idx, regions in regions_by_para.items():
            if para_idx >= len(doc.paragraphs):
                log.warning(f"  Para index {para_idx} out of range (doc has {len(doc.paragraphs)} paragraphs)")
                continue

            paragraph = doc.paragraphs[para_idx]
            applied   = self._splitter.apply_highlights(paragraph, regions)
            total_hl += applied

        log.info(f"Session {session_id}: highlighted {total_hl} runs across {len(regions_by_para)} paragraphs")

        # Build filename
        filename = self._build_filename(session)

        # Save to bytes
        buf = io.BytesIO()
        doc.save(buf)
        buf.seek(0)

        return buf.getvalue(), filename


    def generate_verification_doc(self) -> bytes:
        """
        DEV: Verify template mapping positions.

        Highlights all mapped exclusions colored by match method:
        Green = Exact, Yellow = Fuzzy, Red = Manual, Blue = low confidence.
        This method is used to verify template match locations/output.
        """

        mappings = self._load_template_mappings()

        log.info(f"Verification: loaded {len(mappings)} template mappings")

        regions_by_para = defaultdict(list)

        for mapping in mappings:
            method     = mapping['MatchMethod']
            confidence = float(mapping['MatchConfidence'])

            # FIXME: dev-only — blue for low confidence regardless of method
            if confidence < LOW_CONFIDENCE_THRESHOLD:
                color = LOW_CONFIDENCE_COLOR
            elif method == 'Exact':
                color = VERIFY_EXACT_COLOR
            elif method == 'Fuzzy':
                color = VERIFY_FUZZY_COLOR
            else:
                color = VERIFY_MANUAL_COLOR

            region = HighlightRegion(
                start_char       = mapping['StartChar'],
                end_char         = mapping['EndChar'],
                color            = color,
                mfc_exclusion_id = mapping['MfcExclusionId'],
            )

            regions_by_para[mapping['ParaIndex']].append(region)

        log.info(f"Verification: {len(regions_by_para)} paragraphs to highlight")

        doc      = Document(self._template_path)
        total_hl = 0

        for para_idx, regions in regions_by_para.items():
            if para_idx >= len(doc.paragraphs):
                log.warning(f"  Verify: para index {para_idx} out of range (doc has {len(doc.paragraphs)})")
                continue

            paragraph = doc.paragraphs[para_idx]
            para_text = paragraph.text
            log.debug(f"  Para {para_idx}: {len(regions)} regions, text length {len(para_text)}")

            applied   = self._splitter.apply_highlights(paragraph, regions)
            total_hl += applied

        log.info(f"Verification doc: highlighted {total_hl} chars across {len(regions_by_para)} paragraphs")

        buf = io.BytesIO()
        doc.save(buf)
        buf.seek(0)

        return buf.getvalue()


    def build_editor_data(self, session_id: int) -> dict:
        """Build structured JSON for the browser scope letter editor."""

        session  = self._load_session(session_id)
        matches  = self._load_session_matches_full(session_id)
        mappings = self._load_template_mappings()

        # Build MfcExclusionId -> best match row
        match_lookup: dict[int, dict] = {}
        for m in matches:
            mfc_id = m['MfcExclusionId']
            if not mfc_id:
                continue
            existing = match_lookup.get(mfc_id)
            if not existing or m['MatchType'] == 'Partial':
                match_lookup[mfc_id] = m

        # Build mapping lookup keyed by para index
        mappings_by_para: dict[int, list[dict]] = defaultdict(list)
        for mp in mappings:
            mappings_by_para[mp['ParaIndex']].append(mp)

        # Load template and build paragraph data with run formatting
        doc        = Document(self._template_path)
        paragraphs = []

        for idx, para in enumerate(doc.paragraphs):
            text = para.text

            # Paragraph-level formatting
            pf         = para.paragraph_format
            indent_emu = pf.left_indent
            indent_in  = round(indent_emu / 914400, 2) if indent_emu else None

            # Build region lookup: char position -> region data
            region_map  = {}   # start_char -> region dict
            region_ends = {}   # char_pos   -> True if inside a region
            para_regions = []

            for mp in mappings_by_para.get(idx, []):
                mfc_id = mp['MfcExclusionId']
                match  = match_lookup.get(mfc_id)

                region = {
                    'mfc_id':       mfc_id,
                    'start':        mp['StartChar'],
                    'end':          mp['EndChar'],
                    'snippet':      mp['TemplateSnippet'],
                    'match_type':   match['MatchType'] if match else None,
                    'confidence':   float(match['Confidence']) if match and match['Confidence'] else None,
                    'risk_level':   match['RiskLevel'] if match else None,
                    'risk_notes':   match['RiskNotes'] if match else None,
                    'ai_reasoning': match['AiReasoning'] if match else None,
                    'erector_text': match['ErectorText'] if match else None,
                }

                region_map[mp['StartChar']] = region
                para_regions.append(region)

                for ci in range(mp['StartChar'], mp['EndChar']):
                    region_ends[ci] = region

            # Build segments by walking runs and splitting at region boundaries
            segments = []
            cum_pos  = 0

            for run in para.runs:
                run_text = run.text or ''
                if not run_text:
                    continue

                bold      = run.bold or False
                italic    = run.italic or False
                underline = run.underline or False
                size_pt   = round(run.font.size / 12700, 1) if run.font.size else None
                color_hex = str(run.font.color.rgb) if run.font.color and run.font.color.rgb else None

                # Split this run at region boundaries
                cur_chars    = []
                cur_region   = region_ends.get(cum_pos)

                for offset, char in enumerate(run_text):
                    char_pos   = cum_pos + offset
                    char_region = region_ends.get(char_pos)

                    if char_region is not cur_region and cur_chars:
                        segments.append(self._make_segment(
                            ''.join(cur_chars), bold, italic, underline, size_pt, color_hex, cur_region,
                        ))
                        cur_chars = []

                    cur_region = char_region
                    cur_chars.append(char)

                if cur_chars:
                    segments.append(self._make_segment(
                        ''.join(cur_chars), bold, italic, underline, size_pt, color_hex, cur_region,
                    ))

                cum_pos += len(run_text)

            paragraphs.append({
                'index':    idx,
                'text':     text,
                'indent':   indent_in,
                'segments': segments,
                'regions':  para_regions,
            })

        return {
            'session': {
                'id':       session.get('Id'),
                'erector':  session.get('ErectorNameRaw'),
                'job':      session.get('JobNumber'),
                'job_name': session.get('JobName'),
            },
            'paragraphs': paragraphs,
        }


    @staticmethod
    def _make_segment(
            text: str, bold: bool, italic: bool,
            underline: bool, size_pt: float | None,
            color: str | None, region: dict | None ) -> dict:
        """Build a single segment dict for the editor."""

        seg: dict = {'text': text}

        if bold:      seg['bold']      = True
        if italic:    seg['italic']    = True
        if underline: seg['underline'] = True
        if size_pt:   seg['size']      = size_pt
        if color:     seg['color']     = f'#{color}'

        if region:
            seg['region'] = {
                'mfc_id':     region['mfc_id'],
                'match_type': region['match_type'],
                'risk_level': region['risk_level'],
            }

        return seg


    # ── DB queries ───────────────────────────────────────────────────

    def _load_session(self, session_id: int) -> dict:
        """Load session metadata."""

        sql    = f"SELECT * FROM {self._schema}.AnalysisSessions WHERE Id = ?"
        cursor = self._db.execute(sql, (session_id,))
        row    = cursor.fetchone()

        if not row:
            raise ValueError(f"Session {session_id} not found")

        cols = [col[0] for col in cursor.description]

        return dict(zip(cols, row))


    def _load_session_matches(self, session_id: int) -> list[dict]:
        """Load all match results for a session."""

        sql = f"""
            SELECT MfcExclusionId, MatchType
            FROM {self._schema}.ExclusionMatches
            WHERE SessionId = ? AND MfcExclusionId IS NOT NULL
        """

        cursor = self._db.execute(sql, (session_id,))
        cols   = [col[0] for col in cursor.description]

        return [dict(zip(cols, row)) for row in cursor.fetchall()]


    def _load_session_matches_full(self, session_id: int) -> list[dict]:
        """Load match results with risk, reasoning, and erector text."""

        sql = f"""
            SELECT em.MfcExclusionId, em.MatchType, em.Confidence,
                   em.RiskLevel, em.RiskNotes, em.AiReasoning,
                   ee.RawText AS ErectorText
            FROM {self._schema}.ExclusionMatches em
            LEFT JOIN {self._schema}.ExtractedExclusions ee ON ee.Id = em.ExtractedExclusionId
            WHERE em.SessionId = ? AND em.MfcExclusionId IS NOT NULL
        """

        cursor = self._db.execute(sql, (session_id,))
        cols   = [col[0] for col in cursor.description]

        return [dict(zip(cols, row)) for row in cursor.fetchall()]


    def _load_template_mappings(self) -> list[dict]:
        """Load all template mappings."""

        sql = f"""
            SELECT MfcExclusionId, ParaIndex, StartChar, EndChar,
                   TemplateSnippet, MatchMethod, MatchConfidence
            FROM {self._schema}.TemplateMappings
            ORDER BY ParaIndex, StartChar
        """

        cursor = self._db.execute(sql)
        cols   = [col[0] for col in cursor.description]

        return [dict(zip(cols, row)) for row in cursor.fetchall()]


    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _build_filename(session: dict) -> str:
        """Build output filename from session metadata."""

        job_number   = session.get('JobNumber') or 'NoJob'
        erector_name = session.get('ErectorNameRaw') or 'Unknown'

        # Sanitize for filesystem
        safe_job     = re.sub(r'[^\w\-]', '_', job_number.strip())
        safe_erector = re.sub(r'[^\w\-]', '_', erector_name.strip())

        return f"{safe_job}_{safe_erector}_Scope_Analysis.docx"
