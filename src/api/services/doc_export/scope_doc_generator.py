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

    def _build_filename(self, session: dict) -> str:
        """Build output filename from session metadata."""

        job_number   = session.get('JobNumber') or 'NoJob'
        erector_name = session.get('ErectorNameRaw') or 'Unknown'

        # Sanitize for filesystem
        safe_job     = re.sub(r'[^\w\-]', '_', job_number.strip())
        safe_erector = re.sub(r'[^\w\-]', '_', erector_name.strip())

        return f"{safe_job}_{safe_erector}_Scope_Analysis.docx"
