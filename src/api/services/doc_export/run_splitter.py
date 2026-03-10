"""Run-level highlighting for python-docx paragraphs."""

import logging
from copy import deepcopy
from dataclasses import dataclass

from docx.enum.text import WD_COLOR_INDEX
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.paragraph import Paragraph
from docx.text.run import Run as DocxRun

from ._dataclasses import HighlightRegion


log = logging.getLogger(__name__)

# Priority — higher value wins on overlap (conservative: Partial > everything)
COLOR_PRIORITY = {
    WD_COLOR_INDEX.BRIGHT_GREEN: 1,
    WD_COLOR_INDEX.TURQUOISE:    2,
    WD_COLOR_INDEX.YELLOW:       3,
    WD_COLOR_INDEX.PINK:         4,
    WD_COLOR_INDEX.RED:          5,
}


@dataclass
class _RunSlice:
    """A character range with original formatting XML and optional highlight."""

    text:        str
    source_rpr:  object | None         = None
    highlight:   WD_COLOR_INDEX | None = None


class RunSplitter:
    """Splits and highlights runs within a python-docx paragraph at character boundaries."""

    def apply_highlights(self, paragraph: Paragraph, regions: list[HighlightRegion]) -> int:
        """Rebuild paragraph runs with highlight colors applied. Returns count of highlighted chars."""

        if not regions:

            return 0

        para_text = paragraph.text
        color_map = self._build_color_map(len(para_text), regions)

        if not color_map:

            return 0

        slices      = self._build_slices(paragraph, color_map)
        highlighted = self._rebuild_paragraph(paragraph, slices)

        return highlighted


    def _build_color_map(self, para_len: int, regions: list[HighlightRegion]) -> dict[int, WD_COLOR_INDEX]:
        """Build char position -> color mapping with priority-based overlap resolution."""

        color_map = {}

        sorted_regions = sorted(regions, key=lambda r: COLOR_PRIORITY.get(r.color, 0))

        for region in sorted_regions:
            start = max(0, region.start_char)
            end   = min(para_len, region.end_char)

            for i in range(start, end):
                color_map[i] = region.color

        return color_map


    def _build_slices(self, paragraph: Paragraph, color_map: dict[int, WD_COLOR_INDEX]) -> list[_RunSlice]:
        """Split runs at color boundaries, preserving original formatting XML."""

        slices  = []
        cum_pos = 0

        for run in paragraph.runs:
            text = run.text or ''
            if not text:
                continue

            # Deep copy the original rPr (run properties) XML — preserves ALL formatting
            orig_rpr = run._element.find(qn('w:rPr'))
            rpr_copy = deepcopy(orig_rpr) if orig_rpr is not None else None

            # Walk chars in this run and split where color changes
            cur_chars = []
            cur_color = color_map.get(cum_pos)

            for offset, char in enumerate(text):
                char_pos = cum_pos + offset
                color    = color_map.get(char_pos)

                if color != cur_color and cur_chars:
                    slices.append(_RunSlice(
                        text       = ''.join(cur_chars),
                        source_rpr = deepcopy(rpr_copy) if rpr_copy is not None else None,
                        highlight  = cur_color,
                    ))
                    cur_chars = []
                    cur_color = color

                cur_chars.append(char)

            if cur_chars:
                slices.append(_RunSlice(
                    text       = ''.join(cur_chars),
                    source_rpr = deepcopy(rpr_copy) if rpr_copy is not None else None,
                    highlight  = cur_color,
                ))

            cum_pos += len(text)

        return slices


    def _rebuild_paragraph(self, paragraph: Paragraph, slices: list[_RunSlice]) -> int:
        """Replace paragraph runs with slices. Returns count of highlighted chars."""

        p_elem = paragraph._element

        # Remove existing runs
        for r_elem in p_elem.findall(qn('w:r')):
            p_elem.remove(r_elem)

        highlighted = 0

        for sl in slices:
            # Build new w:r element with original rPr
            r_elem = OxmlElement('w:r')

            if sl.source_rpr is not None:
                r_elem.append(deepcopy(sl.source_rpr))

            # Add text element with whitespace preservation
            t_elem      = OxmlElement('w:t')
            t_elem.text = sl.text

            if sl.text and (sl.text[0] == ' ' or sl.text[-1] == ' '):
                t_elem.set('{http://www.w3.org/XML/1998/namespace}space', 'preserve')

            r_elem.append(t_elem)
            p_elem.append(r_elem)

            # Use python-docx Run wrapper to set highlight (correct XML mapping)
            if sl.highlight is not None:
                doc_run = DocxRun(r_elem, p_elem)
                doc_run.font.highlight_color = sl.highlight
                highlighted += len(sl.text)

        return highlighted
