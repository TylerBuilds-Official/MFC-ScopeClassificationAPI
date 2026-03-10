from dataclasses import dataclass

from docx.enum.text import WD_COLOR_INDEX


@dataclass
class HighlightRegion:
    """A character range within a paragraph to highlight."""

    start_char:       int
    end_char:         int
    color:            WD_COLOR_INDEX
    mfc_exclusion_id: int | None = None
