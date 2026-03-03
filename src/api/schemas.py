"""Pydantic response models — serialisation layer for engine dataclasses."""

from datetime import datetime

from pydantic import BaseModel


# ── Extraction ───────────────────────────────────────────────────────

class ExtractionSummaryOut(BaseModel):

    total_sections:     int
    total_items:        int
    method:             str
    section_labels:     list[str]
    processing_time_ms: int = 0


# ── Classification ───────────────────────────────────────────────────

class ClassificationResultOut(BaseModel):

    extraction_id: int
    category_id:   int
    confidence:    float
    reasoning:     str | None = None


class ClassificationSummaryOut(BaseModel):

    session_id:       int
    total_extracted:  int
    total_classified: int
    total_failed:     int
    batches_sent:     int
    batches_failed:   int
    avg_confidence:   float
    low_confidence:   list[ClassificationResultOut] = []


# ── Comparison ───────────────────────────────────────────────────────

class MatchResultOut(BaseModel):

    erector_extraction_id: int
    category_id:           int
    mfc_ids:               list[int]
    match_type:            str
    confidence:            float
    risk_level:            str | None = None
    risk_notes:            str | None = None
    reasoning:             str | None = None


class ComparisonSummaryOut(BaseModel):

    session_id:          int
    total_erector:       int
    total_mfc:           int
    total_aligned:       int
    total_partial:       int
    total_erector_only:  int
    total_mfc_only:      int
    categories_compared: int
    avg_confidence:      float
    processing_time_ms:  int              = 0
    high_risk_items:     list[MatchResultOut] = []


# ── Engine result ────────────────────────────────────────────────────

class AnalysisResultOut(BaseModel):

    session_id:         int
    source_file:        str
    status:             str
    erector_name:       str | None                    = None
    erector_id:         int | None                    = None
    job_number:         str | None                    = None
    job_name:           str | None                    = None
    error_message:      str | None                    = None
    extraction:         ExtractionSummaryOut | None   = None
    classification:     ClassificationSummaryOut | None = None
    comparison:         ComparisonSummaryOut | None   = None
    high_risk_items:    list[MatchResultOut]           = []
    processing_time_ms: int                            = 0


# ── Session list ─────────────────────────────────────────────────────

class SessionListItem(BaseModel):

    id:                int
    erector_name_raw:  str | None = None
    job_number:        str | None = None
    job_name:          str | None = None
    source_file_name:  str | None = None
    status:            str | None = None
    total_extracted:   int | None = None
    total_classified:  int | None = None
    total_aligned:     int | None = None
    total_erector_only: int | None = None
    total_mfc_only:    int | None = None
    total_partial:     int | None = None
    total_high_risk:   int | None = None
    created_at:        datetime | None = None
    completed_at:      datetime | None = None


class SessionListResponse(BaseModel):

    sessions: list[SessionListItem]
    count:    int


# ── Match row (DB row shape) ────────────────────────────────────────

class MatchRow(BaseModel):

    id:                       int
    session_id:               int
    extracted_exclusion_id:   int | None = None
    mfc_exclusion_id:         int | None = None
    category_id:              int | None = None
    match_type:               str | None = None
    confidence:               float | None = None
    ai_reasoning:             str | None = None
    risk_level:               str | None = None
    risk_notes:               str | None = None
    erector_text:             str | None = None
    mfc_text:                 str | None = None
    mfc_item_type:            str | None = None


class MatchListResponse(BaseModel):

    session_id: int
    matches:    list[MatchRow]
    count:      int


# ── Analyze request ──────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    """

    Body for POST /api/analyze.
    Provide network_path for server-side file access, or use file upload.
    """

    network_path: str | None = None
    erector_name: str | None = None
    job_number:   str | None = None
    job_name:     str | None = None
    initiated_by: str | None = None
    archive:      bool       = True


# ── Action Items ─────────────────────────────────────────────────────

class ActionItemRow(BaseModel):
    """Single action item with joined match data."""

    id:            int
    session_id:    int
    match_id:      int | None     = None
    section:       str
    status:        str
    notes:         str | None     = None
    created_at:    str | None     = None
    updated_at:    str | None     = None
    erector_text:  str | None     = None
    mfc_text:      str | None     = None
    match_type:    str | None     = None
    confidence:    float | None   = None
    risk_level:    str | None     = None
    risk_notes:    str | None     = None
    ai_reasoning:  str | None     = None
    category_id:      int | None     = None
    mfc_exclusion_id: int | None     = None
    mfc_item_type:    str | None     = None


class ActionItemSummary(BaseModel):
    """Counts by status and section."""

    total:        int
    unreviewed:   int
    acknowledged: int
    addressed:    int
    dismissed:    int
    by_section:   dict[str, int]


class ActionItemListResponse(BaseModel):
    """Response for GET session action items."""

    session_id: int
    items:      list[ActionItemRow]
    summary:    ActionItemSummary


class ActionItemUpdate(BaseModel):
    """Body for PATCH single item."""

    status: str | None = None
    notes:  str | None = None


class ActionItemBatchUpdate(BaseModel):
    """Body for PATCH batch."""

    item_ids: list[int]
    status:   str


# ── Training ─────────────────────────────────────────────────────────

class TrainingQueueItem(BaseModel):
    """Single item in the training review queue."""

    extraction_id:             int
    raw_text:                  str
    normalized_text:           str | None = None
    category_id:               int
    category_name:             str
    classification_confidence: float
    session_id:                int
    erector_name:              str | None = None
    job_number:                str | None = None
    job_name:                  str | None = None


class TrainingQueueResponse(BaseModel):
    """Paginated training queue with stats."""

    items:           list[TrainingQueueItem]
    total_pending:   int
    total_verified:  int
    total_overridden: int
    max_confidence:  float


class TrainingVerification(BaseModel):
    """Body for submitting a verification or correction."""

    extraction_id: int
    category_id:   int
    verified_by:   str | None = None


class TrainingStatsResponse(BaseModel):
    """Overview stats for the training system."""

    total_verified:   int
    total_overridden: int
    total_pending:    int
    accuracy_rate:    float | None = None
