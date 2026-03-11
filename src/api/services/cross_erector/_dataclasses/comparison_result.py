from dataclasses import dataclass, field


@dataclass
class ErectorCoverage:
    """Single erector's coverage of a unified item."""

    analysis_session_id:   int
    erector_name:          str
    coverage_type:         str             # 'Excludes' | 'Includes' | 'NotMentioned'
    raw_text:              str | None = None
    extracted_exclusion_id: int | None = None


@dataclass
class UnifiedItemResult:
    """Canonical scope item with coverage across all erectors."""

    canonical_description: str
    category_id:           int | None                  = None
    coverage:              list[ErectorCoverage] = field(default_factory=list)


@dataclass
class ComparisonSessionResult:
    """Full result of a cross-erector comparison."""

    comparison_session_id: int
    job_number:            str | None                   = None
    job_name:              str | None                   = None
    status:                str                          = "Pending"
    total_erectors:        int                          = 0
    total_unified:         int                          = 0
    unified_items:         list[UnifiedItemResult] = field(default_factory=list)
    processing_time_ms:    int                          = 0
    error_message:         str | None                   = None
