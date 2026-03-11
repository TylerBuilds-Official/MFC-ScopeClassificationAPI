class CrossErectorComparisonError(Exception):
    """Raised when the cross-erector comparison pipeline fails."""

    def __init__(self, message: str, phase: str | None = None, comparison_id: int | None = None) -> None:
        """Initialize with message, pipeline phase, and optional comparison session ID."""

        self.phase         = phase
        self.comparison_id = comparison_id
        super().__init__(message)
