class AppError(Exception):
    """Base application exception for orchestration and runtime errors."""


class LegacyStoneDataError(AppError):
    """Raised when only Stone v2-era data exists and a v3 rebuild is required."""

