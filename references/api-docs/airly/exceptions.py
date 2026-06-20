"""Airly API exceptions."""

from __future__ import annotations


class AirlyError(Exception):
    """Base exception for all Airly API errors."""

    def __init__(self, message: str, status_code: int | None = None, error_code: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class AirlyAuthError(AirlyError):
    """HTTP 401 — API key missing or invalid."""


class AirlyForbiddenError(AirlyError):
    """HTTP 403 — access denied."""


class AirlyNotFoundError(AirlyError):
    """HTTP 404 — installation or location does not exist."""


class AirlyMovedError(AirlyError):
    """HTTP 301 — installation replaced; check the `location` attribute for the new URL."""

    def __init__(self, message: str, location: str | None = None):
        super().__init__(message, status_code=301)
        self.location = location


class AirlyRateLimitError(AirlyError):
    """HTTP 429 — daily or per-minute rate limit exceeded."""

    def __init__(self, message: str, remaining_day: int | None = None, limit_day: int | None = None):
        super().__init__(message, status_code=429)
        self.remaining_day = remaining_day
        self.limit_day = limit_day


class AirlyServerError(AirlyError):
    """HTTP 5xx — server-side error."""


class AirlyBadRequestError(AirlyError):
    """HTTP 400 — invalid query parameters."""

    def __init__(self, message: str, violations: list[dict] | None = None):
        super().__init__(message, status_code=400, error_code="API_REQUEST_INVALID")
        self.violations = violations or []
