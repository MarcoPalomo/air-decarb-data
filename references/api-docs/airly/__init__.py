"""Airly API v2 Python client.

Quick start::

    from airly import AirlyClient

    with AirlyClient(api_key="YOUR_KEY") as client:
        # Nearest sensor to Paris centre
        m = client.get_nearest_measurements(lat=48.8566, lng=2.3522)
        if m.current:
            idx = m.current.get_index("AIRLY_CAQI")
            print(f"Air quality: {idx.level} ({idx.value:.0f}) — {idx.description}")
            pm25 = m.current.get_value("PM25")
            print(f"PM2.5: {pm25} µg/m³")

        # Rate limit tracking
        print(f"Requests remaining today: {client.rate_limit.remaining_day}/100")
"""

from .client import AirlyClient
from .exceptions import (
    AirlyAuthError,
    AirlyBadRequestError,
    AirlyError,
    AirlyForbiddenError,
    AirlyMovedError,
    AirlyNotFoundError,
    AirlyRateLimitError,
    AirlyServerError,
)
from .models import (
    Address,
    IndexLevel,
    IndexType,
    IndexValue,
    Installation,
    Location,
    MeasurementPeriod,
    MeasurementType,
    Measurements,
    MeasurementValue,
    RateLimitStatus,
    Sponsor,
    StandardType,
    StandardValue,
)

__version__ = "0.1.0"

__all__ = [
    "AirlyClient",
    # exceptions
    "AirlyError",
    "AirlyAuthError",
    "AirlyBadRequestError",
    "AirlyForbiddenError",
    "AirlyMovedError",
    "AirlyNotFoundError",
    "AirlyRateLimitError",
    "AirlyServerError",
    # models
    "Address",
    "IndexLevel",
    "IndexType",
    "IndexValue",
    "Installation",
    "Location",
    "MeasurementPeriod",
    "MeasurementType",
    "Measurements",
    "MeasurementValue",
    "RateLimitStatus",
    "Sponsor",
    "StandardType",
    "StandardValue",
]
