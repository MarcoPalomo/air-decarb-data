"""Common schema for all environmental data families.

Aligns with aeolus DataRecord / SiteRecord conventions so records
from any family can flow into the same processing layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Family(str, Enum):
    QUALITE = "qualite"       # AQI, eau, sol, biodiversité
    POLLUTION = "pollution"   # PM2.5, NOx, COV, métaux lourds
    DECARB = "decarb"         # CO₂, GES, BEGES
    PREDICTIVE = "predictive" # Météo, énergie, trafic


class Granularity(str, Enum):
    REALTIME = "realtime"     # <1h, IoT sensors
    HOURLY = "hourly"
    DAILY = "daily"
    PERIODIC = "periodic"     # quinquennal, annuel — static baselines


@dataclass
class SiteRecord:
    """Canonical site/station metadata across all sources."""
    site_code: str
    site_name: str
    source: str                  # e.g. "AIRLY", "SANDRE", "EEA", "SOL_DATAVERSE"
    family: Family
    latitude: float | None = None
    longitude: float | None = None
    location_type: str | None = None
    country: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnvironmentalRecord:
    """Canonical measurement record for all environmental data families.

    Compatible with aeolus DataRecord schema — same required fields,
    extended with family and granularity context.
    """
    site_code: str
    date_time: datetime
    measurand: str               # "PM2.5", "Zn", "NO2", "pH", "CO2"
    value: float | None
    units: str                   # "µg/m³", "mg/kg", "ppm", "%"
    source: str                  # "AIRLY", "SANDRE", "EEA", "SOL_DATAVERSE"
    family: Family
    granularity: Granularity = Granularity.HOURLY
    ratification: str = "Provisional"
    extra: dict[str, Any] = field(default_factory=dict)
