"""Airly API response models.

All timestamps are UTC ISO 8601. Coordinates are WGS 84 decimal degrees.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ── Installations ─────────────────────────────────────────────────────────────

@dataclass
class Location:
    latitude: float
    longitude: float


@dataclass
class Address:
    country: str | None = None
    city: str | None = None
    street: str | None = None
    number: str | None = None
    display_address1: str | None = None
    display_address2: str | None = None


@dataclass
class Sponsor:
    id: int
    name: str
    description: str | None = None
    logo: str | None = None
    link: str | None = None
    display_name: str | None = None


@dataclass
class Installation:
    id: int
    location: Location
    location_id: int
    address: Address
    elevation: float | None
    airly: bool
    sponsor: Sponsor | None = None


# ── Measurements ──────────────────────────────────────────────────────────────

@dataclass
class MeasurementValue:
    name: str
    value: float


@dataclass
class IndexValue:
    name: str
    value: float | None = None
    level: str | None = None
    description: str | None = None
    advice: str | None = None
    color: str | None = None


@dataclass
class StandardValue:
    name: str
    pollutant: str
    limit: float
    percent: float
    averaging: str


@dataclass
class MeasurementPeriod:
    from_date_time: datetime
    till_date_time: datetime
    values: list[MeasurementValue] = field(default_factory=list)
    indexes: list[IndexValue] = field(default_factory=list)
    standards: list[StandardValue] = field(default_factory=list)

    def get_value(self, name: str) -> float | None:
        """Return the value for a named pollutant or measurement, e.g. 'PM10'."""
        for v in self.values:
            if v.name == name:
                return v.value
        return None

    def get_index(self, name: str = "AIRLY_CAQI") -> IndexValue | None:
        for idx in self.indexes:
            if idx.name == name:
                return idx
        return None


@dataclass
class Measurements:
    current: MeasurementPeriod | None
    history: list[MeasurementPeriod] = field(default_factory=list)
    forecast: list[MeasurementPeriod] = field(default_factory=list)


# ── Meta ──────────────────────────────────────────────────────────────────────

@dataclass
class MeasurementType:
    name: str
    label: str
    unit: str


@dataclass
class IndexLevel:
    values: str
    level: str
    description: str
    color: str


@dataclass
class IndexType:
    name: str
    levels: list[IndexLevel] = field(default_factory=list)


@dataclass
class StandardType:
    name: str
    standard_limits: dict[str, float] = field(default_factory=dict)


# ── Rate-limit state ──────────────────────────────────────────────────────────

@dataclass
class RateLimitStatus:
    limit_day: int | None = None
    remaining_day: int | None = None
    limit_minute: int | None = None
    remaining_minute: int | None = None
