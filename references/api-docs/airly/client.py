"""Airly REST API v2 client.

Covers all public endpoints:
  - /v2/installations/{id}
  - /v2/installations/nearest
  - /v2/measurements/installation
  - /v2/measurements/location
  - /v2/measurements/nearest
  - /v2/measurements/point
  - /v2/meta/indexes
  - /v2/meta/measurements
  - /v2/meta/standards

Rate limit: 100 requests / day per API key (free tier).
Responses are automatically gzip-decoded by requests.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

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

logger = logging.getLogger(__name__)

BASE_URL = "https://airapi.airly.eu/v2"


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _parse_installation(data: dict) -> Installation:
    loc = data.get("location", {})
    addr = data.get("address", {})
    sp = data.get("sponsor")
    return Installation(
        id=data["id"],
        location=Location(latitude=loc["latitude"], longitude=loc["longitude"]),
        location_id=data.get("locationId", data["id"]),
        address=Address(
            country=addr.get("country"),
            city=addr.get("city"),
            street=addr.get("street"),
            number=addr.get("number"),
            display_address1=addr.get("displayAddress1"),
            display_address2=addr.get("displayAddress2"),
        ),
        elevation=data.get("elevation"),
        airly=data.get("airly", False),
        sponsor=Sponsor(
            id=sp["id"],
            name=sp["name"],
            description=sp.get("description"),
            logo=sp.get("logo"),
            link=sp.get("link"),
            display_name=sp.get("displayName"),
        ) if sp else None,
    )


def _parse_measurement_period(data: dict) -> MeasurementPeriod:
    return MeasurementPeriod(
        from_date_time=_parse_dt(data.get("fromDateTime")),
        till_date_time=_parse_dt(data.get("tillDateTime")),
        values=[
            MeasurementValue(name=v["name"], value=v["value"])
            for v in data.get("values", [])
        ],
        indexes=[
            IndexValue(
                name=i["name"],
                value=i.get("value"),
                level=i.get("level"),
                description=i.get("description"),
                advice=i.get("advice"),
                color=i.get("color"),
            )
            for i in data.get("indexes", [])
        ],
        standards=[
            StandardValue(
                name=s["name"],
                pollutant=s["pollutant"],
                limit=s["limit"],
                percent=s["percent"],
                averaging=s["averaging"],
            )
            for s in data.get("standards", [])
        ],
    )


def _parse_measurements(data: dict) -> Measurements:
    current_raw = data.get("current")
    return Measurements(
        current=_parse_measurement_period(current_raw) if current_raw else None,
        history=[_parse_measurement_period(h) for h in data.get("history", [])],
        forecast=[_parse_measurement_period(f) for f in data.get("forecast", [])],
    )


class AirlyClient:
    """Synchronous client for the Airly API v2.

    Args:
        api_key: Your Airly API key. Falls back to AIRLY_API_KEY env var.
        language: Response language — 'en' (default) or 'pl'.
        timeout: Request timeout in seconds.

    Usage::

        with AirlyClient(api_key="...") as client:
            m = client.get_nearest_measurements(lat=50.062, lng=19.941)
            print(m.current.get_index("AIRLY_CAQI"))

    Rate limit note: the free tier allows 100 requests / day. Check
    `client.rate_limit` after any call to see remaining quota.
    """

    def __init__(
        self,
        api_key: str | None = None,
        language: str = "en",
        timeout: float = 10.0,
    ):
        resolved_key = api_key or os.environ.get("AIRLY_API_KEY")
        if not resolved_key:
            raise AirlyAuthError(
                "No API key provided. Pass api_key= or set AIRLY_API_KEY env var."
            )
        self._api_key = resolved_key
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "apikey": self._api_key,
                "Accept": "application/json",
                "Accept-Language": language,
                "Accept-Encoding": "gzip",
            }
        )
        self.rate_limit = RateLimitStatus()

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> AirlyClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self._session.close()

    # ── Internal HTTP ─────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{BASE_URL}{path}"
        logger.debug("GET %s params=%s", url, params)

        resp = self._session.get(url, params=params, timeout=self._timeout, allow_redirects=False)

        self._update_rate_limit(resp.headers)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 301:
            raise AirlyMovedError(
                "Installation has been replaced. See .location attribute for new URL.",
                location=resp.headers.get("Location"),
            )

        body: dict = {}
        try:
            body = resp.json()
        except Exception:
            pass

        message = body.get("message", resp.reason or "Unknown error")

        if resp.status_code == 400:
            raise AirlyBadRequestError(message, violations=body.get("details", {}).get("violations"))
        if resp.status_code == 401:
            raise AirlyAuthError(message, status_code=401)
        if resp.status_code == 403:
            raise AirlyForbiddenError(message, status_code=403)
        if resp.status_code == 404:
            raise AirlyNotFoundError(message, status_code=404)
        if resp.status_code == 429:
            raise AirlyRateLimitError(
                message,
                remaining_day=self.rate_limit.remaining_day,
                limit_day=self.rate_limit.limit_day,
            )
        if resp.status_code >= 500:
            raise AirlyServerError(message, status_code=resp.status_code)

        raise AirlyError(message, status_code=resp.status_code)

    def _update_rate_limit(self, headers: requests.structures.CaseInsensitiveDict) -> None:
        def _int(key: str) -> int | None:
            v = headers.get(key)
            return int(v) if v is not None else None

        self.rate_limit.limit_day = _int("X-RateLimit-Limit-day")
        self.rate_limit.remaining_day = _int("X-RateLimit-Remaining-day")
        self.rate_limit.limit_minute = _int("X-RateLimit-Limit-minute")
        self.rate_limit.remaining_minute = _int("X-RateLimit-Remaining-minute")

        if self.rate_limit.remaining_day is not None:
            logger.debug(
                "Rate limit: %d/%d requests remaining today",
                self.rate_limit.remaining_day,
                self.rate_limit.limit_day or 0,
            )

    # ── Installations ─────────────────────────────────────────────────────────

    def get_installation(self, installation_id: int) -> Installation:
        """Return metadata for a single installation by ID."""
        data = self._get(f"/installations/{installation_id}")
        return _parse_installation(data)

    def get_nearest_installations(
        self,
        lat: float,
        lng: float,
        max_distance_km: float = 3.0,
        max_results: int = 1,
    ) -> list[Installation]:
        """Return installations closest to (lat, lng), sorted by distance.

        Args:
            lat: Latitude (WGS 84 decimal degrees, -90 to +90).
            lng: Longitude (WGS 84 decimal degrees, -180 to +180).
            max_distance_km: Search radius in km. Negative = no limit.
            max_results: Maximum installations to return. Negative = no limit.
        """
        params = {
            "lat": lat,
            "lng": lng,
            "maxDistanceKM": max_distance_km,
            "maxResults": max_results,
        }
        data = self._get("/installations/nearest", params=params)
        return [_parse_installation(item) for item in data]

    def get_installation_by_location(self, location_id: int) -> list[Installation]:
        """Return installations at a given locationId."""
        data = self._get("/installations", params={"locationId": location_id})
        if isinstance(data, list):
            return [_parse_installation(item) for item in data]
        return [_parse_installation(data)]

    def get_installation_by_sensor(self, sensor_id: int) -> Installation:
        """Return the installation associated with a sensor ID."""
        data = self._get("/installations", params={"sensorId": sensor_id})
        return _parse_installation(data)

    # ── Measurements ──────────────────────────────────────────────────────────

    def get_measurements_by_installation(
        self, installation_id: int, index_type: str = "AIRLY_CAQI"
    ) -> Measurements:
        """Return current/historical/forecast measurements for an installation.

        Raises AirlyMovedError if the installation was replaced (301).
        """
        params = {"installationId": installation_id, "indexType": index_type}
        data = self._get("/measurements/installation", params=params)
        return _parse_measurements(data)

    def get_measurements_by_location(
        self, location_id: int, index_type: str = "AIRLY_CAQI"
    ) -> Measurements:
        """Return measurements for all installations at a location."""
        params = {"locationId": location_id, "indexType": index_type}
        data = self._get("/measurements/location", params=params)
        return _parse_measurements(data)

    def get_nearest_measurements(
        self,
        lat: float,
        lng: float,
        max_distance_km: float = 3.0,
        index_type: str = "AIRLY_CAQI",
    ) -> Measurements:
        """Return measurements for the installation closest to (lat, lng).

        Raises AirlyNotFoundError if no installation found within max_distance_km.
        """
        params = {
            "lat": lat,
            "lng": lng,
            "maxDistanceKM": max_distance_km,
            "indexType": index_type,
        }
        data = self._get("/measurements/nearest", params=params)
        return _parse_measurements(data)

    def get_point_measurements(
        self,
        lat: float,
        lng: float,
        index_type: str = "AIRLY_CAQI",
    ) -> Measurements:
        """Return interpolated measurements for any arbitrary geographic point.

        Values are a distance-weighted average of sensors within 1.5 km.
        """
        params = {"lat": lat, "lng": lng, "indexType": index_type}
        data = self._get("/measurements/point", params=params)
        return _parse_measurements(data)

    # ── Meta ──────────────────────────────────────────────────────────────────

    def get_meta_indexes(self) -> list[IndexType]:
        """Return all supported air quality index types and their level definitions."""
        data = self._get("/meta/indexes")
        return [
            IndexType(
                name=item["name"],
                levels=[
                    IndexLevel(
                        values=lvl["values"],
                        level=lvl["level"],
                        description=lvl["description"],
                        color=lvl["color"],
                    )
                    for lvl in item.get("levels", [])
                ],
            )
            for item in data
        ]

    def get_meta_measurements(self) -> list[MeasurementType]:
        """Return all supported measurement types with names and units."""
        data = self._get("/meta/measurements")
        return [
            MeasurementType(name=item["name"], label=item["label"], unit=item["unit"])
            for item in data
        ]

    def get_meta_standards(self) -> list[StandardType]:
        """Return all supported air quality standard types (e.g. WHO limits)."""
        data = self._get("/meta/standards")
        return [
            StandardType(
                name=item["name"],
                standard_limits=item.get("standardLimits", {}),
            )
            for item in data
        ]
