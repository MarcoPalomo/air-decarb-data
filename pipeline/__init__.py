"""Environmental data pipeline — 4 families.

Quick start::

    from pipeline import ingest

    records = ingest(
        families=["qualite", "pollution"],
        locations=[(48.8566, 2.3522), (45.7640, 4.8357)],  # Paris, Lyon
        airly_api_key="...",
    )

Families:
    qualite    — AQI (Airly, EEA), eau (Sandre)
    pollution  — PM/gaz (Airly, EEA, PurpleAir), métaux lourds sol (dataverse)
    decarb     — CO₂, GES, BEGES [placeholders — see families/decarb.py]
    predictive — Météo, énergie, trafic [à câbler]
"""

from __future__ import annotations

import logging
from datetime import datetime

from .schema import EnvironmentalRecord, Family, Granularity, SiteRecord

logger = logging.getLogger(__name__)


def ingest(
    families: list[str] | None = None,
    locations: list[tuple[float, float]] | None = None,
    airly_api_key: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    include_soil_baseline: bool = True,
    **kwargs,
) -> list[dict]:
    """Unified ingestion entry point across all active families.

    Args:
        families: Subset of ["qualite", "pollution", "decarb"]. Defaults to all active.
        locations: List of (lat, lng) tuples for IoT/point queries.
        airly_api_key: Airly API key (or set AIRLY_API_KEY env var).
        start_date: Start of historical window (EEA/aeolus sources).
        end_date: End of historical window.
        include_soil_baseline: Include static soil metal stats (Pollution family).

    Returns:
        List of dicts in EnvironmentalRecord-compatible schema.
    """
    active = set(families or ["qualite", "pollution", "decarb"])
    results: list[dict] = []
    locations = locations or []

    if "qualite" in active:
        from .families.qualite import fetch_airly_aqi, fetch_sandre_stations
        logger.info("[qualite] Fetching Airly AQI for %d locations…", len(locations))
        results.extend(fetch_airly_aqi(locations, api_key=airly_api_key))

    if "pollution" in active:
        from .families.pollution import fetch_airly_pollution, fetch_soil_baseline
        logger.info("[pollution] Fetching Airly pollutants for %d locations…", len(locations))
        results.extend(fetch_airly_pollution(locations, api_key=airly_api_key))
        if include_soil_baseline:
            logger.info("[pollution] Loading soil baseline (Zn, Cu)…")
            try:
                results.extend(fetch_soil_baseline())
            except FileNotFoundError:
                logger.warning("dataverse_files.zip not found — skipping soil baseline")

    if "decarb" in active:
        from .families.decarb import fetch_edgar_emissions
        country = kwargs.get("country_code", "FRA")
        y_start = kwargs.get("year_start", 1990)
        y_end = kwargs.get("year_end", 2012)
        gases = kwargs.get("gases", None)
        logger.info("[decarb] Fetching EDGAR for %s %d–%d…", country, y_start, y_end)
        results.extend(fetch_edgar_emissions(country, gases=gases, year_start=y_start, year_end=y_end))

    logger.info("Ingestion complete: %d records across %d families", len(results), len(active))
    return results
