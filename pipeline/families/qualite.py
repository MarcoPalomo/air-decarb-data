"""Famille Qualité — AQI air + eau surface.

Sources câblées :
  - Airly        : AQI temps réel (IoT, France/Europe)
  - aeolus/EEA   : stations réglementaires EU (40+ pays)
  - aeolus/OpenAQ: réseau global (agrège vAirify, LAQN, etc.)
  - Sandre       : stations eaux de surface (France, 39 987 stations)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow importing from sibling repos without installing them
_REPO_ROOT = Path(__file__).parents[3]
for _p in [
    _REPO_ROOT / "api-docs",        # airly client
    _REPO_ROOT / "aeolus" / "src",  # aeolus
    _REPO_ROOT / "openaq-python",   # openaq SDK (aeolus dependency)
]:
    _ps = str(_p)
    if _ps not in sys.path:
        sys.path.insert(0, _ps)

logger = logging.getLogger(__name__)


def fetch_airly_aqi(
    locations: list[tuple[float, float]],
    api_key: str | None = None,
    index_type: str = "AIRLY_CAQI",
) -> list[dict]:
    """Fetch AQI + pollutants from Airly for a list of (lat, lng) points.

    Returns list of dicts ready for EnvironmentalRecord conversion.
    """
    from airly import AirlyClient, AirlyNotFoundError, AirlyRateLimitError
    from pipeline.schema import Family, Granularity

    results = []
    with AirlyClient(api_key=api_key) as client:
        for lat, lng in locations:
            try:
                m = client.get_point_measurements(lat=lat, lng=lng, index_type=index_type)
            except AirlyNotFoundError:
                logger.warning("No Airly data at (%.4f, %.4f)", lat, lng)
                continue
            except AirlyRateLimitError:
                logger.error("Airly rate limit reached (%d remaining)", client.rate_limit.remaining_day or 0)
                break

            if not m.current:
                continue

            base = {
                "site_code": f"AIRLY_{lat:.4f}_{lng:.4f}",
                "date_time": m.current.from_date_time,
                "source": "AIRLY",
                "family": Family.QUALITE,
                "granularity": Granularity.REALTIME,
                "ratification": "Provisional",
                "extra": {},
            }

            idx = m.current.get_index(index_type)
            if idx:
                results.append({**base, "measurand": "AQI", "value": idx.value, "units": "CAQI",
                                 "extra": {"level": idx.level, "color": idx.color}})

            for v in m.current.values:
                results.append({**base, "measurand": v.name, "value": v.value, "units": "µg/m³"})

    return results


def fetch_eea_stations(country_code: str = "FR", **kwargs) -> list[dict]:
    """Fetch EEA regulatory station metadata via aeolus.

    Args:
        country_code: ISO 3166-1 alpha-2 country code.

    Returns:
        List of station dicts (site_code, name, lat, lon, measurands).
    """
    try:
        from aeolus.sources.eea import fetch_eea_metadata
        df = fetch_eea_metadata(country_code=country_code, **kwargs)
        return df.to_dict(orient="records")
    except ImportError:
        logger.error("aeolus not importable — check path to aeolus/src/")
        return []


def fetch_sandre_stations(dept_code: str | None = None, max_results: int = 100) -> list[dict]:
    """Fetch surface water monitoring stations from Sandre.

    Args:
        dept_code: French department code filter (e.g. "75", "69").
        max_results: Max stations to return.

    Returns:
        List of station dicts with code, name, lat, lon, nature, dates.
    """
    from pipeline.sources.sandre import fetch_stations
    return fetch_stations(max_results=max_results, dept_code=dept_code)
