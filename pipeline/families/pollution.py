"""Famille Pollution — PM2.5, NOx, COV, métaux lourds.

Sources câblées :
  - Airly        : PM2.5, PM10, NO2, SO2, CO, O3 temps réel
  - aeolus/EEA   : stations réglementaires (NO2, PM, O3, SO2, benzène, métaux traces)
  - aeolus/PurpleAir : réseau IoT (PM1, PM2.5, PM10)
  - SOL_DATAVERSE: stats historiques Zn et Cu dans les sols (baseline)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).parents[3]
for _p in [_REPO_ROOT / "api-docs", _REPO_ROOT / "aeolus" / "src", _REPO_ROOT / "openaq-python"]:
    _ps = str(_p)
    if _ps not in sys.path:
        sys.path.insert(0, _ps)

logger = logging.getLogger(__name__)

# Pollutants that belong to this family (Airly measurement names)
POLLUTION_MEASURANDS = {"PM1", "PM25", "PM10", "NO2", "SO2", "CO", "O3", "NO", "NOx"}


def fetch_airly_pollution(
    locations: list[tuple[float, float]],
    api_key: str | None = None,
) -> list[dict]:
    """Fetch air pollutants from Airly (PM + gases).

    Filters to POLLUTION_MEASURANDS only — AQI index goes to Qualité family.
    """
    from airly import AirlyClient, AirlyNotFoundError, AirlyRateLimitError
    from pipeline.schema import Family, Granularity

    results = []
    with AirlyClient(api_key=api_key) as client:
        for lat, lng in locations:
            try:
                m = client.get_point_measurements(lat=lat, lng=lng)
            except AirlyNotFoundError:
                logger.warning("No Airly sensor within 1.5 km of (%.4f, %.4f)", lat, lng)
                continue
            except AirlyRateLimitError:
                logger.error("Airly rate limit — stopping")
                break

            if not m.current:
                continue

            for v in m.current.values:
                if v.name not in POLLUTION_MEASURANDS:
                    continue
                results.append({
                    "site_code": f"AIRLY_{lat:.4f}_{lng:.4f}",
                    "date_time": m.current.from_date_time,
                    "measurand": v.name,
                    "value": v.value,
                    "units": "µg/m³",
                    "source": "AIRLY",
                    "family": Family.POLLUTION,
                    "granularity": Granularity.REALTIME,
                    "ratification": "Provisional",
                })

            # WHO standards compliance
            for std in m.current.standards:
                results.append({
                    "site_code": f"AIRLY_{lat:.4f}_{lng:.4f}",
                    "date_time": m.current.from_date_time,
                    "measurand": f"{std.pollutant}_WHO_PCT",
                    "value": std.percent,
                    "units": "%",
                    "source": "AIRLY",
                    "family": Family.POLLUTION,
                    "granularity": Granularity.REALTIME,
                    "ratification": "Provisional",
                    "extra": {"limit_µg_m3": std.limit, "averaging": std.averaging},
                })

    return results


def fetch_eea_pollution(
    site_codes: list[str],
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """Fetch verified pollution measurements from EEA via aeolus.

    Covers: NO2, PM10, PM2.5, O3, SO2, CO, NO, NOx, benzene, trace metals.
    """
    try:
        import aeolus
        df = aeolus.download("EEA", site_codes, start_date, end_date)
        records = df.to_dict(orient="records")
        for r in records:
            r["source"] = "EEA"
            r["family"] = "pollution"
        return records
    except ImportError:
        logger.error("aeolus not importable")
        return []
    except Exception as e:
        logger.error("EEA fetch failed: %s", e)
        return []


def fetch_soil_baseline() -> list[dict]:
    """Load historical soil metal statistics (Zn, Cu) from dataverse.

    Returns records in EnvironmentalRecord-compatible format.
    Static baseline — call once and cache.
    """
    from pipeline.sources.sol import as_environmental_records
    return list(as_environmental_records())


def check_who_exceedances(records: list[dict]) -> list[dict]:
    """Filter records to those exceeding WHO limits (pct > 100).

    Input records must have measurand ending in _WHO_PCT and value set.
    """
    return [r for r in records if r.get("measurand", "").endswith("_WHO_PCT") and (r.get("value") or 0) > 100]
