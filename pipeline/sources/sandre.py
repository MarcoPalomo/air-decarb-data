"""Sandre eau.france.fr — water monitoring stations client.

Covers the SANDRE Referentiels API v1 (XML format).
Base URL: https://api.sandre.eaufrance.fr/referentiels/v1/

IMPORTANT — Sandre bulk download design:
  The API does not support filtering or pagination. Every list endpoint
  returns the full referential (~197 MB for StationMesureEauxSurface).
  Strategy:
    - fetch_station(code)          → single station lookup (4 KB, fast)
    - download_all(output_path)    → download once, save to Parquet
    - load_cache(parquet_path)     → read from local cache (milliseconds)

Coordinates: Lambert 93 (EPSG:2154, Sandre system code "26").
Install pyproj for WGS84 conversion: pip install pyproj
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Iterator
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sandre.eaufrance.fr/referentiels/v1"

_pyproj_warned = False
_transformer = None


def _get_transformer():
    global _transformer, _pyproj_warned
    if _transformer is not None:
        return _transformer
    try:
        from pyproj import Transformer
        _transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    except ImportError:
        if not _pyproj_warned:
            logger.warning("pyproj not installed — Lambert 93 coords kept as-is. pip install pyproj")
            _pyproj_warned = True
        _transformer = False
    return _transformer


def _to_wgs84(x: float | None, y: float | None) -> tuple[float | None, float | None]:
    if x is None or y is None:
        return None, None
    tf = _get_transformer()
    if not tf:
        return None, None
    lon, lat = tf.transform(x, y)
    return round(lat, 6), round(lon, 6)


def _strip_ns(xml_bytes: bytes) -> str:
    text = xml_bytes.decode("utf-8")
    text = re.sub(r'\s+xmlns(?::\w+)?="[^"]*"', "", text)
    text = re.sub(r'\s+xsi:\w+="[^"]*"', "", text)
    return text


def _text(el: ET.Element | None, tag: str) -> str | None:
    if el is None:
        return None
    child = el.find(tag)
    return child.text.strip() if child is not None and child.text else None


def _float(el: ET.Element | None, tag: str) -> float | None:
    v = _text(el, tag)
    if v is None:
        return None
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return None


def _parse_station_el(st: ET.Element) -> dict:
    raw_x = _float(st, "CoordXStationMesureEauxSurface")
    raw_y = _float(st, "CoordYStationMesureEauxSurface")
    proj = _text(st, "ProjStationMesureEauxSurface")
    lat, lon = _to_wgs84(raw_x, raw_y) if proj == "26" else (None, None)
    commune = st.find("Commune")
    return {
        "code": _text(st, "CdStationMesureEauxSurface"),
        "name": _text(st, "NomStationMesureEauxSurface") or _text(st, "LbStationMesureEauxSurface"),
        "lat": lat,
        "lon": lon,
        "coord_x_lambert93": raw_x if proj == "26" else None,
        "coord_y_lambert93": raw_y if proj == "26" else None,
        "altitude": _float(st, "AltitudePointCaracteritisque"),
        "commune_code": _text(commune, "CdCommune") if commune is not None else None,
        "commune_name": _text(commune, "LbCommune") if commune is not None else None,
        "nature": _text(st, "NatureStationMesureEauxSurface"),
        "date_created": _text(st, "DateCreationStationMesureEauxSurface"),
        "date_updated": _text(st, "DateMAJInfosStationMesureEauxSurface"),
        "source": "SANDRE",
    }


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_station(code: str, timeout: float = 10.0) -> dict | None:
    """Fetch a single water monitoring station by Sandre code (~4 KB).

    Args:
        code: Sandre station code e.g. "06110600".

    Returns:
        Station dict or None if not found.
    """
    url = f"{BASE_URL}/StationMesureEauxSurface/{code}.xml"
    resp = requests.get(url, timeout=timeout)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    root = ET.fromstring(_strip_ns(resp.content))
    stations = root.findall(".//StationMesureEauxSurface")
    return _parse_station_el(stations[0]) if stations else None


def download_all(
    output_path: Path | str | None = None,
    timeout: float = 300.0,
) -> list[dict]:
    """Download the complete Sandre surface water station referential (~197 MB).

    This is a one-time bulk download. Results are optionally saved to Parquet
    for instant subsequent loads via load_cache().

    Args:
        output_path: Optional path for Parquet cache (e.g. "sandre_stations.parquet").
        timeout: HTTP timeout in seconds — large file, allow 5 min minimum.

    Returns:
        List of all ~40,000 station dicts.
    """
    logger.info("Downloading full Sandre referential (~197 MB)…")
    url = f"{BASE_URL}/StationMesureEauxSurface.xml"
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()

    chunks = []
    downloaded = 0
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        chunks.append(chunk)
        downloaded += len(chunk)
        if downloaded % (10 * 1024 * 1024) == 0:
            logger.info("  %.0f MB downloaded…", downloaded / 1024 / 1024)

    raw = b"".join(chunks)
    logger.info("Download complete (%.1f MB). Parsing…", len(raw) / 1024 / 1024)

    root = ET.fromstring(_strip_ns(raw))
    stations = [_parse_station_el(st) for st in root.findall(".//StationMesureEauxSurface")]
    logger.info("Parsed %d stations.", len(stations))

    if output_path:
        try:
            import pandas as pd
            df = pd.DataFrame(stations)
            df.to_parquet(str(output_path), index=False)
            logger.info("Saved to %s", output_path)
        except ImportError:
            logger.warning("pandas/pyarrow not installed — Parquet save skipped.")

    return stations


def load_cache(parquet_path: Path | str) -> list[dict]:
    """Load a previously downloaded Sandre referential from Parquet cache.

    Args:
        parquet_path: Path to the .parquet file created by download_all().
    """
    import pandas as pd
    df = pd.read_parquet(str(parquet_path))
    return df.to_dict(orient="records")


def fetch_analysis_parameters(timeout: float = 30.0) -> list[dict]:
    """Fetch the Sandre catalogue of chemical analysis parameters."""
    url = f"{BASE_URL}/ParametreAnalyse.xml"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    root = ET.fromstring(_strip_ns(resp.content))
    results = []
    for p in root.findall(".//ParametreAnalyse"):
        results.append({
            "code": _text(p, "CdParametreAnalyse") or _text(p, "CdParametre"),
            "name": _text(p, "NomParametreAnalyse") or _text(p, "NomParametre"),
            "unit": _text(p, "UniteReferenceSandreAnalyse"),
            "cas_number": _text(p, "NumeroCASParametreAnalyse"),
            "family": _text(p, "FamilleParametreAnalyse"),
            "source": "SANDRE",
        })
    return results
