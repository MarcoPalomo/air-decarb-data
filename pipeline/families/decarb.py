"""Famille Décarb — CO₂, GES, bilan carbone, BEGES.

Sources implémentées :
  - EDGAR v4.3.2   : inventaires GES nationaux (CO₂, CH₄, N₂O) par secteur IPCC
                     Données locales : ../../data/edgar/csv/*.csv (1970–2012, 225 pays, Gg)
  - BEGES (ADEME)  : bilans carbone entreprises via data.ademe.fr open data API

Source à câbler :
  - CAMS ADS (cdsapi) : colonnes atmosphériques CH₄/CO₂/NO₂ Copernicus
"""

from __future__ import annotations

import csv
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

# ── EDGAR paths ───────────────────────────────────────────────────────────────

_EDGAR_DIR = Path(__file__).parents[2] / "data" / "edgar" / "csv"

_EDGAR_FILES: dict[str, Path] = {
    "CO2":      _EDGAR_DIR / "co2_excl_short-cycle_org_c.csv",
    "CO2_BIO":  _EDGAR_DIR / "co2_org-short-cycle_c.csv",
    "CH4":      _EDGAR_DIR / "ch4.csv",
    "N2O":      _EDGAR_DIR / "n2o.csv",
}

# GWP100 AR5 (IPCC 2014) — used to convert to CO₂eq
GWP100: dict[str, float] = {"CO2": 1.0, "CO2_BIO": 1.0, "CH4": 28.0, "N2O": 265.0}

# IPCC sector codes → human label
IPCC_SECTORS: dict[str, str] = {
    "1A1a":  "Energy / Electricity & Heat",
    "1A1bc": "Energy / Petroleum refining",
    "1A2":   "Energy / Manufacturing & Construction",
    "1A3a":  "Transport / Aviation",
    "1A3b":  "Transport / Road",
    "1A3c":  "Transport / Railways",
    "1A3d":  "Transport / Navigation",
    "1A3e":  "Transport / Other",
    "1A4":   "Energy / Buildings & Agriculture",
    "1A5":   "Energy / Other",
    "1B1":   "Fugitive / Coal",
    "1B2":   "Fugitive / Oil & Gas",
    "1C1":   "Carbon capture / Transport",
    "1C2":   "Carbon capture / Injection & Storage",
    "2A1":   "Industry / Cement",
    "2A2":   "Industry / Lime",
    "2A3":   "Industry / Glass",
    "2A4":   "Industry / Ceramics",
    "2A7":   "Industry / Other mineral",
    "2B":    "Industry / Chemicals",
    "2C":    "Industry / Metals",
    "2G":    "Industry / Other",
    "3A":    "Solvents",
    "3B":    "Agriculture / Livestock",
    "3C":    "Agriculture / Manure",
    "3D":    "Agriculture / Rice & Soils",
    "4D4":   "Waste / Wastewater",
    "6C":    "Waste / Incineration",
    "7A":    "Other",
}

# BEGES API (data.ademe.fr — CKAN / data-fair)
_BEGES_API = "https://data.ademe.fr/data-fair/api/v1/datasets/bilans-ges-ademe/lines"


# ── EDGAR ─────────────────────────────────────────────────────────────────────

def fetch_edgar_emissions(
    country_code: str = "FRA",
    gases: list[str] | None = None,
    year_start: int = 1990,
    year_end: int = 2012,
    categories: list[str] | None = None,
    as_co2eq: bool = False,
) -> list[dict]:
    """Load national GHG inventory from local EDGAR v4.3.2 CSV files.

    Args:
        country_code: ISO 3166-1 alpha-3 code (e.g. "FRA", "DEU", "CHN").
        gases: Subset of ["CO2", "CO2_BIO", "CH4", "N2O"]. Defaults to all.
        year_start: First year of the window (inclusive). Min: 1970.
        year_end: Last year of the window (inclusive). Max: 2012.
        categories: IPCC sector codes to include. Defaults to all 29 sectors.
        as_co2eq: If True, multiply Gg emissions by GWP100 to get Gg CO₂eq.

    Returns:
        List of EnvironmentalRecord-compatible dicts.
        Emissions unit: "Gg" (gigagrams = 10³ tonnes) or "Gg CO₂eq" if as_co2eq.
    """
    target_gases = gases or list(_EDGAR_FILES.keys())
    results: list[dict] = []

    for gas in target_gases:
        path = _EDGAR_FILES.get(gas)
        if path is None:
            logger.warning("Unknown gas '%s' — skipping. Valid: %s", gas, list(_EDGAR_FILES))
            continue
        if not path.exists():
            logger.error("EDGAR file not found: %s", path)
            continue

        gwp = GWP100[gas] if as_co2eq else 1.0
        units = "Gg CO₂eq" if as_co2eq else "Gg"
        measurand = "GHG_CO2eq" if as_co2eq else gas

        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if row["Code"] != country_code:
                    continue
                year = int(row["Year"])
                if not (year_start <= year <= year_end):
                    continue
                cat = row["Category"]
                if categories and cat not in categories:
                    continue

                raw = row["Emissions"].strip()
                if not raw:
                    continue
                value = float(raw)
                results.append({
                    "site_code": f"EDGAR_{country_code}",
                    "date_time": datetime(year, 1, 1, tzinfo=timezone.utc),
                    "measurand": measurand if as_co2eq else gas,
                    "value": round(value * gwp, 6),
                    "units": units,
                    "source": "EDGAR",
                    "family": "decarb",
                    "granularity": "periodic",
                    "ratification": "Verified",
                    "extra": {
                        "country_code": country_code,
                        "ipcc_category": cat,
                        "sector_label": IPCC_SECTORS.get(cat, cat),
                        "gas": gas,
                        "gwp100": gwp,
                        "edgar_version": "v4.3.2",
                    },
                })

    logger.info(
        "EDGAR: %d records for %s, gases=%s, %d–%d",
        len(results), country_code, target_gases, year_start, year_end,
    )
    return results


def aggregate_edgar_by_year(records: list[dict]) -> list[dict]:
    """Sum EDGAR records by (country, year) to get annual totals.

    Input records must come from fetch_edgar_emissions(as_co2eq=True).
    Returns one record per year with value = total Gg CO₂eq.
    """
    totals: dict[tuple, float] = {}
    meta: dict[tuple, dict] = {}

    for r in records:
        year = r["date_time"].year
        country = r["extra"]["country_code"]
        key = (country, year)
        totals[key] = totals.get(key, 0.0) + (r["value"] or 0.0)
        meta[key] = r

    return [
        {
            **meta[key],
            "measurand": "GHG_total_CO2eq",
            "value": round(total, 3),
            "extra": {
                **meta[key]["extra"],
                "ipcc_category": "ALL",
                "sector_label": "All sectors",
            },
        }
        for key, total in sorted(totals.items())
    ]


# ── BEGES ─────────────────────────────────────────────────────────────────────

def fetch_beges_registry(
    siren: str | None = None,
    year: int | None = None,
    page_size: int = 100,
    max_pages: int = 5,
) -> list[dict]:
    """Fetch corporate carbon footprints from ADEME BEGES open data registry.

    Source: data.ademe.fr — updated periodically (annual declarations).

    Args:
        siren: Filter by company SIREN number (9-digit string).
        year: Filter by reporting year.
        page_size: Records per API page (max 10 000).
        max_pages: Safety cap on pagination.

    Returns:
        List of EnvironmentalRecord-compatible dicts (one per scope per company).
    """
    params: dict = {"size": page_size, "page": 1}
    if siren:
        params["q"] = siren
    if year:
        params["annee_de_reporting"] = year

    results: list[dict] = []

    for page in range(1, max_pages + 1):
        params["page"] = page
        try:
            resp = requests.get(_BEGES_API, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("BEGES API error (page %d): %s", page, exc)
            break

        data = resp.json()
        items = data.get("results", [])
        if not items:
            break

        for item in items:
            reporting_year = item.get("annee_de_reporting") or item.get("Annee_de_reporting")
            company_name = item.get("nom_de_lorganisme") or item.get("Nom_de_lorganisme", "")
            siren_val = item.get("siren") or item.get("SIREN", "")
            sector = item.get("secteur_naf") or item.get("Secteur_NAF", "")

            if not reporting_year:
                continue

            try:
                dt = datetime(int(reporting_year), 1, 1, tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue

            base = {
                "site_code": f"BEGES_{siren_val}",
                "date_time": dt,
                "source": "BEGES_ADEME",
                "family": "decarb",
                "granularity": "periodic",
                "ratification": "Declared",
                "units": "tCO2eq",
                "extra": {
                    "company_name": company_name,
                    "siren": siren_val,
                    "sector_naf": sector,
                    "reporting_year": reporting_year,
                },
            }

            scopes = {
                "Scope1": item.get("emissions_scope_1") or item.get("Scope_1"),
                "Scope2": item.get("emissions_scope_2") or item.get("Scope_2"),
                "Scope3": item.get("emissions_scope_3") or item.get("Scope_3"),
            }
            for scope, val in scopes.items():
                if val is None:
                    continue
                try:
                    results.append({**base, "measurand": f"GHG_{scope}", "value": float(val)})
                except (ValueError, TypeError):
                    pass

        if len(items) < page_size:
            break

    logger.info("BEGES: %d scope records retrieved", len(results))
    return results


# ── CAMS / Copernicus ─────────────────────────────────────────────────────────
# Requires: pip install cdsapi xarray netCDF4
# Credentials: ~/.cdsapirc  (https://ads.atmosphere.copernicus.eu/ → API key)
#
# Dataset coverage:
#   EGG4 (CO₂/CH₄ reanalysis) : 2003-01-01 → 2020-12-31, 6-hourly, 0.75°×0.75°
#   EAC4 (air quality)         : 2003-01-01 → present (lag ~1 month), 3-hourly, 0.75°

_CAMS_VARIABLES: dict[str, tuple[str, str, str, str]] = {
    # name -> (dataset_id, cams_variable, nc_short_name, unit)
    "methane": (
        "cams-global-ghg-reanalysis-egg4",
        "total_column_methane",
        "tcch4",
        "kg/m²",
    ),
    "carbon_dioxide": (
        "cams-global-ghg-reanalysis-egg4",
        "total_column_carbon_dioxide",
        "tcco2",
        "kg/m²",
    ),
    "nitrogen_dioxide": (
        "cams-global-reanalysis-eac4",
        "nitrogen_dioxide",
        "tcno2",
        "kg/m²",
    ),
    "ozone": (
        "cams-global-reanalysis-eac4",
        "total_column_ozone",
        "gtco3",
        "kg/m²",
    ),
}


def fetch_cams_column(
    lat: float,
    lng: float,
    date: str,
    variable: str = "methane",
    time: str = "00:00",
) -> dict | None:
    """Fetch an atmospheric column value from Copernicus CAMS ADS.

    Downloads a small NetCDF bounding box (±0.5°) and extracts the
    nearest-grid-point value for the requested variable and date.

    Supported variables: "methane", "carbon_dioxide", "nitrogen_dioxide", "ozone".
    EGG4 dataset covers 2003–2020; EAC4 covers 2003–present (~1-month lag).

    Args:
        lat: Latitude (decimal degrees, WGS84).
        lng: Longitude (decimal degrees, WGS84).
        date: ISO date string "YYYY-MM-DD".
        variable: CAMS variable name (see _CAMS_VARIABLES keys).
        time: UTC time of day — "00:00", "06:00", "12:00" or "18:00".

    Returns:
        EnvironmentalRecord-compatible dict, or None on error.

    Setup:
        pip install cdsapi xarray netCDF4
        # Create ~/.cdsapirc:
        # url: https://ads.atmosphere.copernicus.eu/api/v2
        # key: <UID>:<API-key>   (from your ADS account)
    """
    try:
        import cdsapi
    except ImportError:
        logger.error(
            "cdsapi not installed. Run: pip install cdsapi\n"
            "Then create ~/.cdsapirc with your ADS credentials."
        )
        return None

    if variable not in _CAMS_VARIABLES:
        logger.error(
            "Unknown CAMS variable '%s'. Valid: %s", variable, list(_CAMS_VARIABLES)
        )
        return None

    dataset_id, cams_var, nc_short, unit = _CAMS_VARIABLES[variable]

    request: dict = {
        "variable": cams_var,
        "date": date,
        "time": time,
        "area": [lat + 0.5, lng - 0.5, lat - 0.5, lng + 0.5],  # N/W/S/E
        "format": "netcdf",
    }

    tmp_path = Path(tempfile.mktemp(suffix=".nc"))
    try:
        client = cdsapi.Client(quiet=True)
        logger.info("CAMS: retrieving %s at (%.3f, %.3f) for %s…", variable, lat, lng, date)
        client.retrieve(dataset_id, request, str(tmp_path))

        value = _cams_extract_point(tmp_path, nc_short, lat, lng)
        if value is None:
            return None

        dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        measurand = f"COLUMN_{variable.upper()}"

        return {
            "site_code": f"CAMS_{lat:.3f}_{lng:.3f}",
            "date_time": dt,
            "measurand": measurand,
            "value": round(value, 9),
            "units": unit,
            "source": "CAMS",
            "family": "decarb",
            "granularity": "daily",
            "ratification": "Reanalysis",
            "extra": {
                "latitude": lat,
                "longitude": lng,
                "cams_dataset": dataset_id,
                "cams_variable": cams_var,
                "time": time,
            },
        }

    except Exception as exc:
        logger.error("CAMS fetch failed: %s", exc)
        return None
    finally:
        tmp_path.unlink(missing_ok=True)


def _cams_extract_point(
    nc_path: Path,
    short_name: str,
    lat: float,
    lng: float,
) -> float | None:
    """Extract nearest-point value from a CAMS NetCDF file.

    Tries xarray first (preferred), falls back to netCDF4.
    Takes the first time step from the file.
    """
    # ── xarray (preferred) ────────────────────────────────────────────────────
    try:
        import xarray as xr

        ds = xr.open_dataset(nc_path)
        # Variable name: try exact short name first, then partial match, then first var
        var = (
            short_name
            if short_name in ds.data_vars
            else next((v for v in ds.data_vars if short_name.lower() in v.lower()), None)
            or list(ds.data_vars)[0]
        )
        arr = ds[var]
        if "time" in arr.dims:
            arr = arr.isel(time=0)
        # Nearest lat/lon (handles both "latitude"/"longitude" and "lat"/"lon")
        lat_dim = "latitude" if "latitude" in arr.dims else "lat"
        lon_dim = "longitude" if "longitude" in arr.dims else "lon"
        arr = arr.sel({lat_dim: lat, lon_dim: lng}, method="nearest")
        val = float(arr.values)
        ds.close()
        logger.info("CAMS: %s = %.6e %s (via xarray)", var, val, "kg/m²")
        return val

    except ImportError:
        pass

    # ── netCDF4 fallback ──────────────────────────────────────────────────────
    try:
        import netCDF4 as nc
        import numpy as np

        ds = nc.Dataset(nc_path)
        lats = ds.variables.get("latitude") or ds.variables["lat"]
        lons = ds.variables.get("longitude") or ds.variables["lon"]
        lat_arr = lats[:]
        lon_arr = lons[:]
        lat_idx = int(np.argmin(np.abs(lat_arr - lat)))
        lon_idx = int(np.argmin(np.abs(lon_arr - lng)))

        data_vars = [k for k in ds.variables if k not in ("latitude", "longitude", "lat", "lon", "time")]
        target = next((v for v in data_vars if short_name.lower() in v.lower()), data_vars[0])
        raw = ds.variables[target]
        val = float(raw[0, lat_idx, lon_idx])  # first time step
        ds.close()
        logger.info("CAMS: %s = %.6e kg/m² (via netCDF4)", target, val)
        return val

    except ImportError:
        pass

    logger.error(
        "No NetCDF reader available. Install: pip install xarray netCDF4\n"
        "  or:  conda install xarray netcdf4"
    )
    return None
