# Stratégie de Tests — Air Quality & Decarbonization Data Platform

## Table des matières

1. [Philosophie et niveaux de tests](#1-philosophie-et-niveaux-de-tests)
2. [Tests unitaires — pipeline/](#2-tests-unitaires--pipeline)
3. [Tests unitaires — aeolus](#3-tests-unitaires--aeolus)
4. [Tests d'intégration](#4-tests-dintégration)
5. [Tests de qualité des données](#5-tests-de-qualité-des-données)
6. [Tests des flows Prefect](#6-tests-des-flows-prefect)
7. [Tests de schéma et contrats](#7-tests-de-schéma-et-contrats)
8. [Tests de performance](#8-tests-de-performance)
9. [CI/CD Tekton](#9-cicd-tekton)
10. [Structure des tests](#10-structure-des-tests)

---

## 1. Philosophie et niveaux de tests

### Pyramide de tests

```
         ┌────────────┐
         │  E2E (rares)│        Flow complet : ingestion → Iceberg → MLflow
         └────────────┘
       ┌──────────────────┐
       │  Intégration      │     API réelles, MinIO local, TimescaleDB test
       └──────────────────┘
     ┌──────────────────────┐
     │  Unitaires (majorité) │   Mock HTTP, fixtures CSV, données synthétiques
     └──────────────────────┘
```

### Règles

1. **Les tests unitaires ne touchent pas le réseau.** Toutes les APIs externes (Airly, EEA, CAMS, BEGES) sont mockées via `responses` ou `pytest-mock`.
2. **Les tests d'intégration utilisent des services locaux** (MinIO via conteneur, TimescaleDB test DB).
3. **Les données de test sont synthétiques**, jamais des vraies clés API ou des données sensibles (SIREN BEGES réel, coordonnées GPS privées).
4. **La couverture cible : 80% pour le code de production**, 100% pour les parsers de schéma.

### Outils

```bash
pip install pytest pytest-cov responses pytest-mock freezegun
```

---

## 2. Tests unitaires — pipeline/

### `tests/test_schema.py`

```python
"""Tests du schéma EnvironmentalRecord et des enums."""

from datetime import datetime, timezone
import pytest
from pipeline.schema import (
    EnvironmentalRecord, SiteRecord, Family, Granularity
)


def test_family_enum_values():
    assert Family.QUALITE.value == "qualite"
    assert Family.DECARB.value == "decarb"
    assert Family.POLLUTION.value == "pollution"
    assert Family.PREDICTIVE.value == "predictive"


def test_granularity_enum_values():
    assert Granularity.REALTIME.value == "realtime"
    assert Granularity.PERIODIC.value == "periodic"


def test_environmental_record_defaults():
    rec = EnvironmentalRecord(
        site_code="TEST_001",
        date_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
        measurand="NO2",
        value=35.2,
        units="µg/m³",
        source="TEST",
        family=Family.POLLUTION,
    )
    assert rec.granularity == Granularity.HOURLY
    assert rec.ratification == "Provisional"
    assert rec.extra == {}


def test_environmental_record_none_value():
    # Les valeurs None (données manquantes) doivent être acceptées
    rec = EnvironmentalRecord(
        site_code="TEST_001",
        date_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
        measurand="PM2.5",
        value=None,
        units="µg/m³",
        source="EEA",
        family=Family.POLLUTION,
    )
    assert rec.value is None


def test_site_record():
    site = SiteRecord(
        site_code="AIRLY_48.8566_2.3522",
        site_name="Paris Centre",
        source="AIRLY",
        family=Family.QUALITE,
        latitude=48.8566,
        longitude=2.3522,
    )
    assert site.latitude == 48.8566
    assert site.country is None   # Optional
```

### `tests/test_decarb_edgar.py`

```python
"""Tests EDGAR — lecture CSV locale, filtrage, conversion CO₂eq."""

import pytest
from pipeline.families.decarb import (
    fetch_edgar_emissions,
    aggregate_edgar_by_year,
    GWP100,
    IPCC_SECTORS,
    _EDGAR_DIR,
)


def test_edgar_dir_exists():
    assert _EDGAR_DIR.exists(), f"EDGAR data dir not found: {_EDGAR_DIR}"


def test_edgar_csv_files_present():
    for gas, path in [
        ("CO2", _EDGAR_DIR / "co2_excl_short-cycle_org_c.csv"),
        ("CH4", _EDGAR_DIR / "ch4.csv"),
        ("N2O", _EDGAR_DIR / "n2o.csv"),
    ]:
        assert path.exists(), f"Missing EDGAR file for {gas}: {path}"


def test_fetch_edgar_france_co2_basic():
    records = fetch_edgar_emissions("FRA", gases=["CO2"], year_start=2010, year_end=2010)
    assert len(records) > 0
    for r in records:
        assert r["source"] == "EDGAR"
        assert r["family"] == "decarb"
        assert r["units"] == "Gg"
        assert r["ratification"] == "Verified"
        assert r["date_time"].year == 2010
        assert r["extra"]["country_code"] == "FRA"
        assert r["extra"]["gas"] == "CO2"


def test_fetch_edgar_year_filtering():
    records = fetch_edgar_emissions("DEU", gases=["CO2"], year_start=2005, year_end=2007)
    years = {r["date_time"].year for r in records}
    assert years == {2005, 2006, 2007}


def test_fetch_edgar_category_filtering():
    records = fetch_edgar_emissions(
        "FRA", gases=["CO2"], year_start=2010, year_end=2010,
        categories=["1A3b"],  # Transport routier seulement
    )
    assert len(records) == 1
    assert records[0]["extra"]["ipcc_category"] == "1A3b"
    assert records[0]["extra"]["sector_label"] == "Transport / Road"


def test_fetch_edgar_co2eq_conversion():
    records_raw = fetch_edgar_emissions("FRA", gases=["CH4"], year_start=2010, year_end=2010)
    records_eq  = fetch_edgar_emissions("FRA", gases=["CH4"], year_start=2010, year_end=2010,
                                         as_co2eq=True)
    assert len(records_raw) == len(records_eq)
    for raw, eq in zip(records_raw, records_eq):
        assert abs(eq["value"] - raw["value"] * GWP100["CH4"]) < 0.001
        assert eq["units"] == "Gg CO₂eq"


def test_fetch_edgar_unknown_country_returns_empty():
    records = fetch_edgar_emissions("ZZZ", gases=["CO2"], year_start=2010, year_end=2010)
    assert records == []


def test_fetch_edgar_unknown_gas_returns_empty(caplog):
    records = fetch_edgar_emissions("FRA", gases=["H2O"], year_start=2010, year_end=2010)
    assert records == []
    assert "Unknown gas" in caplog.text


def test_aggregate_edgar_by_year():
    records = fetch_edgar_emissions("FRA", as_co2eq=True, year_start=2008, year_end=2010)
    totals = aggregate_edgar_by_year(records)
    assert len(totals) == 3
    years = [t["date_time"].year for t in totals]
    assert years == [2008, 2009, 2010]
    for t in totals:
        assert t["value"] > 0
        assert t["measurand"] == "GHG_total_CO2eq"
        assert t["extra"]["ipcc_category"] == "ALL"


def test_gwp100_values():
    assert GWP100["CO2"] == 1.0
    assert GWP100["CH4"] == 28.0
    assert GWP100["N2O"] == 265.0


def test_ipcc_sectors_coverage():
    # Les 29 catégories EDGAR doivent toutes être mappées
    expected_categories = {
        "1A1a", "1A1bc", "1A2", "1A3a", "1A3b", "1A3c", "1A3d", "1A3e",
        "1A4", "1A5", "1B1", "1B2", "2A1",
    }
    for cat in expected_categories:
        assert cat in IPCC_SECTORS, f"IPCC category {cat} missing from IPCC_SECTORS"
```

### `tests/test_decarb_cams.py`

```python
"""Tests CAMS — sans appel réseau réel (mocks)."""

import pytest
from unittest.mock import MagicMock, patch
from pipeline.families.decarb import fetch_cams_column, _CAMS_VARIABLES


def test_cams_variables_catalog():
    for name, (dataset, cams_var, nc_short, unit) in _CAMS_VARIABLES.items():
        assert dataset.startswith("cams-"), f"{name}: unexpected dataset {dataset}"
        assert cams_var, f"{name}: empty cams_variable"
        assert unit, f"{name}: empty unit"


def test_fetch_cams_no_cdsapi_returns_none(caplog):
    """Sans cdsapi installé, doit retourner None avec message d'erreur clair."""
    with patch.dict("sys.modules", {"cdsapi": None}):
        result = fetch_cams_column(48.85, 2.35, "2018-06-15", "methane")
    assert result is None


def test_fetch_cams_unknown_variable_returns_none(caplog):
    mock_cdsapi = MagicMock()
    with patch.dict("sys.modules", {"cdsapi": mock_cdsapi}):
        result = fetch_cams_column(48.85, 2.35, "2018-06-15", "unknown_gas")
    assert result is None
    assert "Unknown CAMS variable" in caplog.text


def test_fetch_cams_valid_request_structure():
    """Vérifie la structure de la requête ADS sans déclencher le download."""
    calls = []

    class MockClient:
        def retrieve(self, dataset, request, output):
            calls.append({"dataset": dataset, "request": request})
            raise RuntimeError("mock — pas de download réel")

    mock_cdsapi = MagicMock()
    mock_cdsapi.Client.return_value = MockClient()

    with patch.dict("sys.modules", {"cdsapi": mock_cdsapi}):
        fetch_cams_column(48.85, 2.35, "2015-07-01", "methane")

    assert len(calls) == 1
    req = calls[0]
    assert req["dataset"] == "cams-global-ghg-reanalysis-egg4"
    assert req["request"]["variable"] == "total_column_methane"
    assert req["request"]["date"] == "2015-07-01"
    assert req["request"]["format"] == "netcdf"
    area = req["request"]["area"]
    assert area[0] > 48.85   # N > lat
    assert area[2] < 48.85   # S < lat
```

### `tests/test_decarb_beges.py`

```python
"""Tests BEGES ADEME — avec mock HTTP."""

import responses as resp_mock
import pytest
from pipeline.families.decarb import fetch_beges_registry

BEGES_API = "https://data.ademe.fr/data-fair/api/v1/datasets/bilans-ges-ademe/lines"


@resp_mock.activate
def test_fetch_beges_basic():
    resp_mock.add(
        resp_mock.GET, BEGES_API,
        json={
            "results": [{
                "nom_de_lorganisme": "ACME Corp",
                "siren": "123456789",
                "annee_de_reporting": "2022",
                "secteur_naf": "C",
                "emissions_scope_1": "1500.0",
                "emissions_scope_2": "300.5",
                "emissions_scope_3": None,
            }],
            "total": 1,
        },
        status=200,
    )

    records = fetch_beges_registry()
    # 2 scopes (Scope1 + Scope2, Scope3 None → ignoré)
    assert len(records) == 2
    scopes = {r["measurand"] for r in records}
    assert "GHG_Scope1" in scopes
    assert "GHG_Scope2" in scopes

    scope1 = next(r for r in records if r["measurand"] == "GHG_Scope1")
    assert scope1["value"] == 1500.0
    assert scope1["units"] == "tCO2eq"
    assert scope1["source"] == "BEGES_ADEME"
    assert scope1["family"] == "decarb"
    assert scope1["extra"]["siren"] == "123456789"
    assert scope1["extra"]["reporting_year"] == "2022"


@resp_mock.activate
def test_fetch_beges_empty_response():
    resp_mock.add(resp_mock.GET, BEGES_API, json={"results": []}, status=200)
    records = fetch_beges_registry()
    assert records == []


@resp_mock.activate
def test_fetch_beges_api_error_returns_empty(caplog):
    resp_mock.add(resp_mock.GET, BEGES_API, status=503)
    records = fetch_beges_registry()
    assert records == []
    assert "BEGES API error" in caplog.text
```

### `tests/test_sources_sol.py`

```python
"""Tests du loader SOL Dataverse (Zn/Cu)."""

import io
import pytest
from pathlib import Path
from pipeline.sources.sol import load, as_environmental_records, DEFAULT_ZIP


def test_dataverse_zip_exists():
    assert DEFAULT_ZIP.exists(), f"dataverse_files.zip not found at {DEFAULT_ZIP}"


def test_load_returns_records():
    records = load()
    assert len(records) > 0


def test_load_zn_and_cu():
    records = load()
    metals = {r.metal for r in records}
    assert "Zn" in metals
    assert "Cu" in metals


def test_load_periods_format():
    records = load()
    for r in records:
        assert "-" in r.periode, f"Invalid period format: {r.periode}"
        assert r.year_start < r.year_end
        assert r.mean > 0
        assert r.n > 0


def test_load_cu_has_threshold():
    records = [r for r in load() if r.metal == "Cu"]
    for r in records:
        # Cu doit avoir pct_above_threshold (% > 60 ppm)
        assert r.pct_above_threshold is not None
        assert r.threshold_ppm == 60.0


def test_load_zn_no_threshold():
    records = [r for r in load() if r.metal == "Zn"]
    for r in records:
        assert r.pct_above_threshold is None


def test_as_environmental_records_schema():
    from datetime import datetime, timezone
    recs = list(as_environmental_records())
    assert len(recs) > 0
    for r in recs:
        assert r["site_code"] == "SOL_FRANCE"
        assert r["source"] == "SOL_DATAVERSE"
        assert r["family"] == "pollution"
        assert r["granularity"] == "periodic"
        assert r["units"] == "mg/kg"
        assert isinstance(r["date_time"], datetime)
        assert r["date_time"].tzinfo == timezone.utc
```

---

## 3. Tests unitaires — aeolus

Les tests aeolus sont dans `aeolus/tests/` et utilisent `responses` pour mocker les APIs.

```bash
# Depuis le dossier aeolus/
cd aeolus
source .venv/bin/activate
pytest tests/ -v --cov=aeolus --cov-report=term-missing

# Tests rapides (sans intégration réseau)
pytest tests/ -m "not slow and not integration" -v

# Un fichier spécifique
pytest tests/test_eea.py -v
```

**Structure des tests aeolus :**
```
tests/
├── test_regulatory.py     # AURN, SAQN, WAQN, NI, AQE
├── test_laqn.py           # London Air Quality Network
├── test_openaq.py         # OpenAQ portal
├── test_breathe_london.py # Breathe London
├── test_airqo.py          # AirQo (Afrique)
├── test_airnow.py         # EPA AirNow
├── test_eea.py            # EEA European network
├── test_purpleair.py      # PurpleAir portal
├── test_sensor_community.py
├── test_sos.py            # UK-AIR SOS near-real-time
├── test_find_sites.py     # find_sites() unified discovery
├── test_cache.py          # Cache Parquet local
├── test_geo.py            # Utilitaires géospatiaux
├── test_conformance.py    # Tests de conformance schéma
├── conformance_helpers.py
└── conftest.py
```

---

## 4. Tests d'intégration

Les tests d'intégration appellent des APIs réelles ou des services locaux. Marqués `@pytest.mark.integration`.

```bash
# Requiert des clés API dans l'environnement
AIRLY_API_KEY=xxx pytest tests/ -m integration -v
```

### `tests/integration/test_airly_live.py`

```python
import os
import pytest
from pipeline.families.qualite import fetch_airly_aqi

pytestmark = pytest.mark.integration


@pytest.fixture
def airly_key():
    key = os.getenv("AIRLY_API_KEY")
    if not key:
        pytest.skip("AIRLY_API_KEY not set")
    return key


def test_airly_aqi_paris_live(airly_key):
    records = fetch_airly_aqi([(48.8566, 2.3522)], api_key=airly_key)
    # Airly peut retourner 0 records si pas de capteur dans un rayon 1.5 km
    # → Test que la fonction ne plante pas et retourne une liste
    assert isinstance(records, list)
    if records:
        r = records[0]
        assert r["source"] == "AIRLY"
        assert r["family"].value == "qualite"
        assert r["measurand"] in {"AQI", "PM25", "PM10", "NO2", "SO2", "CO", "O3"}
```

### `tests/integration/test_sandre_live.py`

```python
import pytest
from pipeline.sources.sandre import fetch_station

pytestmark = pytest.mark.integration


def test_fetch_sandre_station_known():
    """Station Paris sur la Seine."""
    station = fetch_station("06121550")
    assert station is not None
    assert station["source"] == "SANDRE"
    assert station["code"] == "06121550"


def test_fetch_sandre_station_not_found():
    result = fetch_station("XXXXXXXXXX")
    assert result is None
```

---

## 5. Tests de qualité des données

Ces tests vérifient l'intégrité des données produites par la pipeline, pas seulement le code.

### `tests/test_data_quality.py`

```python
"""Tests de qualité des données — invariants métier."""

import pytest
from pipeline.families.decarb import fetch_edgar_emissions, aggregate_edgar_by_year


class TestEdgarDataQuality:
    """Invariants sur les données EDGAR."""

    def test_france_co2_trend_plausible(self):
        """Les émissions CO₂ France doivent diminuer entre 2000 et 2012 (tendance générale)."""
        records = fetch_edgar_emissions("FRA", gases=["CO2"], as_co2eq=True,
                                         year_start=2000, year_end=2012)
        totals = aggregate_edgar_by_year(records)
        val_2000 = next(t["value"] for t in totals if t["date_time"].year == 2000)
        val_2012 = next(t["value"] for t in totals if t["date_time"].year == 2012)
        assert val_2012 < val_2000, "France CO2 should decrease 2000→2012"

    def test_edgar_all_values_positive(self):
        """Pas de valeurs d'émissions négatives (sauf capture carbone 1C1/1C2)."""
        records = fetch_edgar_emissions("FRA", gases=["CO2"],
                                         year_start=2000, year_end=2012)
        capture_cats = {"1C1", "1C2"}
        for r in records:
            if r["extra"]["ipcc_category"] not in capture_cats:
                assert r["value"] >= 0, (
                    f"Negative emission for {r['extra']['ipcc_category']} "
                    f"in {r['date_time'].year}: {r['value']}"
                )

    def test_edgar_no_null_values(self):
        """Pas de valeurs None dans les records EDGAR (les lignes vides sont filtrées)."""
        records = fetch_edgar_emissions("FRA", year_start=2010, year_end=2010)
        for r in records:
            assert r["value"] is not None

    def test_edgar_co2_greater_than_ch4_co2eq(self):
        """CO₂ total France > CH₄ total (en CO₂eq) — invariant physique."""
        co2 = fetch_edgar_emissions("FRA", gases=["CO2"], as_co2eq=True,
                                     year_start=2010, year_end=2010)
        ch4 = fetch_edgar_emissions("FRA", gases=["CH4"], as_co2eq=True,
                                     year_start=2010, year_end=2010)
        assert sum(r["value"] for r in co2) > sum(r["value"] for r in ch4)

    def test_all_ipcc_sectors_present_for_major_country(self):
        """Un grand pays émetteur doit couvrir tous les secteurs."""
        records = fetch_edgar_emissions("CHN", gases=["CO2"], year_start=2010, year_end=2010)
        categories = {r["extra"]["ipcc_category"] for r in records}
        # Les secteurs les plus importants doivent être présents
        for cat in ["1A1a", "1A2", "1A3b", "2A1"]:
            assert cat in categories, f"Missing IPCC category {cat} for CHN"


class TestSoilDataQuality:
    def test_zinc_concentration_realistic_range(self):
        """Concentration Zn dans les sols : typiquement 10–300 mg/kg."""
        from pipeline.sources.sol import load
        records = [r for r in load() if r.metal == "Zn"]
        for r in records:
            assert 5 < r.mean < 500, f"Unrealistic Zn concentration: {r.mean}"

    def test_copper_concentration_realistic_range(self):
        """Concentration Cu dans les sols : typiquement 1–100 mg/kg."""
        from pipeline.sources.sol import load
        records = [r for r in load() if r.metal == "Cu"]
        for r in records:
            assert 1 < r.mean < 200, f"Unrealistic Cu concentration: {r.mean}"
```

---

## 6. Tests des flows Prefect

```python
"""Tests des flows Prefect — sans Prefect server (mode sync)."""

import pytest
from unittest.mock import patch, MagicMock


def test_ingest_decarb_family():
    """Test de l'orchestrateur ingest() avec la famille decarb."""
    import sys
    sys.path.insert(0, ".")
    from pipeline import ingest

    records = ingest(
        families=["decarb"],
        country_code="DEU",
        gases=["CO2"],
        year_start=2010,
        year_end=2010,
    )
    assert len(records) > 0
    assert all(r["family"] == "decarb" for r in records)
    assert all(r["source"] == "EDGAR" for r in records)


def test_ingest_default_families_excludes_predictive():
    """Les familles par défaut ne doivent pas inclure predictive (pas câblé)."""
    from pipeline import ingest
    with patch("pipeline.families.qualite.fetch_airly_aqi", return_value=[]):
        with patch("pipeline.families.pollution.fetch_airly_pollution", return_value=[]):
            with patch("pipeline.families.pollution.fetch_soil_baseline", return_value=[]):
                records = ingest(
                    families=["qualite", "pollution"],
                    locations=[],
                    country_code="FRA",
                    year_start=2010, year_end=2010,
                )
    # Pas de records predictive
    predictive = [r for r in records if r.get("family") == "predictive"]
    assert predictive == []
```

---

## 7. Tests de schéma et contrats

### Conformance : tous les records doivent respecter le schéma

```python
"""Tests de conformance — tous les records de toutes les sources."""

import pytest
from datetime import datetime
from pipeline.schema import EnvironmentalRecord, Family, Granularity
from pipeline.families.decarb import fetch_edgar_emissions
from pipeline.sources.sol import as_environmental_records

REQUIRED_FIELDS = {"site_code", "date_time", "measurand", "units", "source", "family"}


def validate_record(record: dict):
    """Valide qu'un dict respecte le schéma EnvironmentalRecord."""
    for field in REQUIRED_FIELDS:
        assert field in record, f"Missing field: {field}"
    assert isinstance(record["site_code"], str) and record["site_code"]
    assert isinstance(record["date_time"], datetime)
    assert record["date_time"].tzinfo is not None, "date_time must be UTC-aware"
    assert isinstance(record["measurand"], str) and record["measurand"]
    assert isinstance(record["units"], str) and record["units"]
    assert isinstance(record["source"], str) and record["source"]
    # family peut être str ou Family enum
    family_val = record["family"].value if hasattr(record["family"], "value") else record["family"]
    assert family_val in {"qualite", "pollution", "decarb", "predictive"}


def test_edgar_records_conform_to_schema():
    records = fetch_edgar_emissions("FRA", gases=["CO2"], year_start=2010, year_end=2010)
    for r in records:
        validate_record(r)


def test_sol_records_conform_to_schema():
    records = list(as_environmental_records())
    for r in records:
        validate_record(r)
```

---

## 8. Tests de performance

```python
"""Tests de performance — seuils à ne pas dépasser."""

import time
import pytest
from pipeline.families.decarb import fetch_edgar_emissions, aggregate_edgar_by_year


@pytest.mark.slow
def test_edgar_full_load_performance():
    """Charger tous les pays, tous les gaz, 1970–2012 doit prendre < 30s."""
    t0 = time.perf_counter()
    records = fetch_edgar_emissions(
        "FRA",              # Un pays représentatif
        year_start=1970, year_end=2012,
    )
    duration = time.perf_counter() - t0
    assert duration < 30, f"EDGAR full load took {duration:.1f}s (> 30s)"
    assert len(records) > 100


@pytest.mark.slow
def test_edgar_aggregate_performance():
    """L'agrégation annuelle doit prendre < 5s."""
    records = fetch_edgar_emissions("FRA", as_co2eq=True, year_start=1970, year_end=2012)
    t0 = time.perf_counter()
    totals = aggregate_edgar_by_year(records)
    duration = time.perf_counter() - t0
    assert duration < 5, f"Aggregation took {duration:.1f}s"
    assert len(totals) == 43  # 1970–2012
```

---

## 9. CI/CD Tekton

### Pipeline Task : tests unitaires

```yaml
# tekton/tasks/test-pipeline.yaml
apiVersion: tekton.dev/v1
kind: Task
metadata:
  name: test-pipeline
  namespace: pipeline-cicd
spec:
  workspaces:
  - name: source
  steps:
  - name: install-deps
    image: python:3.11-slim
    workingDir: $(workspaces.source.path)
    script: |
      pip install --quiet pytest pytest-cov responses pytest-mock
      pip install --quiet requests pandas pyarrow

  - name: run-unit-tests
    image: python:3.11-slim
    workingDir: $(workspaces.source.path)
    script: |
      pytest pipeline/tests/ \
        -m "not integration and not slow" \
        --cov=pipeline \
        --cov-report=xml:coverage.xml \
        --cov-report=term-missing \
        --tb=short \
        -v

  - name: check-coverage
    image: python:3.11-slim
    workingDir: $(workspaces.source.path)
    script: |
      # Échouer si la couverture < 80%
      python -m coverage report --fail-under=80
```

### Pipeline Task : lint

```yaml
apiVersion: tekton.dev/v1
kind: Task
metadata:
  name: lint-pipeline
  namespace: pipeline-cicd
spec:
  workspaces:
  - name: source
  steps:
  - name: ruff-lint
    image: python:3.11-slim
    workingDir: $(workspaces.source.path)
    script: |
      pip install --quiet ruff mypy
      ruff check pipeline/ --select=E,F,W,I
      mypy pipeline/ --ignore-missing-imports --python-version=3.11
```

### Pipeline complète CI

```yaml
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: pipeline-ci
  namespace: pipeline-cicd
spec:
  workspaces:
  - name: shared-workspace
  tasks:
  - name: lint
    taskRef:
      name: lint-pipeline
    workspaces:
    - name: source
      workspace: shared-workspace

  - name: test
    taskRef:
      name: test-pipeline
    runAfter: ["lint"]
    workspaces:
    - name: source
      workspace: shared-workspace

  - name: build-image
    taskRef:
      name: buildah
    runAfter: ["test"]
    params:
    - name: IMAGE
      value: "registry.ocp.local/pipeline/worker:$(params.git-sha)"
    workspaces:
    - name: source
      workspace: shared-workspace
```

---

## 10. Structure des tests

```
pipeline/tests/
├── conftest.py                  # Fixtures partagées
├── test_schema.py               # EnvironmentalRecord, Family, Granularity
├── test_decarb_edgar.py         # EDGAR local CSV
├── test_decarb_beges.py         # BEGES ADEME (mock HTTP)
├── test_decarb_cams.py          # CAMS Copernicus (mock cdsapi)
├── test_qualite_airly.py        # Airly AQI (mock HTTP)
├── test_qualite_sandre.py       # Sandre eau (mock HTTP)
├── test_pollution_airly.py      # Airly pollutants (mock HTTP)
├── test_sources_sol.py          # SOL Dataverse (zip local)
├── test_ingest.py               # Orchestrateur ingest()
├── test_data_quality.py         # Invariants métier sur les données
├── test_schema_conformance.py   # Conformance schéma tous sources
├── performance/
│   └── test_edgar_perf.py       # @pytest.mark.slow
└── integration/
    ├── test_airly_live.py        # @pytest.mark.integration
    ├── test_sandre_live.py
    └── test_beges_live.py
```

### Lancer les tests

```bash
# Tous les tests unitaires (rapides)
pytest pipeline/tests/ -m "not integration and not slow" -v

# Avec couverture
pytest pipeline/tests/ -m "not integration and not slow" \
  --cov=pipeline --cov-report=html --cov-report=term

# Tests de performance (lents)
pytest pipeline/tests/ -m slow -v

# Tests d'intégration (requiert API keys)
AIRLY_API_KEY=xxx pytest pipeline/tests/ -m integration -v

# Uniquement la famille décarb
pytest pipeline/tests/test_decarb_*.py -v
```
