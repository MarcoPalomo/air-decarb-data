# Air Quality & Decarbonization Data Platform

Plateforme de données environnementales de bout en bout : ingestion multi-sources, normalisation, traitement analytique et alimentation de modèles ML pour la qualité de l'air et la trajectoire carbone.

---

## Vue d'ensemble

La plateforme agrège quatre familles de données environnementales :

| Famille | Sources câblées | Statut |
|---|---|---|
| **Qualité** | Airly (IoT), EEA (réglementaire EU), Sandre (eau surface) | ✅ Opérationnel |
| **Pollution** | Airly (PM/gaz), EEA, SOL-Dataverse (métaux lourds Zn/Cu) | ✅ Opérationnel |
| **Décarb** | EDGAR v4.3.2 (GHG nationaux), BEGES ADEME, CAMS Copernicus | ✅ Opérationnel |
| **Prédictives** | Open-Meteo (météo), RTE (énergie), trafic | 🔧 À câbler |

Elle s'appuie sur [**aeolus**](../aeolus/README.md) comme bibliothèque de téléchargement air quality (AURN, SAQN, OpenAQ, PurpleAir, EEA, LAQN, AirQo, AirNow, Sensor.Community).

En production, les flows sont orchestrés par **Prefect** sur un cluster **OpenShift**, avec stockage **Iceberg/MinIO** et suivi ML via **MLflow**. Voir [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Prérequis

```
Python >= 3.11
```

Dépendances de base (ingestion + décarb local) :
```bash
pip install requests pandas pyarrow
```

Dépendances complètes avec aeolus :
```bash
cd aeolus && pip install -e ".[dev,all]"
```

Sources optionnelles :
```bash
pip install cdsapi xarray netCDF4   # CAMS Copernicus
pip install pyproj                  # Conversion Lambert 93 → WGS84 (Sandre)
pip install prefect mlflow duckdb polars  # Orchestration + compute
```

---

## Structure du projet

```
air-decarb-data/
│
├── aeolus/                     # Bibliothèque air quality (aeolus_aq)
│   ├── src/aeolus/
│   │   ├── sources/            # AURN, EEA, OpenAQ, PurpleAir, LAQN, AirQo...
│   │   ├── api.py              # Point d'entrée : aeolus.download()
│   │   ├── cache.py            # Cache Parquet local
│   │   └── metrics/            # aq_stats(), trend(), time_variation()
│   └── notebooks/              # 8 notebooks user-story
│
├── pipeline/                   # Pipeline environnementale (ce projet)
│   ├── __init__.py             # ingest() — point d'entrée unifié
│   ├── schema.py               # EnvironmentalRecord, SiteRecord, Family, Granularity
│   ├── families/
│   │   ├── qualite.py          # AQI Airly + EEA + Sandre eau
│   │   ├── pollution.py        # PM/gaz Airly + EEA + sol Dataverse
│   │   ├── decarb.py           # EDGAR + BEGES ADEME + CAMS Copernicus
│   │   └── decarb.py           # (prédictives — à venir)
│   └── sources/
│       ├── sandre.py           # Client Sandre eaufrance.fr (XML → dict)
│       └── sol.py              # Loader Dataverse ODS (Zn/Cu BDAT/RMQs)
│
├── edgar/                      # Données EDGAR v4.3.2 locales (1970–2012)
│   └── data/                   # co2.csv, ch4.csv, n2o.csv, co2_bio.csv
│
├── docs/                       # Documentation (ce dossier)
│   ├── README.md               # Ce fichier
│   ├── ARCHITECTURE.md         # Architecture complète
│   ├── PERFORMANCE.md          # Stratégie de performance
│   ├── SECURITY.md             # Sécurité et gestion des secrets
│   └── TEST.md                 # Stratégie de tests
│
├── dataverse_files.zip         # Archive BDAT (Zn/Cu statistiques sol France)
└── geospatial-data-catalogs/   # Catalogues AWS/Copernicus open geo-data
```

---

## Utilisation rapide

### Ingestion unifiée

```python
from pipeline import ingest

# Qualité + Pollution — capteurs Airly sur Paris et Lyon
records = ingest(
    families=["qualite", "pollution"],
    locations=[(48.8566, 2.3522), (45.7640, 4.8357)],
    airly_api_key="YOUR_KEY",
)

# Décarb — inventaire GHG France 2000–2012 en CO₂eq
records = ingest(
    families=["decarb"],
    country_code="FRA",
    gases=["CO2", "CH4", "N2O"],
    year_start=2000,
    year_end=2012,
)
```

### Famille Décarb en détail

```python
from pipeline.families.decarb import (
    fetch_edgar_emissions,
    aggregate_edgar_by_year,
    fetch_beges_registry,
    fetch_cams_column,
)

# EDGAR — émissions CO₂ France par secteur IPCC
records = fetch_edgar_emissions(
    country_code="FRA",
    gases=["CO2"],
    year_start=1990,
    year_end=2012,
    categories=["1A1a", "1A3b"],  # Électricité + Transport routier
)

# Agrégation annuelle toutes sources en CO₂eq
all_ghg = fetch_edgar_emissions("FRA", as_co2eq=True, year_start=2000)
totaux = aggregate_edgar_by_year(all_ghg)
# → [{date_time: 2000-01-01, value: 581234.5, units: "Gg CO₂eq"}, ...]

# BEGES — bilans carbone entreprises
bilans = fetch_beges_registry(siren="542107651", year=2022)

# CAMS — colonne atmosphérique CH₄ (requiert cdsapi + .cdsapirc)
col = fetch_cams_column(lat=48.85, lng=2.35, date="2018-06-15", variable="methane")
```

### Famille Qualité / Pollution

```python
from pipeline.families.qualite import fetch_airly_aqi, fetch_sandre_stations
from pipeline.families.pollution import fetch_soil_baseline, check_who_exceedances

# AQI temps réel via Airly
aqi = fetch_airly_aqi([(48.8566, 2.3522)], api_key="KEY")

# Stations eau de surface (Sandre) — département 75
stations = fetch_sandre_stations(dept_code="75", max_results=50)

# Baseline métaux lourds sols (Zn, Cu) depuis dataverse_files.zip
soil = fetch_soil_baseline()

# Filtrer dépassements WHO
exceedances = check_who_exceedances(records)
```

### aeolus — téléchargement air quality

```python
import aeolus
from datetime import datetime

# Stations EEA France dans un bbox
sites = aeolus.find_sites("EEA", bbox=(-5.0, 41.0, 10.0, 51.5))

# Données AURN NO₂ sur 30 jours
data = aeolus.download("AURN", ["MY1", "KC1"], last="30d")

# Stats réglementaires annuelles
stats = aeolus.aq_stats(data, year=2023)
```

---

## Schéma de données commun

Tous les records retournés par `ingest()` et les fonctions de famille respectent le schéma `EnvironmentalRecord` :

| Champ | Type | Description |
|---|---|---|
| `site_code` | `str` | Identifiant unique du site/station |
| `date_time` | `datetime` (UTC) | Horodatage UTC, intervalle gauche-fermé |
| `measurand` | `str` | Polluant ou GHG (`PM2.5`, `NO2`, `CO2`, `CH4`, `AQI`...) |
| `value` | `float \| None` | Valeur mesurée |
| `units` | `str` | Unité (`µg/m³`, `Gg`, `Gg CO₂eq`, `kg/m²`, `tCO2eq`) |
| `source` | `str` | Source (`AIRLY`, `EEA`, `EDGAR`, `CAMS`, `SANDRE`...) |
| `family` | `Family` | `qualite`, `pollution`, `decarb`, `predictive` |
| `granularity` | `Granularity` | `realtime`, `hourly`, `daily`, `periodic` |
| `ratification` | `str` | `Provisional`, `Verified`, `Reanalysis`, `Declared` |
| `extra` | `dict` | Métadonnées spécifiques à la source |

---

## Variables d'environnement

| Variable | Description | Requis |
|---|---|---|
| `AIRLY_API_KEY` | Clé Airly IoT | Familles qualite/pollution |
| `OPENAQ_API_KEY` | Clé OpenAQ | aeolus/openaq |
| `PURPLEAIR_API_KEY` | Clé PurpleAir | aeolus/purpleair |
| `BL_API_KEY` | Breathe London | aeolus/breathe_london |
| `AIRNOW_API_KEY` | EPA AirNow | aeolus/airnow |
| `AIRQO_API_KEY` | AirQo Afrique | aeolus/airqo |

CAMS : fichier `~/.cdsapirc` (voir [ARCHITECTURE.md](ARCHITECTURE.md#cams-copernicus)).

---

## Codes pays EDGAR

EDGAR utilise des codes ISO 3166-1 **alpha-3** : `FRA`, `DEU`, `GBR`, `CHN`, `USA`, `IND`...

```python
# Comparer France, Allemagne, Royaume-Uni
for country in ["FRA", "DEU", "GBR"]:
    recs = fetch_edgar_emissions(country, as_co2eq=True, year_start=2010, year_end=2012)
    totals = aggregate_edgar_by_year(recs)
    print(country, [f"{t['value']:.0f}" for t in totals])
```

---

## Liens

- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture complète OpenShift + flux de données
- [PERFORMANCE.md](PERFORMANCE.md) — Stratégie de performance et tuning
- [SECURITY.md](SECURITY.md) — Sécurité, RBAC, gestion des secrets
- [TEST.md](TEST.md) — Stratégie de tests et CI/CD
- [aeolus CLAUDE.md](../aeolus/CLAUDE.md) — Guide de développement aeolus
