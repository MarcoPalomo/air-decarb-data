# Architecture — Air Quality & Decarbonization Data Platform

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Couche Ingestion](#2-couche-ingestion)
3. [Familles de données](#3-familles-de-données)
4. [Couche Processing](#4-couche-processing--feature-engineering)
5. [Couche ML](#5-couche-ml)
6. [Infra OpenShift](#6-infra-openshift)
7. [Flux de données end-to-end](#7-flux-de-données-end-to-end)
8. [Catalogue Iceberg](#8-catalogue-iceberg--nessie)
9. [Conventions de schéma](#9-conventions-de-schéma)
10. [Choix technologiques](#10-choix-technologiques-et-justifications)

---

## 1. Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────┐
│                        INGESTION                            │
│   Capteurs IoT · APIs publiques · Fichiers réglementaires   │
└───────────┬────────────┬──────────────┬────────────────────-┘
            │            │              │
     ┌──────┴──┐   ┌─────┴──┐   ┌──────┴──┐   ┌─────────────┐
     │ DÉCARB  │   │QUALITÉ │   │PRÉDICTIF│   │ POLLUTION   │
     │CO₂/GES  │   │AQI/eau │   │Météo/RTE│   │PM/NOₓ/métaux│
     └────┬────┘   └────┬───┘   └────┬────┘   └──────┬──────┘
          └─────────────┴────────────┴────────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            │   PROCESSING & FEATURE ENGINEERING │
            │  Normalisation · Anomalies · Aggr. │
            └─────────────────┬─────────────────┘
                              │
              ┌───────────────┼──────────────┐
              │               │              │
       ┌──────┴──┐    ┌───────┴────┐  ┌─────┴──────┐
       │FORECAST │    │  ANOMALY   │  │  SCORING   │
       │CO₂ traj │    │  DETECT.   │  │ Qualité env│
       └──────┬──┘    └───────┬────┘  └─────┬──────┘
              │               │              │
       ┌──────┴──┐    ┌───────┴────┐  ┌─────┴──────┐
       │API/Dash │    │  Alertes   │  │Conformité  │
       └─────────┘    └────────────┘  └────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            │    OBSERVABILITÉ & GOUVERNANCE      │
            │  Data lineage · Drift · MLflow/DVC  │
            └───────────────────────────────────--┘
```

---

## 2. Couche Ingestion

### Sources par famille

#### Famille Qualité
| Source | Type | Couverture | Fréquence | Clé API |
|---|---|---|---|---|
| **Airly** | IoT capteurs | Europe, réseau dense | Temps réel (<15 min) | `AIRLY_API_KEY` |
| **EEA** (via aeolus) | Réglementaire EU | 40+ pays, 7 000+ stations | Horaire/journalier | Non |
| **OpenAQ** (via aeolus) | Agrégateur global | 100+ pays | Variable | `OPENAQ_API_KEY` |
| **Sandre** | Eau de surface | France, 39 987 stations | Ponctuel (référentiel) | Non |
| **LAQN** (via aeolus) | Réglementaire | Londres (~250 sites) | Horaire | Non |

#### Famille Pollution
| Source | Type | Couverture | Fréquence |
|---|---|---|---|
| **Airly** | IoT | PM1, PM2.5, PM10, NO₂, SO₂, CO, O₃ | Temps réel |
| **EEA** (via aeolus) | Réglementaire | NO₂, PM, O₃, SO₂, benzène, métaux traces | Horaire/journalier |
| **PurpleAir** (via aeolus) | IoT global | PM1, PM2.5, PM10 (30 000+ capteurs) | Temps réel |
| **SOL-Dataverse** | Fichier statique | Zn, Cu sols agricoles France | Quinquennal (1990–2009) |

#### Famille Décarb
| Source | Type | Couverture | Fréquence |
|---|---|---|---|
| **EDGAR v4.3.2** | CSV local | 225 pays, 29 secteurs IPCC, 4 GHG | Annuel (1970–2012) |
| **BEGES ADEME** | API REST | Bilans GES entreprises FR | Annuel (déclarations) |
| **CAMS EGG4** | NetCDF/cdsapi | Colonnes CH₄/CO₂ globales | 6-horaire (2003–2020) |
| **CAMS EAC4** | NetCDF/cdsapi | NO₂, O₃ atmosphériques | 3-horaire (2003–présent) |

#### Famille Prédictive (à câbler)
| Source | Données |
|---|---|
| **Open-Meteo** | Température, vent, précipitations, rayonnement |
| **RTE Open Data** | Production/consommation électrique FR, mix énergétique |
| **trafic** (TOMTOM/HERE) | Congestion, flux véhicules |

### Configuration CAMS Copernicus

```bash
# Installation
pip install cdsapi xarray netCDF4

# ~/.cdsapirc
url: https://ads.atmosphere.copernicus.eu/api/v2
key: <UID>:<API-key>
# Compte gratuit sur : https://ads.atmosphere.copernicus.eu/
```

Variables disponibles et leurs datasets :
```
methane          → cams-global-ghg-reanalysis-egg4  (2003–2020)
carbon_dioxide   → cams-global-ghg-reanalysis-egg4  (2003–2020)
nitrogen_dioxide → cams-global-reanalysis-eac4       (2003–présent)
ozone            → cams-global-reanalysis-eac4       (2003–présent)
```

---

## 3. Familles de données

### Schéma commun : `EnvironmentalRecord`

Défini dans `pipeline/schema.py`. Tous les records produits par toutes les sources respectent ce schéma :

```python
@dataclass
class EnvironmentalRecord:
    site_code:    str          # Identifiant unique du site
    date_time:    datetime     # UTC-aware, intervalle gauche-fermé
    measurand:    str          # "PM2.5", "NO2", "CO2", "CH4", "AQI"...
    value:        float | None
    units:        str          # "µg/m³", "Gg", "Gg CO₂eq", "kg/m²"
    source:       str          # "AIRLY", "EEA", "EDGAR", "CAMS"...
    family:       Family       # qualite | pollution | decarb | predictive
    granularity:  Granularity  # realtime | hourly | daily | periodic
    ratification: str          # Provisional | Verified | Reanalysis | Declared
    extra:        dict         # Métadonnées spécifiques à la source
```

### Secteurs IPCC (EDGAR)

```
1A1a  Energy / Electricity & Heat
1A1bc Energy / Petroleum refining
1A2   Energy / Manufacturing & Construction
1A3b  Transport / Road
1A3a  Transport / Aviation
1A3d  Transport / Navigation
1B1   Fugitive / Coal
1B2   Fugitive / Oil & Gas
2A1   Industry / Cement
2B    Industry / Chemicals
2C    Industry / Metals
3B    Agriculture / Livestock
3D    Agriculture / Rice & Soils
4D4   Waste / Wastewater
6C    Waste / Incineration
...   (29 catégories au total)
```

### GWP100 (AR5 IPCC 2014)

| Gaz | GWP100 | Source EDGAR |
|---|---|---|
| CO₂ | 1.0 | `co2_excl_short-cycle_org_c.csv` |
| CO₂ biogénique | 1.0 | `co2_org-short-cycle_c.csv` |
| CH₄ | 28.0 | `ch4.csv` |
| N₂O | 265.0 | `n2o.csv` |

---

## 4. Couche Processing & Feature Engineering

> **Statut : À implémenter** — les modules ci-dessous seront dans `pipeline/processing/`

### Modules prévus

```
pipeline/processing/
├── normalise.py       # Harmonisation unités, fuseau horaire, outliers
├── aggregate.py       # Agrégations spatiotemporelles (hourly→daily, bbox→grid)
├── anomaly.py         # Détection de pics (z-score, IQR, isolation forest)
├── enrich.py          # Jointure spatiale (stations ↔ population, industrie)
└── quality.py         # Data capture, completeness, WHO exceedances
```

### Agrégation spatiotemporelle

```
Raw IoT (15 min, point)
    → Horaire (moyenne pondérée, data capture ≥ 75%)
    → Journalier (rolling 24h, percentiles P50/P98)
    → Grille 0.1° × 0.1° (kriging ou IDW interpolation)
    → Zone administrative (NUTS3 / commune)
```

### Détection d'anomalies

Trois niveaux selon la granularité et l'urgence :
- **Seuils réglementaires** : dépassement WHO/EU, alertes immédiates
- **z-score glissant** : pic statistique sur fenêtre de 7 jours
- **Isolation Forest** : anomalies multi-variées (pour la couche ML)

---

## 5. Couche ML

> **Statut : À implémenter** — orchestrée via Prefect + MLflow

### Modèles prévus

| Tâche | Approche | Input | Output |
|---|---|---|---|
| **Forecasting CO₂** | LSTM / XGBoost + Theil-Sen | EDGAR + CAMS historique | Trajectoire 5 ans |
| **Anomaly Detection** | Isolation Forest, LSTM-AE | Séries IoT multi-variées | Score d'anomalie |
| **Scoring qualité** | Règles + XGBoost | AQI + WHO exceedances | Score 0–100 + badge |

### Tracking MLflow

- **Backend store** : PostgreSQL (`mlflow-db` dans `pipeline-orchestration`)
- **Artifact store** : MinIO bucket `mlflow-artifacts/`
- Chaque run Prefect log : paramètres, métriques, modèle sérialisé

---

## 6. Infra OpenShift

L'ensemble de la plateforme tourne sur un cluster **OpenShift** (OCP 4.x), organisé en 4 namespaces aux responsabilités séparées.

### Namespace : `pipeline-cicd`

Responsabilité : automatisation CI/CD du code vers le cluster.

```
┌─────────────────────────────────────────────────────┐
│  ns: pipeline-cicd                                  │
│                                                     │
│  ┌──────────────────┐  ┌──────────────────────────┐ │
│  │ Tekton Pipelines │  │    Tekton Triggers       │ │
│  │ Build · Test     │  │  Git webhook →           │ │
│  │ Push image       │  │  PipelineRun             │ │
│  └──────────────────┘  └──────────────────────────┘ │
│  ┌──────────────────────────────────────────────┐   │
│  │          ArgoCD (GitOps)                     │   │
│  │  Sync manifests repo → cluster               │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

**Flux CI/CD :**
1. Push git → Tekton Trigger intercepte le webhook
2. `PipelineRun` : lint → test → build image → push registry OCP
3. ArgoCD détecte le changement de tag dans le repo Helm/Kustomize
4. ArgoCD applique les manifests dans les namespaces cibles

### Namespace : `pipeline-orchestration`

Responsabilité : scheduling des flows de données et tracking ML.

```
┌─────────────────────────────────────────────────────┐
│  ns: pipeline-orchestration                         │
│                                                     │
│  ┌──────────────────┐  ┌──────────────────────────┐ │
│  │  Prefect Server  │  │     Prefect Flows        │ │
│  │  UI · API        │  │  décarb · qualite        │ │
│  │  Kubernetes      │  │  pollution · predictif   │ │
│  │  Work Pool       │  │                          │ │
│  └──────────────────┘  └──────────────────────────┘ │
│  ┌──────────────────┐  ┌──────────────────────────┐ │
│  │     MLflow       │  │     PostgreSQL           │ │
│  │  Tracking ·      │  │  Backend Prefect         │ │
│  │  Registry        │  │  + MLflow metadata       │ │
│  └──────────────────┘  └──────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

**Prefect Kubernetes Work Pool :** chaque flow step crée un Kubernetes Job dans `pipeline-compute`. Les workers sont éphémères — ils se créent, exécutent, et se suppriment.

### Namespace : `pipeline-compute`

Responsabilité : exécution des Prefect worker pods (Jobs éphémères).

**Important :** DuckDB et Polars sont des bibliothèques Python embarquées dans les images worker — pas des services déployés. Ce namespace est un namespace d'exécution, pas un namespace de services persistants.

```
┌─────────────────────────────────────────────────────┐
│  ns: pipeline-compute  (worker pods éphémères)      │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  Prefect Worker Pod (Job Kubernetes)        │   │
│  │                                             │   │
│  │  [Python env]                               │   │
│  │   ├── pipeline/families/decarb.py           │   │
│  │   ├── DuckDB (in-process)   ← bibliothèque  │   │
│  │   ├── Polars (in-process)   ← bibliothèque  │   │
│  │   └── aeolus, requests, cdsapi...           │   │
│  │                                             │   │
│  │  → Lit depuis MinIO (Iceberg)               │   │
│  │  → Écrit vers MinIO (Iceberg)               │   │
│  │  → Log métriques vers MLflow                │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
│  ┌─────────────────┐  (optionnel, scale-out)       │
│  │  Ray / Dask     │  KubeRay operator             │
│  │  cluster pods   │  activé si volume > 1 pod     │
│  └─────────────────┘                               │
└─────────────────────────────────────────────────────┘
```

**Ray vs Dask :**
- **Dask** : préféré pour les flux Prefect — intégration native, plus léger
- **Ray** (KubeRay operator) : si le volume de données dépasse la capacité d'un pod unique (~100 GB+)

### Namespace : `pipeline-storage`

Responsabilité : persistance de toutes les données.

```
┌─────────────────────────────────────────────────────────────┐
│  ns: pipeline-storage                                       │
│                                                             │
│  ┌──────────────────────┐  ┌──────────────────────────┐    │
│  │   MinIO (ODF)        │  │       Nessie             │    │
│  │   Object store S3    │  │  REST Catalog Iceberg    │    │
│  │   OpenShift Data     │  │  Git-like table branching│    │
│  │   Foundation         │  │                          │    │
│  └──────────────────────┘  └──────────────────────────┘    │
│                                                             │
│  ┌──────────────────────┐  ┌──────────────────────────┐    │
│  │  Iceberg / Delta     │  │      TimescaleDB         │    │
│  │  Table format ACID   │  │  Time-series SQL         │    │
│  │  Time travel         │  │  Capteurs IoT temps réel │    │
│  │  Schema evolution    │  │  Hypertables + compression│   │
│  └──────────────────────┘  └──────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Structure des buckets MinIO

```
minio/
├── raw/                        # Données brutes ingérées (immuable)
│   ├── airly/YYYY/MM/DD/
│   ├── eea/YYYY/MM/DD/
│   ├── cams/YYYY/MM/DD/
│   └── sandre/
│
├── iceberg/                    # Tables Iceberg (format Parquet + metadata)
│   ├── qualite/
│   │   ├── measurements/       # Table principale air quality
│   │   └── stations/           # Métadonnées stations
│   ├── pollution/
│   │   ├── measurements/
│   │   └── soil_baseline/
│   ├── decarb/
│   │   ├── edgar_emissions/
│   │   ├── beges_declarations/
│   │   └── cams_columns/
│   └── predictive/
│
├── mlflow-artifacts/           # Modèles, plots, metrics MLflow
│   └── experiments/
│
└── cache/                      # Cache Parquet aeolus (ephémère)
    └── aeolus/
```

---

## 7. Flux de données end-to-end

```
Git push
  │
  ├─[Tekton]──→ Build image worker → Push OCP registry
  │
  └─[ArgoCD]──→ Update manifests → Deploy

Prefect Schedule (cron)
  │
  └─[Prefect Server]──→ PipelineRun (Kubernetes Job)
        │
        └─[Worker Pod]
              │
              ├─ Ingestion
              │    ├─ fetch_airly_aqi()         → Airly API
              │    ├─ aeolus.download("EEA")    → EEA SPARQL
              │    ├─ fetch_edgar_emissions()   → Local CSV
              │    └─ fetch_cams_column()       → CAMS ADS
              │
              ├─ Normalisation (EnvironmentalRecord schema)
              │
              ├─ Processing
              │    ├─ DuckDB : agrégations SQL in-proc
              │    └─ Polars : transformations DataFrame lazy
              │
              ├─ Écriture Iceberg
              │    └─ PyIceberg → Nessie catalog → MinIO
              │
              ├─ Écriture TimescaleDB (IoT temps réel)
              │
              └─ MLflow log (métriques, data quality)
```

---

## 8. Catalogue Iceberg : Nessie

**Pourquoi Nessie ?**
- REST Catalog compatible avec PyIceberg, DuckDB, Polars, Trino
- Branching Git-like : développer sur une branche, merger en prod sans downtime
- Léger : 1 pod, backend PostgreSQL ou RocksDB
- Open source (Project Nessie par Dremio)

**Configuration PyIceberg avec Nessie :**

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="nessie",
    **{
        "uri": "http://nessie.pipeline-storage.svc:19120/iceberg",
        "warehouse": "s3://iceberg/",
        "s3.endpoint": "http://minio.pipeline-storage.svc:9000",
        "s3.access-key-id": "...",
        "s3.secret-access-key": "...",
    }
)

table = catalog.load_table("decarb.edgar_emissions")
```

**Configuration DuckDB avec Nessie + Iceberg :**

```sql
INSTALL iceberg; LOAD iceberg;
CREATE SECRET minio (...);  -- endpoint MinIO
SELECT * FROM iceberg_scan('s3://iceberg/decarb/edgar_emissions/');
```

---

## 9. Conventions de schéma

### Nommage des tables Iceberg

```
{namespace}.{family}_{entity}
ex:
  decarb.edgar_emissions
  qualite.measurements
  qualite.stations
  pollution.soil_baseline
```

### Partitionnement

| Table | Partition |
|---|---|
| `*.measurements` | `year(date_time), source` |
| `decarb.edgar_emissions` | `country_code, year(date_time)` |
| `decarb.cams_columns` | `year(date_time), variable` |

### Timestamps

- Tous les `date_time` sont **UTC-aware**
- Les intervalles sont **gauche-fermés** : l'horodatage `13:00` représente `[12:00, 13:00)`
- `created_at` = moment de l'ingestion (non modifié lors des re-runs)

---

## 10. Choix technologiques et justifications

| Composant | Choix | Alternative rejetée | Raison |
|---|---|---|---|
| Orchestration | **Prefect 3** | Airflow | Python-natif, flows = fonctions décorées, moins de XML |
| Table format | **Iceberg** | Delta Lake | Meilleur support multi-engines (DuckDB+Polars+Trino) |
| Catalog | **Nessie** | Hive Metastore | Léger (1 pod), Git branching, REST API standard |
| Object store | **MinIO/ODF** | AWS S3 | On-premise, intégré OCP, S3-compat |
| Compute in-proc | **DuckDB** | Spark SQL | 10–100× plus rapide pour <100 GB, zéro cluster |
| DataFrame | **Polars** | Pandas | Multi-thread natif, lazy eval, ~5× plus rapide |
| Time-series | **TimescaleDB** | InfluxDB | SQL standard, compression, extension PostgreSQL |
| CI/CD | **Tekton + ArgoCD** | Jenkins + Helm | Natif OCP, GitOps déclaratif |
| ML tracking | **MLflow** | Weights & Biases | Open source, self-hosted, intégration simple |
| Scale-out | **Dask** (optionnel Ray) | Spark | Plus léger que Spark pour Python, intégration Prefect |
