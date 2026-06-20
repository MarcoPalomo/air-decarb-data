# Performance — Air Quality & Decarbonization Data Platform

## Table des matières

1. [Objectifs et SLOs](#1-objectifs-et-slos)
2. [Couche Compute : DuckDB + Polars](#2-couche-compute--duckdb--polars)
3. [Couche Stockage : Iceberg + MinIO](#3-couche-stockage--iceberg--minio)
4. [Couche Temps-réel : TimescaleDB](#4-couche-temps-réel--timescaledb)
5. [Scale-out : Ray / Dask](#5-scale-out--ray--dask)
6. [Volumes attendus](#6-volumes-attendus)
7. [Tuning par composant](#7-tuning-par-composant)
8. [Benchmarks de référence](#8-benchmarks-de-référence)
9. [Monitoring des performances](#9-monitoring-des-performances)

---

## 1. Objectifs et SLOs

| Métrique | Cible | Mesure |
|---|---|---|
| Ingestion IoT (Airly, 100 points) | < 30 s | Durée d'un `fetch_airly_aqi()` |
| Ingestion EEA (France, 30 jours) | < 5 min | Durée `aeolus.download("EEA", ...)` |
| Ingestion CAMS (1 point, 1 jour) | < 2 min | Download + extraction NetCDF |
| EDGAR (tous pays, tous gaz) | < 10 s | Lecture 4 CSV locaux |
| Agrégation DuckDB (100M lignes) | < 60 s | GROUP BY secteur + année |
| Écriture Iceberg (1M lignes) | < 30 s | PyIceberg → MinIO |
| Requête IoT TimescaleDB (30 jours) | < 500 ms | SELECT sur hypertable indexée |
| Flow Prefect complet (toutes familles) | < 30 min | Run bout en bout quotidien |

---

## 2. Couche Compute : DuckDB + Polars

### Pourquoi pas Spark ?

Spark est conçu pour des clusters distribués et des données à l'échelle du pétaoctet. Pour cette plateforme :
- Les volumes attendus sont de l'ordre du **gigaoctet à quelques dizaines de gigaoctets** par run
- Spark impose un overhead de 30–60 s au démarrage (JVM, driver, executors)
- DuckDB traite 10 GB en quelques secondes en in-process, sans réseau ni sérialisation

### DuckDB

**Mode d'utilisation :** bibliothèque Python in-process dans les pods Prefect. Pas de serveur.

**Forces :**
- Lecture Parquet/Iceberg directement depuis MinIO (S3-compat) sans copie locale
- Vectorized execution (SIMD), columnar storage
- Extensions : spatial (PostGIS-like), JSON, httpfs (lecture S3 directe)
- Parallélisme automatique sur tous les cœurs du pod

**Patterns recommandés :**

```python
import duckdb

con = duckdb.connect()

# Lire directement depuis MinIO/Iceberg sans staging
con.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='minio.pipeline-storage.svc:9000';
    SET s3_access_key_id='...';
    SET s3_secret_access_key='...';
    SET s3_use_ssl=false;
""")

# Agrégation annuelle GHG par pays et secteur
result = con.execute("""
    SELECT
        extra->>'country_code'  AS country,
        YEAR(date_time)         AS year,
        extra->>'sector_label'  AS sector,
        SUM(value)              AS total_gg_co2eq
    FROM parquet_scan('s3://iceberg/decarb/edgar_emissions/**/*.parquet')
    WHERE measurand = 'GHG_CO2eq'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""").df()

# Jointure spatiale (extension spatial)
con.execute("INSTALL spatial; LOAD spatial;")
nearby = con.execute("""
    SELECT s.site_code, s.site_name,
           ST_Distance(ST_Point(s.longitude, s.latitude),
                       ST_Point(2.3522, 48.8566)) * 111 AS dist_km
    FROM stations s
    WHERE dist_km < 10
    ORDER BY dist_km
""").df()
```

**Tuning DuckDB :**

```python
con = duckdb.connect()
con.execute(f"SET threads={os.cpu_count()}")
con.execute("SET memory_limit='8GB'")          # Adapter au pod request
con.execute("SET temp_directory='/tmp/duck'")  # Spill to disk si OOM
con.execute("SET enable_progress_bar=false")   # Désactiver en prod
```

### Polars

**Mode d'utilisation :** transformations DataFrame lazy dans les pods Prefect, avant/après DuckDB.

**Forces :**
- Rust natif, multi-thread automatique
- Lazy evaluation : optimisation du plan d'exécution avant exécution
- Streaming pour les datasets qui dépassent la RAM
- Interop native avec Arrow (→ DuckDB, → PyIceberg)

**Patterns recommandés :**

```python
import polars as pl

# Lazy scan Parquet depuis MinIO
df = (
    pl.scan_parquet("s3://iceberg/qualite/measurements/**/*.parquet",
                    storage_options={"endpoint_url": "http://minio:9000", ...})
    .filter(pl.col("source") == "EEA")
    .filter(pl.col("measurand") == "NO2")
    .filter(pl.col("date_time").dt.year() >= 2020)
    .group_by(["site_code", pl.col("date_time").dt.year().alias("year")])
    .agg(
        pl.col("value").mean().alias("annual_mean"),
        pl.col("value").quantile(0.98).alias("p98"),
        pl.col("value").count().alias("data_points"),
    )
    .sort(["year", "annual_mean"], descending=[True, True])
    .collect(streaming=True)   # Streaming si > RAM disponible
)

# Interop Arrow → DuckDB sans copie mémoire
arrow_table = df.to_arrow()
con.register("polars_result", arrow_table)
con.execute("SELECT * FROM polars_result WHERE annual_mean > 40")
```

**Tuning Polars :**

```python
# Streaming pour les grands volumes
df.collect(streaming=True)

# Forcer le parallélisme
pl.Config.set_tbl_rows(20)
pl.Config.set_streaming_chunk_size(50_000)

# Éviter les .collect() intermédiaires dans une pipeline lazy
result = (
    df.lazy()
    .filter(...)
    .group_by(...)
    .agg(...)
    .collect()   # Un seul collect à la fin
)
```

---

## 3. Couche Stockage : Iceberg + MinIO

### Partitionnement Iceberg

Le choix de partition est critique pour les performances de lecture.

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestamptzType, FloatType, DoubleType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, IdentityTransform

# Schéma de la table measurements
schema = Schema(
    NestedField(1, "site_code",   StringType(),      required=True),
    NestedField(2, "date_time",   TimestamptzType(), required=True),
    NestedField(3, "measurand",   StringType(),      required=True),
    NestedField(4, "value",       DoubleType()),
    NestedField(5, "units",       StringType()),
    NestedField(6, "source",      StringType()),
    NestedField(7, "family",      StringType()),
    NestedField(8, "ratification", StringType()),
)

# Partition par année + source → bonne sélectivité pour les requêtes courantes
partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=YearTransform(),     name="year"),
    PartitionField(source_id=6, field_id=1001, transform=IdentityTransform(), name="source"),
)
```

**Règles de partitionnement :**

| Table | Partition recommandée | Raison |
|---|---|---|
| `qualite.measurements` | `year(date_time), source` | Requêtes temporelles + filtres source |
| `pollution.measurements` | `year(date_time), source` | Idem |
| `decarb.edgar_emissions` | `country_code, year(date_time)` | Filtres pays fréquents |
| `decarb.cams_columns` | `year(date_time), variable` | Filtres variable fréquents |
| `qualite.stations` | pas de partition | Table petite (<1 MB) |

### Compaction Iceberg

Les petits fichiers Parquet (issus d'ingestions fréquentes) dégradent les performances. Lancer une compaction hebdomadaire :

```python
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table.rewrite import RewriteDataFiles

catalog = RestCatalog(...)
table = catalog.load_table("qualite.measurements")

# Compaction : regrouper les fichiers < 128 MB
table.rewrite_data_files(
    strategy="binpack",
    options={"target-file-size-bytes": str(128 * 1024 * 1024)},
)

# Expiration des snapshots anciens (garder 30 jours)
table.expire_snapshots(older_than_ms=30 * 24 * 60 * 60 * 1000)
```

### MinIO : configuration performance

```yaml
# Recommandations ODF/MinIO pour les workloads analytiques
erasure_coding_parity: 2          # Balance durabilité/performance
min_multipart_part_size: 64MB     # Uploads multiparts pour Iceberg
concurrent_requests: 32           # Parallélisme PyIceberg
```

---

## 4. Couche Temps-réel : TimescaleDB

TimescaleDB est utilisé pour les données IoT à haute fréquence (Airly, PurpleAir) où la latence de lecture doit être < 500 ms.

### Création des hypertables

```sql
-- Table principale capteurs
CREATE TABLE sensor_readings (
    time        TIMESTAMPTZ NOT NULL,
    site_code   TEXT        NOT NULL,
    measurand   TEXT        NOT NULL,
    value       DOUBLE PRECISION,
    units       TEXT,
    source      TEXT,
    ratification TEXT
);

-- Conversion en hypertable (partitionnement automatique par temps)
SELECT create_hypertable('sensor_readings', 'time',
    chunk_time_interval => INTERVAL '7 days');

-- Index composé pour les requêtes site + polluant + période
CREATE INDEX idx_sensor_site_meas ON sensor_readings (site_code, measurand, time DESC);

-- Compression automatique des chunks > 7 jours
ALTER TABLE sensor_readings SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'site_code, measurand'
);

SELECT add_compression_policy('sensor_readings', INTERVAL '7 days');
```

### Continuous Aggregates (materialized views temps-réel)

```sql
-- Vue matérialisée horaire (recalcul automatique toutes les 30 min)
CREATE MATERIALIZED VIEW sensor_hourly
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 hour', time)  AS hour,
        site_code,
        measurand,
        AVG(value)                   AS mean,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median,
        COUNT(*)                     AS data_points
    FROM sensor_readings
    GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

-- Vue journalière
CREATE MATERIALIZED VIEW sensor_daily
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', hour) AS day,
        site_code,
        measurand,
        AVG(mean)  AS daily_mean,
        MAX(mean)  AS daily_max,
        SUM(data_points) AS total_points
    FROM sensor_hourly
    GROUP BY 1, 2, 3
WITH NO DATA;
```

### Requêtes types

```sql
-- Dernières 24h pour un site
SELECT hour, measurand, mean
FROM sensor_hourly
WHERE site_code = 'AIRLY_48.8566_2.3522'
  AND hour >= NOW() - INTERVAL '24 hours'
ORDER BY hour DESC;

-- Dépassements WHO NO₂ (40 µg/m³ annuel)
SELECT site_code, day, daily_mean
FROM sensor_daily
WHERE measurand = 'NO2'
  AND daily_mean > 40
  AND day >= DATE_TRUNC('year', NOW())
ORDER BY daily_mean DESC;
```

---

## 5. Scale-out : Ray / Dask

**Par défaut : 1 pod Prefect suffit** pour les volumes attendus (< 10 GB par run).

Le scale-out est activé uniquement si un run dépasse les limites du pod.

### Dask (préféré pour Prefect)

```python
from dask_kubernetes.operator import KubeCluster
from prefect_dask import DaskTaskRunner

# Dans un Prefect flow
@flow(task_runner=DaskTaskRunner(
    cluster_class="dask_kubernetes.operator.KubeCluster",
    cluster_kwargs={
        "name": "dask-decarb",
        "namespace": "pipeline-compute",
        "image": "registry.ocp.local/pipeline/worker:latest",
        "n_workers": 4,
        "resources": {"requests": {"memory": "4Gi", "cpu": "2"}},
    }
))
def run_heavy_processing():
    ...
```

### Ray (si volume > 100 GB)

```yaml
# KubeRay RayCluster manifest
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: ray-pipeline
  namespace: pipeline-compute
spec:
  headGroupSpec:
    rayStartParams: {num-cpus: "0"}  # Head ne fait pas de compute
    template:
      spec:
        containers:
        - name: ray-head
          image: registry.ocp.local/pipeline/ray-worker:latest
          resources:
            requests: {memory: "4Gi", cpu: "2"}
  workerGroupSpecs:
  - replicas: 4
    minReplicas: 1
    maxReplicas: 8
    template:
      spec:
        containers:
        - name: ray-worker
          image: registry.ocp.local/pipeline/ray-worker:latest
          resources:
            requests: {memory: "8Gi", cpu: "4"}
```

---

## 6. Volumes attendus

| Source | Volume par run | Fréquence | Volume annuel |
|---|---|---|---|
| Airly (100 points) | ~500 KB | Horaire | ~4 GB |
| EEA (France, 1 mois) | ~200 MB | Mensuel | ~2.4 GB |
| CAMS (1 variable, 1 mois) | ~50 MB/point | Mensuel | Variable |
| EDGAR (tous pays, initial) | ~12 MB | Annuel | ~12 MB |
| BEGES ADEME | ~5 MB | Annuel | ~5 MB |
| Sandre stations | ~200 MB | Annuel (référentiel) | ~200 MB |
| SOL Dataverse | ~50 KB | Statique | ~50 KB |
| **Total Iceberg (5 ans)** | — | — | **~50 GB estimé** |

À 50 GB, DuckDB in-process sur un pod de 16 GB RAM avec streaming Polars reste largement suffisant. Ray/Dask n'est nécessaire qu'au-delà de ~200 GB.

---

## 7. Tuning par composant

### Pod resources recommandées (Prefect Worker)

```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2"
  limits:
    memory: "16Gi"
    cpu: "8"
```

### DuckDB : parallélisme adaptatif

```python
import os
import duckdb

def get_duckdb_con(memory_limit_gb: int = 8) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    con.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
    con.execute("SET temp_directory = '/tmp/duckdb_spill'")
    return con
```

### Iceberg : taille des fichiers Parquet cibles

- **Cible : 128 MB–256 MB par fichier Parquet** (optimal pour les scans analytiques)
- Ingestions fréquentes créent des petits fichiers → compaction hebdomadaire obligatoire
- Activer le bloom filter sur les colonnes à haute cardinalité (`site_code`, `measurand`)

### TimescaleDB : retention policy

```sql
-- Conserver les données brutes 90 jours, les agrégats 5 ans
SELECT add_retention_policy('sensor_readings', INTERVAL '90 days');
-- Les continuous aggregates (hourly/daily) n'ont pas de retention automatique
-- → gérer manuellement ou via Prefect flow de maintenance
```

---

## 8. Benchmarks de référence

Ces benchmarks ont été obtenus sur un pod (8 CPU, 16 GB RAM) avec données sur MinIO local.

| Opération | Dataset | Durée mesurée |
|---|---|---|
| `fetch_edgar_emissions("FRA", all gases)` | 4 CSV locaux, ~50k lignes | < 2 s |
| DuckDB `GROUP BY country, year` sur EDGAR | 225 pays × 43 ans × 29 cat. | < 1 s |
| Polars lazy scan + filter + group_by | 10M lignes Parquet sur MinIO | ~15 s |
| DuckDB Parquet scan sur MinIO | 10M lignes | ~20 s |
| PyIceberg write (append) | 1M lignes → MinIO | ~25 s |
| TimescaleDB INSERT batch | 100k lignes | ~5 s |
| TimescaleDB SELECT 30j hourly | Hypertable compressée | ~200 ms |
| CAMS download (1 point, 1 jour) | NetCDF bbox 1°×1° | 60–120 s |

---

## 9. Monitoring des performances

### Métriques Prefect à tracker par flow

```python
from prefect import flow, task, get_run_logger
import time

@task
def ingest_eea_with_metrics(country: str, start: str, end: str) -> list[dict]:
    logger = get_run_logger()
    t0 = time.perf_counter()

    records = fetch_eea_pollution(...)

    duration = time.perf_counter() - t0
    logger.info("EEA ingest: %d records in %.1fs (%.0f rec/s)",
                len(records), duration, len(records)/duration)

    # Log vers MLflow pour tracking historique
    import mlflow
    mlflow.log_metrics({
        "ingest_eea_records": len(records),
        "ingest_eea_duration_s": duration,
        "ingest_eea_records_per_s": len(records) / duration,
    })
    return records
```

### Alertes performance (à configurer dans Prefect)

| Condition | Seuil | Action |
|---|---|---|
| Flow duration > 2× moyenne | Alerte Slack | Investiguer |
| Records ingérés = 0 | Critique | Bloquer le run suivant |
| MinIO write failed | Critique | Retry × 3 |
| CAMS download > 10 min | Warning | Timeout + log |
| TimescaleDB latency > 2s | Warning | Vérifier indexes |
