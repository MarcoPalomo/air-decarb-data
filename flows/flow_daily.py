"""Flow Prefect — ingestion quotidienne qualite + pollution.

Cible : exécution nocturne (cron 02:00 UTC) sur le Kubernetes Work Pool.
Sources : AURN, SAQN, EEA, OpenAQ, LAQN (via aeolus)
Destination : MinIO raw/ → Iceberg → TimescaleDB (continuous aggregates J+1)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=60)
def ingest_qualite(date: str) -> list[dict]:
    from pipeline import ingest
    logger = get_run_logger()
    logger.info("Ingestion qualite : %s", date)
    return ingest(families=["qualite"], start_date=datetime.fromisoformat(date))


@task(retries=3, retry_delay_seconds=60)
def ingest_pollution(date: str) -> list[dict]:
    from pipeline import ingest
    logger = get_run_logger()
    logger.info("Ingestion pollution : %s", date)
    return ingest(families=["pollution"], start_date=datetime.fromisoformat(date))


@task
def write_to_iceberg(records: list[dict], table: str) -> int:
    # TODO: PyIceberg + Nessie catalog
    logger = get_run_logger()
    logger.info("Write %d records → iceberg/%s", len(records), table)
    return len(records)


@flow(name="daily-air-quality", log_prints=True)
def flow_daily(
    date: str | None = None,
) -> dict:
    if date is None:
        date = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    qualite = ingest_qualite(date)
    pollution = ingest_pollution(date)

    n_qualite = write_to_iceberg(qualite, "air_quality")
    n_pollution = write_to_iceberg(pollution, "pollution")

    return {"date": date, "qualite": n_qualite, "pollution": n_pollution}


if __name__ == "__main__":
    flow_daily()
