"""Flow Prefect — ingestion décarb (EDGAR + BEGES + CAMS).

Cible : exécution hebdomadaire (cron lundi 03:00 UTC).
Sources : EDGAR v4.3.2 (local), BEGES ADEME (API), CAMS (cdsapi optionnel)
Destination : MinIO raw/ → Iceberg decarb/ → TimescaleDB
"""
from __future__ import annotations

from prefect import flow, task, get_run_logger


@task
def ingest_edgar(
    country_code: str = "FRA",
    year_start: int = 1990,
    year_end: int = 2012,
    as_co2eq: bool = True,
) -> list[dict]:
    from pipeline.families.decarb import fetch_edgar_emissions
    logger = get_run_logger()
    records = fetch_edgar_emissions(
        country_code=country_code,
        year_start=year_start,
        year_end=year_end,
        as_co2eq=as_co2eq,
    )
    logger.info("EDGAR : %d records (%s %d–%d)", len(records), country_code, year_start, year_end)
    return records


@task(retries=2, retry_delay_seconds=120)
def ingest_beges(siren: str | None = None, year: int | None = None) -> list[dict]:
    from pipeline.families.decarb import fetch_beges_registry
    logger = get_run_logger()
    records = fetch_beges_registry(siren=siren, year=year)
    logger.info("BEGES : %d records", len(records))
    return records


@task
def write_to_iceberg(records: list[dict], table: str) -> int:
    # TODO: PyIceberg + Nessie catalog
    logger = get_run_logger()
    logger.info("Write %d records → iceberg/%s", len(records), table)
    return len(records)


@flow(name="decarb-ingest", log_prints=True)
def flow_decarb(
    country_code: str = "FRA",
    year_start: int = 1990,
    year_end: int = 2012,
    siren: str | None = None,
    beges_year: int | None = None,
) -> dict:
    edgar = ingest_edgar(country_code, year_start, year_end)
    beges = ingest_beges(siren, beges_year)

    n_edgar = write_to_iceberg(edgar, "edgar_emissions")
    n_beges = write_to_iceberg(beges, "beges_corporate")

    return {"edgar": n_edgar, "beges": n_beges}


if __name__ == "__main__":
    flow_decarb()
