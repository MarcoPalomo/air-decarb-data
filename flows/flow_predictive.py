"""Flow Prefect — entraînement et inférence modèles prédictifs.

Cible : exécution mensuelle ou à la demande.
Sources : Iceberg air_quality + edgar_emissions (features)
Destination : MLflow model registry + Iceberg predictions/
"""
from __future__ import annotations

from prefect import flow, task, get_run_logger


@task
def load_features(date_start: str, date_end: str) -> object:
    # TODO: PyIceberg → DuckDB → Polars LazyFrame
    logger = get_run_logger()
    logger.info("Load features %s → %s", date_start, date_end)
    raise NotImplementedError("famille predictive — à implémenter")


@task
def train_model(features: object) -> str:
    # TODO: sklearn/xgboost + MLflow autolog
    raise NotImplementedError


@task
def register_model(run_id: str, model_name: str) -> str:
    # TODO: mlflow.register_model
    raise NotImplementedError


@flow(name="predictive-train", log_prints=True)
def flow_predictive(
    date_start: str = "2020-01-01",
    date_end: str = "2023-12-31",
    model_name: str = "air-quality-forecast",
) -> dict:
    features = load_features(date_start, date_end)
    run_id = train_model(features)
    version = register_model(run_id, model_name)
    return {"model": model_name, "version": version}


if __name__ == "__main__":
    flow_predictive()
