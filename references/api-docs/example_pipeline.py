"""Example: Airly IoT ingestion — Famille 1 de la pipeline environnementale.

Démontre comment récupérer toutes les données d'une zone géographique
et les structurer pour alimenter les étapes aval (processing, ML).

Usage:
    AIRLY_API_KEY=... python example_pipeline.py
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict

import airly
from airly import AirlyClient, AirlyNotFoundError, AirlyRateLimitError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

ZONES = {
    "paris_centre":   (48.8566, 2.3522),
    "paris_nord":     (48.8903, 2.3477),
    "boulogne":       (48.8352, 2.2399),
    "saint_denis":    (48.9362, 2.3574),
}

MAX_DISTANCE_KM = 5.0
INDEX_TYPE = "AIRLY_CAQI"


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_zone_snapshot(client: AirlyClient, name: str, lat: float, lng: float) -> dict | None:
    """Fetch current air quality snapshot for a zone.

    Returns a flat dict ready for DataFrame ingestion.
    """
    try:
        m = client.get_nearest_measurements(lat, lng, max_distance_km=MAX_DISTANCE_KM)
    except AirlyNotFoundError:
        log.warning("No sensor found within %.1f km of %s", MAX_DISTANCE_KM, name)
        return None
    except AirlyRateLimitError as e:
        log.error("Rate limit hit (%d/%d remaining). Stop fetching.", e.remaining_day, e.limit_day)
        raise

    if m.current is None:
        return None

    row: dict = {
        "zone": name,
        "lat": lat,
        "lng": lng,
        "timestamp": m.current.from_date_time.isoformat(),
    }

    for v in m.current.values:
        row[v.name.lower()] = v.value

    idx = m.current.get_index(INDEX_TYPE)
    if idx:
        row["caqi_value"] = idx.value
        row["caqi_level"] = idx.level
        row["caqi_color"] = idx.color

    return row


def build_meta_catalogue(client: AirlyClient) -> dict:
    """Pull all meta endpoints once — use this to seed your schema registry."""
    return {
        "indexes": [asdict(i) for i in client.get_meta_indexes()],
        "measurement_types": [asdict(t) for t in client.get_meta_measurements()],
        "standards": [asdict(s) for s in client.get_meta_standards()],
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    with AirlyClient() as client:

        # 1. Meta catalogue (schema ref — call once and cache)
        log.info("Fetching meta catalogue…")
        meta = build_meta_catalogue(client)
        log.info(
            "  %d indexes · %d measurement types · %d standards",
            len(meta["indexes"]),
            len(meta["measurement_types"]),
            len(meta["standards"]),
        )

        # 2. Zone snapshots (IoT real-time family)
        snapshots = []
        for zone_name, (lat, lng) in ZONES.items():
            log.info("Fetching zone: %s (%.4f, %.4f)", zone_name, lat, lng)
            row = fetch_zone_snapshot(client, zone_name, lat, lng)
            if row:
                snapshots.append(row)

        log.info("Rate limit status: %d/%d requests remaining today",
                 client.rate_limit.remaining_day or 0,
                 client.rate_limit.limit_day or 100)

        # 3. Nearest installations discovery
        log.info("Discovering installations near Paris centre…")
        installations = client.get_nearest_installations(
            lat=48.8566, lng=2.3522,
            max_distance_km=10.0,
            max_results=5,
        )
        for inst in installations:
            log.info(
                "  ID=%d  %s %s  (airly=%s, elevation=%.0fm)",
                inst.id,
                inst.address.city or "?",
                inst.address.street or "",
                inst.airly,
                inst.elevation or 0,
            )

        # 4. Point interpolation (any coordinate, even without a nearby sensor)
        log.info("Interpolated reading at Eiffel Tower…")
        point = client.get_point_measurements(lat=48.8584, lng=2.2945)
        if point.current:
            pm25 = point.current.get_value("PM25")
            pm10 = point.current.get_value("PM10")
            idx = point.current.get_index(INDEX_TYPE)
            log.info(
                "  PM2.5=%.1f µg/m³  PM10=%.1f µg/m³  CAQI=%s (%s)",
                pm25 or 0, pm10 or 0,
                idx.level if idx else "N/A",
                idx.value if idx else "N/A",
            )

            # Check WHO standards compliance
            for std in point.current.standards:
                status = "OK" if std.percent <= 100 else "EXCEEDED"
                log.info(
                    "  Standard %s / %s: %.1f µg/m³ = %.0f%% of limit [%s]",
                    std.name, std.pollutant, std.limit, std.percent, status
                )

        # 5. 24h history (time series for ML)
        log.info("24h history has %d data points", len(point.history))
        log.info("24h forecast has %d data points", len(point.forecast))

        # Output snapshots (in a real pipeline → Parquet / BigQuery / Kafka)
        log.info("\n── Zone snapshots ──")
        for row in snapshots:
            log.info(row)


if __name__ == "__main__":
    main()
