"""Soil heavy metals loader — dataverse_files.zip (BDAT / RMQs).

Parses the two ODS files extracted from the dataverse archive:
  - Statzn.ods : Zinc (Zn) soil concentrations, France 1990–2009+
  - Statcu.ods : Copper (Cu) soil concentrations + % > 60 ppm threshold

Data structure per file:
  periode | moyenne | Médiane | Q1 | Q25 | Q75 | Q90 | Q99 | n [| % > seuil]

These are 5-year aggregate statistics, not time series — use as baseline
reference for the Pollution family (métaux lourds) or BEGES context.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

DEFAULT_ZIP = Path(__file__).parents[2] / "dataverse_files.zip"

ODS_FILES = {
    "Zn": "Statzn.ods",
    "Cu": "Statcu.ods",
}

# Regulatory threshold for copper in agricultural soils (French standard)
CU_THRESHOLD_PPM = 60.0


@dataclass
class SoilMetalRecord:
    metal: str              # "Zn" or "Cu"
    periode: str            # "1990-1994"
    year_start: int
    year_end: int
    mean: float
    median: float
    q1: float               # 1st percentile
    q25: float
    q75: float
    q90: float
    q99: float
    n: int                  # sample count
    pct_above_threshold: float | None = None  # Cu only: % > 60 ppm
    threshold_ppm: float | None = None


def load(zip_path: Path | str = DEFAULT_ZIP) -> list[SoilMetalRecord]:
    """Load all soil metal records from the dataverse ODS archive.

    Args:
        zip_path: Path to dataverse_files.zip. Defaults to sibling of pipeline/.

    Returns:
        List of SoilMetalRecord, ordered by metal then period.
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"Dataverse archive not found: {zip_path}")

    records: list[SoilMetalRecord] = []
    with zipfile.ZipFile(zip_path) as outer:
        for metal, ods_name in ODS_FILES.items():
            ods_bytes = outer.read(ods_name)
            records.extend(_parse_ods(metal, ods_bytes))

    return records


def load_as_dataframe(zip_path: Path | str = DEFAULT_ZIP):
    """Return a pandas DataFrame of all soil metal records.

    Requires pandas (already in pipeline/pipeline deps).
    """
    import pandas as pd
    records = load(zip_path)
    return pd.DataFrame([r.__dict__ for r in records])


def as_environmental_records(zip_path: Path | str = DEFAULT_ZIP) -> Iterator[dict]:
    """Yield records in the aeolus DataRecord-compatible schema.

    date_time = midpoint of the 5-year period (Jan 1 of middle year).
    site_code = "SOL_FRANCE" (national aggregate, no spatial granularity).
    """
    from datetime import datetime, timezone
    records = load(zip_path)
    for r in records:
        mid_year = (r.year_start + r.year_end) // 2
        dt = datetime(mid_year, 1, 1, tzinfo=timezone.utc)
        yield {
            "site_code": "SOL_FRANCE",
            "date_time": dt,
            "measurand": r.metal,
            "value": r.mean,
            "units": "mg/kg",
            "source": "SOL_DATAVERSE",
            "family": "pollution",
            "granularity": "periodic",
            "ratification": "Verified",
            "extra": {
                "periode": r.periode,
                "median": r.median,
                "q25": r.q25,
                "q75": r.q75,
                "q90": r.q90,
                "q99": r.q99,
                "n": r.n,
                "pct_above_threshold": r.pct_above_threshold,
                "threshold_ppm": r.threshold_ppm,
            },
        }


# ── ODS parser (no external dependencies) ────────────────────────────────────

def _parse_ods(metal: str, ods_bytes: bytes) -> list[SoilMetalRecord]:
    with zipfile.ZipFile(__import__("io").BytesIO(ods_bytes)) as ods:
        xml = ods.read("content.xml").decode("utf-8")

    texts = [t.strip() for t in re.findall(r"<text:p[^>]*>([^<]*)</text:p>", xml) if t.strip()]
    texts = [t.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&") for t in texts]

    if not texts or texts[0] != "periode":
        raise ValueError(f"Unexpected ODS structure in {metal}: first cell = {texts[:3]}")

    # Detect columns from header row
    header_end = next(
        i for i, t in enumerate(texts[1:], 1)
        if re.match(r"\d{4}-\d{4}", t)
    )
    headers = texts[:header_end]
    has_pct = any("Pourcentage" in h or "%" in h for h in headers)

    # Parse data rows: each row is header_end cells wide
    row_size = header_end
    data_texts = texts[header_end:]
    records: list[SoilMetalRecord] = []

    i = 0
    while i + row_size <= len(data_texts):
        row = data_texts[i : i + row_size]
        i += row_size

        periode = row[0]
        m = re.match(r"(\d{4})-(\d{4})", periode)
        if not m:
            continue

        def f(v: str) -> float:
            return float(v.replace(",", "."))

        pct = f(row[9]) if has_pct and len(row) > 9 else None

        records.append(SoilMetalRecord(
            metal=metal,
            periode=periode,
            year_start=int(m.group(1)),
            year_end=int(m.group(2)),
            mean=f(row[1]),
            median=f(row[2]),
            q1=f(row[3]),
            q25=f(row[4]),
            q75=f(row[5]),
            q90=f(row[6]),
            q99=f(row[7]),
            n=int(float(row[8].replace(",", "."))),
            pct_above_threshold=pct,
            threshold_ppm=CU_THRESHOLD_PPM if (has_pct and pct is not None) else None,
        ))

    return records
