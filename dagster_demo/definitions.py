from __future__ import annotations

import csv
from pathlib import Path

from dagster import (
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    SensorResult,
    asset,
    define_asset_job,
    sensor,
)
from pipelines.advanced_assets import (
    batch_loaded_to_csv,
    batch_quality_check,
    batch_raw_rows,
    batch_transformed_rows,
    event_incremental_summary,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "input.csv"
OUTPUT_FILE = DATA_DIR / "output_dagster.csv"


@asset
def raw_rows() -> list[dict]:
    with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@asset
def transformed_rows(raw_rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for row in raw_rows:
        qty = int(row["qty"])
        price = float(row["price"])
        if qty <= 0 or price < 0:
            continue
        result.append(
            {
                "order_id": int(row["order_id"]),
                "user_id": int(row["user_id"]),
                "category": row["category"],
                "event_date": row["event_date"],
                "qty": qty,
                "price": round(price, 2),
                "total": round(qty * price, 2),
            }
        )
    return result


@asset
def output_file(transformed_rows: list[dict]) -> MaterializeResult:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "order_id",
                "user_id",
                "category",
                "event_date",
                "qty",
                "price",
                "total",
            ],
        )
        writer.writeheader()
        writer.writerows(transformed_rows)
    return MaterializeResult(metadata={"rows": len(transformed_rows), "path": str(OUTPUT_FILE)})


etl_job = define_asset_job(name="etl_job")
advanced_batch_job = define_asset_job(name="advanced_batch_job", selection=["batch_raw_rows", "batch_transformed_rows", "batch_loaded_to_csv"])
advanced_events_job = define_asset_job(name="advanced_events_job", selection=["event_incremental_summary"])

advanced_batch_schedule = ScheduleDefinition(job=advanced_batch_job, cron_schedule="0 * * * *")


@sensor(job=advanced_events_job, minimum_interval_seconds=60)
def advanced_events_sensor():
    yield SensorResult(run_key="advanced-events-fixed-run")

defs = Definitions(
    assets=[
        raw_rows,
        transformed_rows,
        output_file,
        batch_raw_rows,
        batch_transformed_rows,
        batch_loaded_to_csv,
        event_incremental_summary,
    ],
    asset_checks=[batch_quality_check],
    jobs=[etl_job, advanced_batch_job, advanced_events_job],
    schedules=[advanced_batch_schedule],
    sensors=[advanced_events_sensor],
)
