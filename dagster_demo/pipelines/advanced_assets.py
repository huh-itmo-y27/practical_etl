from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import psycopg2
from dagster import (
    AssetCheckResult,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
    asset_check,
)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
BATCH_INPUT = DATA_DIR / "advanced_batch_input.csv"
BATCH_OUTPUT = DATA_DIR / "dagster_advanced_batch_output.csv"
EVENT_SUMMARY = DATA_DIR / "dagster_event_summary.json"

partitions_def = DailyPartitionsDefinition(start_date="2026-05-01")


@asset(partitions_def=partitions_def, group_name="advanced_batch")
def batch_raw_rows() -> list[dict]:
    with BATCH_INPUT.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@asset(partitions_def=partitions_def, group_name="advanced_batch")
def batch_transformed_rows(batch_raw_rows: list[dict]) -> list[dict]:
    out = []
    for row in batch_raw_rows:
        qty = int(row["qty"])
        price = float(row["price"])
        if qty <= 0 or price < 0:
            continue
        out.append(
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
    return out


@asset(partitions_def=partitions_def, group_name="advanced_batch")
def batch_loaded_to_csv(batch_transformed_rows: list[dict]) -> MaterializeResult:
    BATCH_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with BATCH_OUTPUT.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(batch_transformed_rows[0].keys()))
        writer.writeheader()
        writer.writerows(batch_transformed_rows)
    return MaterializeResult(metadata={"rows": len(batch_transformed_rows), "path": str(BATCH_OUTPUT)})


@asset_check(asset=batch_loaded_to_csv)
def batch_quality_check() -> AssetCheckResult:
    if not BATCH_OUTPUT.exists():
        return AssetCheckResult(passed=False, metadata={"reason": f"missing output file: {BATCH_OUTPUT}"})

    with BATCH_OUTPUT.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    passed = len(rows) >= 3
    return AssetCheckResult(
        passed=passed,
        metadata={"rows_after_transform": len(rows), "checked_file": str(BATCH_OUTPUT)},
    )


@asset(group_name="advanced_events")
def event_incremental_summary() -> MaterializeResult:
    conn = psycopg2.connect(host="localhost", port=5433, dbname="orchestration_demo", user="demo", password="demo")
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM dagster_events")
        total = cur.fetchone()[0]
    except Exception:
        total = 0
    finally:
        conn.close()
    payload = {"processed_events": total, "generated_at": datetime.utcnow().isoformat()}
    EVENT_SUMMARY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return MaterializeResult(metadata=payload)
