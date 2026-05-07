from __future__ import annotations

import csv
import json
from pathlib import Path

import psycopg2
from kafka import KafkaConsumer
from prefect import flow, task
from prefect.artifacts import create_table_artifact

BASE_DIR = Path(__file__).resolve().parents[1]
CHECKPOINT = BASE_DIR / "data" / "prefect_event_checkpoint.json"
OUTPUT = BASE_DIR / "data" / "prefect_event_summary.csv"


@task(retries=1, retry_delay_seconds=3)
def consume() -> list[dict]:
    consumer = KafkaConsumer(
        "orders_events",
        bootstrap_servers="localhost:19092",
        auto_offset_reset="latest",
        consumer_timeout_ms=3000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    events = [m.value for m in consumer]
    consumer.close()
    return events


@task
def deduplicate(events: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for event in events:
        eid = event["event_id"]
        if eid in seen:
            continue
        seen.add(eid)
        out.append(event)
    return out


@task
def upsert(events: list[dict]) -> int:
    conn = psycopg2.connect(host="localhost", port=5433, dbname="orchestration_demo", user="demo", password="demo")
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS prefect_events (event_id text primary key, user_id int, amount numeric, event_ts timestamptz)"
        )
        for e in events:
            cur.execute(
                """
                INSERT INTO prefect_events(event_id,user_id,amount,event_ts)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT(event_id) DO UPDATE
                SET user_id=excluded.user_id, amount=excluded.amount, event_ts=excluded.event_ts
                """,
                (e["event_id"], e["user_id"], e["amount"], e["event_ts"]),
            )
        conn.commit()
        return len(events)
    finally:
        conn.close()


@task
def write_outputs(processed: int) -> None:
    CHECKPOINT.write_text(json.dumps({"processed": processed}, indent=2), encoding="utf-8")
    with OUTPUT.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["processed"])
        writer.writeheader()
        writer.writerow({"processed": processed})


@flow(name="prefect-event-driven-flow")
def event_driven_flow() -> None:
    events = consume()
    unique = deduplicate(events)
    processed = upsert(unique)
    write_outputs(processed)
    create_table_artifact(
        table=[
            {
                "received": len(events),
                "unique": len(unique),
                "upserted": processed,
            }
        ],
        key="prefect-events-summary",
        description="Event ingestion summary for the current run.",
    )


if __name__ == "__main__":
    event_driven_flow()
