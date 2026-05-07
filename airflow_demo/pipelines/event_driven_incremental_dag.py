from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensor
from kafka import KafkaConsumer
import psycopg2

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
CHECKPOINT = DATA_DIR / "airflow_event_checkpoint.json"
SUMMARY = DATA_DIR / "airflow_event_summary.json"
BI_MARKER = DATA_DIR / "superset_events_publish_marker.json"


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "orchestration_demo"),
        user=os.getenv("POSTGRES_USER", "demo"),
        password=os.getenv("POSTGRES_PASSWORD", "demo"),
    )


@dag(
    dag_id="event_driven_incremental_airflow",
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["advanced", "events", "incremental"],
)
def event_driven_incremental_airflow():
    wait_for_window = TimeDeltaSensor(task_id="wait_for_window", delta=timedelta(seconds=5))

    @task
    def consume_events() -> list[dict]:
        consumer = KafkaConsumer(
            "orders_events",
            bootstrap_servers="localhost:19092",
            auto_offset_reset="latest",
            consumer_timeout_ms=3000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        events = [msg.value for msg in consumer]
        consumer.close()
        return events

    @task
    def deduplicate(events: list[dict]) -> list[dict]:
        seen = set()
        unique = []
        for event in events:
            key = event["event_id"]
            if key in seen:
                continue
            seen.add(key)
            unique.append(event)
        return unique

    @task
    def upsert(unique_events: list[dict]) -> int:
        conn = pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS airflow_events (event_id text primary key, user_id int, amount numeric, event_ts timestamptz)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS airflow_events_hourly_metrics (event_hour timestamptz, events_count int, total_amount numeric)"
            )
            for event in unique_events:
                cur.execute(
                    """
                    INSERT INTO airflow_events(event_id,user_id,amount,event_ts)
                    VALUES(%s,%s,%s,%s)
                    ON CONFLICT(event_id) DO UPDATE
                    SET user_id=excluded.user_id, amount=excluded.amount, event_ts=excluded.event_ts
                    """,
                    (event["event_id"], event["user_id"], event["amount"], event["event_ts"]),
                )
            cur.execute("TRUNCATE airflow_events_hourly_metrics")
            cur.execute(
                """
                INSERT INTO airflow_events_hourly_metrics(event_hour, events_count, total_amount)
                SELECT date_trunc('hour', event_ts), COUNT(*), SUM(amount)
                FROM airflow_events
                GROUP BY 1
                ORDER BY 1
                """
            )
            conn.commit()
            return len(unique_events)
        finally:
            conn.close()

    @task
    def checkpoint(processed_count: int) -> dict:
        payload = {"processed_count": processed_count, "processed_at": datetime.now(timezone.utc).isoformat()}
        CHECKPOINT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        SUMMARY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    @task
    def publish_bi_marker(checkpoint_payload: dict) -> dict:
        marker = {
            "published_dataset": "airflow_events_hourly_metrics",
            "processed_count": checkpoint_payload["processed_count"],
            "published_at": checkpoint_payload["processed_at"],
        }
        BI_MARKER.write_text(json.dumps(marker, indent=2), encoding="utf-8")
        return marker

    wait_for_window >> publish_bi_marker(checkpoint(upsert(deduplicate(consume_events()))))


event_driven_incremental_airflow()
