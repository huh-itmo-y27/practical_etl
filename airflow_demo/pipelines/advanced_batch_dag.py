from __future__ import annotations

from datetime import datetime, timedelta
import csv
import json
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

INPUT = DATA_DIR / "advanced_batch_input.csv"
CURATED = DATA_DIR / "advanced_batch_curated.csv"
METRICS = DATA_DIR / "advanced_batch_metrics.csv"
BI_MARKER = DATA_DIR / "superset_batch_publish_marker.json"


def read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "orchestration_demo"),
        user=os.getenv("POSTGRES_USER", "demo"),
        password=os.getenv("POSTGRES_PASSWORD", "demo"),
    )


@dag(
    dag_id="advanced_batch_elt_airflow",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["advanced", "batch", "quality-gate"],
)
def advanced_batch_elt_airflow():
    @task
    def extract() -> list[dict]:
        return read_csv(INPUT)

    @task
    def validate(rows: list[dict]) -> dict:
        valid, rejected = [], 0
        for row in rows:
            qty = int(row["qty"])
            price = float(row["price"])
            if qty <= 0 or price < 0:
                rejected += 1
                continue
            valid.append(row)
        return {"valid": valid, "rejected": rejected, "total": len(rows)}

    @task
    def transform(payload: dict) -> list[dict]:
        result = []
        for row in payload["valid"]:
            qty = int(row["qty"])
            price = float(row["price"])
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
        write_csv(CURATED, result)
        return result

    @task
    def load_to_postgres(rows: list[dict]) -> int:
        conn = pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS airflow_batch_orders (order_id int, user_id int, category text, event_date date, qty int, price numeric, total numeric)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS airflow_batch_daily_metrics (event_date date, category text, orders_count int, gross_revenue numeric)"
            )
            cur.execute("TRUNCATE airflow_batch_orders")
            cur.execute("TRUNCATE airflow_batch_daily_metrics")
            cur.executemany(
                "INSERT INTO airflow_batch_orders(order_id,user_id,category,event_date,qty,price,total) VALUES(%(order_id)s,%(user_id)s,%(category)s,%(event_date)s,%(qty)s,%(price)s,%(total)s)",
                rows,
            )
            cur.execute(
                """
                INSERT INTO airflow_batch_daily_metrics(event_date, category, orders_count, gross_revenue)
                SELECT event_date::date, category, COUNT(*), SUM(total)
                FROM airflow_batch_orders
                GROUP BY event_date::date, category
                ORDER BY event_date::date, category
                """
            )
            conn.commit()
            return len(rows)
        finally:
            conn.close()

    @task
    def compute_quality(payload: dict) -> float:
        if payload["total"] == 0:
            return 0.0
        score = len(payload["valid"]) / payload["total"]
        write_csv(METRICS, [{"quality_score": round(score, 3), "total_rows": payload["total"], "rejected_rows": payload["rejected"]}])
        return score

    @task
    def publish_bi_marker(loaded_rows: int, quality_score: float) -> dict:
        marker = {
            "published_dataset": "airflow_batch_daily_metrics",
            "loaded_rows": loaded_rows,
            "quality_score": round(quality_score, 3),
            "published_at": datetime.utcnow().isoformat(),
        }
        BI_MARKER.write_text(json.dumps(marker, indent=2), encoding="utf-8")
        return marker

    def quality_branch(score: float) -> str:
        return "publish_success" if score >= 0.8 else "publish_failure"

    publish_success = EmptyOperator(task_id="publish_success")
    publish_failure = EmptyOperator(task_id="publish_failure")

    rows = extract()
    validated = validate(rows)
    transformed = transform(validated)
    loaded = load_to_postgres(transformed)
    quality = compute_quality(validated)
    published = publish_bi_marker(loaded, quality)

    branch = BranchPythonOperator(
        task_id="quality_gate_branch",
        python_callable=quality_branch,
        op_args=[quality],
    )

    [loaded, quality, published] >> branch >> [publish_success, publish_failure]


advanced_batch_elt_airflow()
