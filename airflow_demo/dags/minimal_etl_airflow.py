from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "input.csv"
OUTPUT_CSV = DATA_DIR / "output_airflow.csv"
OUTPUT_SQLITE = DATA_DIR / "output_airflow.db"


@dag(
    dag_id="minimal_etl_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["demo", "etl", "minimal"],
)
def minimal_etl_airflow():
    @task
    def extract() -> list[dict]:
        rows: list[dict] = []
        with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        if not rows:
            raise ValueError("Input dataset is empty")
        return rows

    @task
    def transform(rows: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in rows:
            qty = int(row["qty"])
            price = float(row["price"])
            if qty <= 0 or price < 0:
                continue
            transformed.append(
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
        if not transformed:
            raise ValueError("No valid rows after transformation")
        return transformed

    @task
    def load(rows: list[dict]) -> dict:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
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
            writer.writerows(rows)

        conn = sqlite3.connect(OUTPUT_SQLITE)
        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS order_metrics")
            cur.execute(
                """
                CREATE TABLE order_metrics (
                    order_id INTEGER,
                    user_id INTEGER,
                    category TEXT,
                    event_date TEXT,
                    qty INTEGER,
                    price REAL,
                    total REAL
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO order_metrics
                (order_id, user_id, category, event_date, qty, price, total)
                VALUES
                (:order_id, :user_id, :category, :event_date, :qty, :price, :total)
                """,
                rows,
            )
            conn.commit()
            count = cur.execute("SELECT COUNT(*) FROM order_metrics").fetchone()[0]
        finally:
            conn.close()

        return {
            "rows_loaded": count,
            "csv_path": str(OUTPUT_CSV),
            "sqlite_path": str(OUTPUT_SQLITE),
        }

    load(transform(extract()))


minimal_etl_airflow()
