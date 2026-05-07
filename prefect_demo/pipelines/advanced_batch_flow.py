from __future__ import annotations

import csv
from pathlib import Path

import psycopg2
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT = BASE_DIR / "data" / "advanced_batch_input.csv"
OUTPUT = BASE_DIR / "data" / "advanced_prefect_batch_output.csv"


@task(retries=2, retry_delay_seconds=5)
def extract() -> list[dict]:
    with INPUT.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@task(cache_key_fn=lambda *_args, **_kwargs: "prefect_batch_transform_v1", cache_expiration=None)
def transform(rows: list[dict]) -> list[dict]:
    result = []
    for row in rows:
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


@task
def quality_gate(rows: list[dict], total: int) -> bool:
    return total > 0 and (len(rows) / total) >= 0.8


@task
def load(rows: list[dict]) -> int:
    with OUTPUT.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    conn = psycopg2.connect(host="localhost", port=5433, dbname="orchestration_demo", user="demo", password="demo")
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS prefect_batch_orders (order_id int, user_id int, category text, event_date date, qty int, price numeric, total numeric)"
        )
        cur.execute("TRUNCATE prefect_batch_orders")
        cur.executemany(
            "INSERT INTO prefect_batch_orders(order_id,user_id,category,event_date,qty,price,total) VALUES(%(order_id)s,%(user_id)s,%(category)s,%(event_date)s,%(qty)s,%(price)s,%(total)s)",
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


@flow(name="prefect-advanced-batch-flow")
def advanced_batch_flow() -> None:
    source = extract()
    transformed = transform(source)
    passed = quality_gate(transformed, len(source))
    if not passed:
        create_markdown_artifact(key="prefect-batch-status", markdown="Quality gate failed. Load skipped.")
        return
    loaded = load(transformed)
    create_markdown_artifact(
        key="prefect-batch-status",
        markdown=f"Quality gate passed. Loaded **{loaded}** rows into Postgres and CSV artifact.",
    )


if __name__ == "__main__":
    advanced_batch_flow()
