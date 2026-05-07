from __future__ import annotations

import csv
from pathlib import Path

from prefect import flow, task

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "input.csv"
OUTPUT_FILE = DATA_DIR / "output_prefect.csv"


@task
def extract(path: Path = INPUT_FILE) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


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
    return transformed


@task
def load(rows: list[dict], path: Path = OUTPUT_FILE) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
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
    return str(path)


@flow(name="minimal-etl-prefect")
def etl_flow() -> str:
    rows = extract()
    transformed = transform(rows)
    return load(transformed)


if __name__ == "__main__":
    output_path = etl_flow()
    print("Output written:", output_path)
