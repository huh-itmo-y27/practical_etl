from __future__ import annotations

import csv
from pathlib import Path


def main() -> None:
    rows = [
        {"order_id": 11, "user_id": 201, "qty": 2, "price": 120.0, "category": "electronics", "event_date": "2026-05-01"},
        {"order_id": 12, "user_id": 202, "qty": 1, "price": 75.5, "category": "books", "event_date": "2026-05-01"},
        {"order_id": 13, "user_id": 201, "qty": 4, "price": 9.9, "category": "groceries", "event_date": "2026-05-02"},
        {"order_id": 14, "user_id": 203, "qty": 2, "price": 49.0, "category": "sports", "event_date": "2026-05-02"},
    ]
    base = Path(__file__).resolve().parents[1]
    targets = [
        base / "airflow_demo" / "data" / "advanced_batch_input.csv",
        base / "prefect_demo" / "data" / "advanced_batch_input.csv",
        base / "dagster_demo" / "data" / "advanced_batch_input.csv",
    ]
    for path in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Seeded {path}")


if __name__ == "__main__":
    main()
