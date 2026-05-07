from __future__ import annotations

import json
from datetime import datetime, timezone

from kafka import KafkaProducer


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=["localhost:19092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i in range(5):
        payload = {
            "event_id": f"evt-{i}",
            "user_id": 300 + i % 2,
            "amount": 10.5 + i,
            "event_ts": datetime.now(timezone.utc).isoformat(),
        }
        producer.send("orders_events", payload)
        print(f"Published {payload}")
    producer.flush()


if __name__ == "__main__":
    main()
