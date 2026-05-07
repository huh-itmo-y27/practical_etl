from __future__ import annotations

import json
import os
import time
from typing import Any

import requests


SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

DATABASE_NAME = os.getenv("SUPERSET_DEMO_DATABASE_NAME", "orchestration_demo")
DATABASE_URI = os.getenv(
    "SUPERSET_DEMO_DATABASE_URI",
    "postgresql://demo:demo@postgres:5432/orchestration_demo",
)
TABLES = [
    "airflow_batch_daily_metrics",
    "airflow_events_hourly_metrics",
]


def wait_health(timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Superset health endpoint is not ready in time")


def login(session: requests.Session) -> str:
    payload = {
        "username": ADMIN_USER,
        "password": ADMIN_PASSWORD,
        "provider": "db",
        "refresh": True,
    }
    r = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def api_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_csrf_token(session: requests.Session, token: str) -> str:
    r = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["result"]


def write_headers(token: str, csrf_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-CSRFToken": csrf_token,
        "Referer": SUPERSET_URL,
    }


def ensure_database(session: requests.Session, token: str, csrf_token: str) -> int:
    payload = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": DATABASE_URI,
        "expose_in_sqllab": True,
        "allow_ctas": True,
        "allow_cvas": True,
    }
    r = session.post(
        f"{SUPERSET_URL}/api/v1/database/",
        headers=write_headers(token, csrf_token),
        json=payload,
        timeout=20,
    )
    if r.status_code not in (200, 201, 400, 409, 422):
        r.raise_for_status()
    if r.status_code == 400:
        # Superset may return 400 for validation/duplication edge cases.
        # We still attempt a lookup by name and proceed if DB exists.
        try:
            body = r.json()
        except ValueError:
            body = {"raw": r.text}
        print(f"Superset database create returned 400: {body}")

    query = json.dumps(
        {"filters": [{"col": "database_name", "opr": "eq", "value": DATABASE_NAME}]}
    )
    rr = session.get(
        f"{SUPERSET_URL}/api/v1/database/",
        headers=api_headers(token),
        params={"q": query},
        timeout=20,
    )
    rr.raise_for_status()
    rows: list[dict[str, Any]] = rr.json().get("result", [])
    if not rows:
        print(
            "Superset database lookup returned no rows.",
            f"create_status={r.status_code}",
            f"create_body={r.text}",
            sep="\n",
        )
        raise RuntimeError(
            f"Database '{DATABASE_NAME}' not found after create attempt. "
            f"Check SUPERSET_DEMO_DATABASE_URI (current: {DATABASE_URI})"
        )
    return int(rows[0]["id"])


def ensure_datasets(session: requests.Session, token: str, csrf_token: str, database_id: int) -> None:
    for table in TABLES:
        payload = {
            "database": database_id,
            "schema": "public",
            "table_name": table,
        }
        r = session.post(
            f"{SUPERSET_URL}/api/v1/dataset/",
            headers=write_headers(token, csrf_token),
            json=payload,
            timeout=20,
        )
        if r.status_code not in (200, 201, 409, 422):
            r.raise_for_status()
        print(f"Ensured dataset: {table}")


def main() -> None:
    wait_health()
    session = requests.Session()
    token = login(session)
    csrf_token = get_csrf_token(session, token)
    db_id = ensure_database(session, token, csrf_token)
    ensure_datasets(session, token, csrf_token, db_id)
    print("Superset assets bootstrap completed")


if __name__ == "__main__":
    main()
