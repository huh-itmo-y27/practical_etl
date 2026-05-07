# Practical ETL Advanced Showcase

This folder contains a production-like local demo for:

- `Airflow` (DAG-centric orchestration + BI handoff)
- `Prefect` (Python-first flow orchestration)
- `Dagster` (asset-centric orchestration and quality checks)
- `Superset` (BI layer over Airflow-produced marts)

---

## 1) What each framework demonstrates

- `Airflow`
  - Batch DAG with retries, validation, transform, Postgres load, quality-branch decision.
  - Event DAG with Kafka consume, deduplication, upsert, checkpoint.
  - Publishes BI-ready tables for Superset:
    - `airflow_batch_daily_metrics`
    - `airflow_events_hourly_metrics`
- `Prefect`
  - Advanced batch flow with retries/caching/quality gate/artifacts.
  - Event flow with Kafka consume, dedup, upsert, summary artifact.
  - Strong “pure Python” orchestration developer experience.
- `Dagster`
  - Asset and job model with schedule/sensor support.
  - Asset checks and partitioned advanced assets.
  - Strong data product / lineage style organization.
- `Superset`
  - Visualization layer for Airflow marts.
  - In this demo, Superset metadata runs on local SQLite for stable startup.
  - Analytics source remains Postgres.

---

## 2) Infrastructure components

`docker compose` services:

- Postgres
- Redis
- Zookeeper
- Kafka
- MinIO
- Superset

Main config files:

- `docker-compose.yml`
- `.env.example`
- `scripts/superset_config.py`

---

## 3) One-time setup

Run from repository root:

```bash
cp .env.example .env
make setup
make airflow-install
```

This creates local environments and installs Python dependencies.
Also adjust values in `.env` if needed before starting infrastructure.

---

## 4) Start shared infrastructure

```bash
make infra-up
make superset-init
make superset-health
make superset-open
make seed-data
make publish-events
```

If Superset datasource/datasets are not registered yet:

```bash
make superset-bootstrap-assets
```

---

## 5) Commands to run each demo

### Airflow demo (recommended first)

Terminal 1:

```bash
make run-airflow
```

Then in Airflow UI (`http://localhost:8080`) trigger:

- `advanced_batch_elt_airflow`
- `event_driven_incremental_airflow`

After Airflow runs, optionally re-run Superset registration (if tables were missing on first bootstrap):

```bash
make superset-bootstrap-assets
```

Verify output markers:

- `airflow_demo/data/superset_batch_publish_marker.json`
- `airflow_demo/data/superset_events_publish_marker.json`

### Prefect demo

Run batch flow:

```bash
make run-prefect-batch
```

Run event flow:

```bash
make run-prefect-events
```

Optional persistent Prefect UI:

```bash
uv run prefect server start
```

### Dagster demo

Run Dagster UI:

```bash
make run-dagster
```

Open Dagster UI (typically `http://127.0.0.1:3000`) and launch:

- `advanced_batch_job`
- `advanced_events_job`

Or materialize from CLI:

```bash
make run-dagster-materialize
```

### Superset demo

1. Open Superset: `http://localhost:8088`
2. Login with credentials from `.env.example` (default `admin/admin`)
3. Ensure datasets are present (run once if needed):
  - `make superset-bootstrap-assets`
4. Query datasets:
  - `airflow_batch_daily_metrics`
  - `airflow_events_hourly_metrics`

---

## 6) Validation commands

```bash
make validate
make verify-all
```

What is validated:

- Python syntax for all orchestration scripts.
- Presence of seeded advanced input files.
- Superset URL expectation output.

---

## 7) Stop and cleanup

```bash
make infra-down
```

This stops and removes the compose stack volumes for a clean restart.