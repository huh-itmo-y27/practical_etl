# Airflow Advanced Demo

## Goal
Show two production-like DAGs:
- `advanced_batch_elt_airflow` (batch ELT + quality gate + branching + Postgres load)
- `event_driven_incremental_airflow` (Kafka consume + dedup + upsert + checkpoint)

Airflow focus in this demo:
- DAG-centric orchestration and scheduling
- quality branching and retries
- operational visibility in Grid/Graph/Task logs
- BI handoff to Postgres mart tables for Superset

## Structure
- `dags/minimal_etl_airflow.py` - baseline DAG
- `pipelines/advanced_batch_dag.py` - advanced batch DAG
- `pipelines/event_driven_incremental_dag.py` - advanced incremental DAG
- `data/advanced_batch_input.csv` - shared seeded input

## Demo processes

### Process 1: `advanced_batch_elt_airflow`
Purpose: show scheduled batch ELT with quality control.

Steps inside DAG:
1. `extract` - reads seeded CSV batch input.
2. `validate` - filters invalid records and counts rejected rows.
3. `transform` - computes normalized rows and derived metrics.
4. `load_to_postgres` - writes facts and aggregate mart table:
   - `airflow_batch_orders`
   - `airflow_batch_daily_metrics`
5. `compute_quality` - calculates quality score.
6. `quality_gate_branch` - routes execution to success/failure branch.
7. `publish_bi_marker` - writes publish marker for BI readiness.

Expected outcome:
- branch decision visible in Airflow UI
- mart table ready for Superset queries

### Process 2: `event_driven_incremental_airflow`
Purpose: show incremental/event-style processing.

Steps inside DAG:
1. `wait_for_window` - small sensor gate before polling events.
2. `consume_events` - reads events from Kafka topic `orders_events`.
3. `deduplicate` - removes duplicate event IDs.
4. `upsert` - merges events into Postgres table `airflow_events`.
5. `checkpoint` - stores processed counters and timestamp.
6. `publish_bi_marker` - marks hourly mart freshness for BI.

Expected outcome:
- stable incremental upsert behavior
- checkpoint and publish marker artifacts in `data/`

## Setup and run
```bash
cd tbd/practical_etl
make setup
make infra-up
make seed-data
make publish-events
make run-airflow
```

Then open `http://localhost:8080`, enable and run:
- `advanced_batch_elt_airflow`
- `event_driven_incremental_airflow`

## Validation
```bash
ls .airflow_home/data
```
