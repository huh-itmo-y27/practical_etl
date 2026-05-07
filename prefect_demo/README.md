# Prefect Advanced Demo

## What is implemented
- `pipelines/advanced_batch_flow.py`: quality-gated batch load with retries/caching/artifacts
- `pipelines/event_driven_flow.py`: Kafka-driven incremental processing with checkpoint + table artifact

Prefect focus in this demo:
- Python-first flow orchestration
- task retries and transform caching
- lightweight artifacts for run reporting
- fast local execution with optional Prefect UI

## Demo processes

### Process 1: `advanced_batch_flow`
Purpose: show batch flow with quality gate and reporting artifact.

Flow steps:
1. `extract` - loads batch CSV input.
2. `transform` - applies business transform (`total = qty * price`) with cache.
3. `quality_gate` - validates transformed/total ratio.
4. `load` - writes curated CSV + loads Postgres table `prefect_batch_orders`.
5. `create_markdown_artifact` - publishes run status summary.

Expected outcome:
- completed batch run with explicit artifact status
- reproducible load into Postgres and output file

### Process 2: `event_driven_flow`
Purpose: show event-driven incremental flow.

Flow steps:
1. `consume` - reads Kafka events from `orders_events`.
2. `deduplicate` - removes duplicates by `event_id`.
3. `upsert` - writes/updates Postgres table `prefect_events`.
4. `write_outputs` - stores checkpoint JSON + summary CSV.
5. `create_table_artifact` - publishes received/unique/upserted counts.

Expected outcome:
- transparent event summary in artifacts
- checkpointed incremental run behavior

## Setup
```bash
make setup
make infra-up
make seed-data
make publish-events
```

## Run flows
```bash
make run-prefect-batch
make run-prefect-events
```

## Persistent UI (optional)
```bash
uv run prefect server start
```
