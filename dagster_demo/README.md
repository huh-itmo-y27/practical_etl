# Dagster Advanced Demo

## What is implemented
- Base assets + `etl_job`
- Advanced partitioned batch assets in `pipelines/advanced_assets.py`
- Asset quality check (`batch_quality_check`)
- Additional jobs, schedule, and sensor in `definitions.py`

Dagster focus in this demo:
- asset-centric data orchestration
- partitioned materialization
- data quality as first-class `asset_check`
- schedule/sensor style orchestration around assets

## Demo processes

### Process 1: Advanced batch assets (`advanced_batch_job`)
Purpose: demonstrate partitioned asset pipeline with quality validation.

Asset chain:
1. `batch_raw_rows` - reads seeded batch input.
2. `batch_transformed_rows` - applies transform and filtering.
3. `batch_loaded_to_csv` - writes curated batch output.
4. `batch_quality_check` - validates transformed asset quality contract.

Expected outcome:
- materialization metadata for each asset
- pass/fail quality check in Dagster UI

### Process 2: Event summary asset (`advanced_events_job`)
Purpose: demonstrate event-oriented summary as managed asset.

Asset flow:
1. `event_incremental_summary` - computes summary from event sink state.
2. Schedule/sensor wiring in `definitions.py` triggers the job lifecycle.

Expected outcome:
- updated summary asset with metadata
- clear asset history in Dagster UI

## Setup
```bash
cd tbd/practical_etl
make setup
make infra-up
make seed-data
```

## Run (dev UI)
```bash
make run-dagster
```

Then materialize advanced assets and run jobs from UI.

## Run via Jobs tab
This demo defines named jobs:
- `etl_job`
- `advanced_batch_job`
- `advanced_events_job`

In Dagster UI:
1. Open **Jobs**
2. Select `advanced_batch_job` (or `advanced_events_job`)
3. Click **Launch Run**
