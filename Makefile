UV ?= uv
AIRFLOW_VERSION ?= 3.0.2
AIRFLOW_HOME ?= $(CURDIR)/.airflow_home
VENV ?= .venv
AIRFLOW_VENV ?= .venv-airflow

.PHONY: help venv sync airflow-venv airflow-install setup setup-shared airflow-prepare run-airflow run-prefect run-dagster run-airflow-advanced run-prefect-batch run-prefect-events run-dagster-materialize infra-up infra-down superset-init superset-open superset-health superset-bootstrap-assets seed-data publish-events clean-airflow validate verify-all

help:
	@echo "Available targets:"
	@echo "  make setup               - setup shared deps + separate Airflow env"
	@echo "  make infra-up            - start Postgres/Redis/Kafka/MinIO"
	@echo "  make infra-down          - stop infrastructure stack"
	@echo "  make superset-init       - wait until Superset is healthy"
	@echo "  make superset-bootstrap-assets - register Postgres source and demo datasets in Superset"
	@echo "  make superset-open       - print Superset URL and creds"
	@echo "  make superset-health     - check Superset health endpoint"
	@echo "  make seed-data           - create shared batch input datasets"
	@echo "  make publish-events      - publish demo events to Kafka topic"
	@echo "  make run-airflow         - start Airflow standalone (all DAGs)"
	@echo "  make run-prefect-batch   - run Prefect advanced batch flow"
	@echo "  make run-prefect-events  - run Prefect advanced event flow"
	@echo "  make run-dagster         - start Dagster dev UI"
	@echo "  make run-dagster-materialize - materialize all Dagster assets from CLI"
	@echo "  make verify-all          - syntax and basic file-level validations"

venv:
	$(UV) venv "$(VENV)"

sync: venv
	$(UV) sync

airflow-venv:
	$(UV) venv "$(AIRFLOW_VENV)"

airflow-install: airflow-venv
	@PYV=$$($(AIRFLOW_VENV)/bin/python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"); \
	CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$$PYV.txt"; \
	echo "Installing apache-airflow $(AIRFLOW_VERSION) with constraints for Python $$PYV"; \
	$(UV) pip install --python "$(AIRFLOW_VENV)/bin/python" "apache-airflow==$(AIRFLOW_VERSION)" --constraint "$$CONSTRAINT_URL" && \
	$(UV) pip install --python "$(AIRFLOW_VENV)/bin/python" psycopg2-binary kafka-python

setup-shared: sync

setup: setup-shared airflow-install

airflow-prepare:
	mkdir -p "$(AIRFLOW_HOME)/dags"
	rm -rf "$(AIRFLOW_HOME)/data"
	cp airflow_demo/dags/minimal_etl_airflow.py "$(AIRFLOW_HOME)/dags/"
	cp airflow_demo/pipelines/*.py "$(AIRFLOW_HOME)/dags/"
	cp -R airflow_demo/data "$(AIRFLOW_HOME)/"

run-airflow: setup airflow-prepare
	PATH="$(CURDIR)/$(AIRFLOW_VENV)/bin:$$PATH" AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__LOAD_EXAMPLES=False "$(AIRFLOW_VENV)/bin/airflow" standalone

run-airflow-advanced: run-airflow

run-prefect: sync
	$(UV) run python prefect_demo/flow.py

run-prefect-batch: sync
	$(UV) run python prefect_demo/pipelines/advanced_batch_flow.py

run-prefect-events: sync
	$(UV) run python prefect_demo/pipelines/event_driven_flow.py

run-dagster: sync
	cd dagster_demo && ../$(VENV)/bin/dagster dev -f definitions.py

run-dagster-materialize: sync
	cd dagster_demo && ../$(VENV)/bin/python -c "from definitions import defs; defs.get_implicit_global_asset_job_def().execute_in_process()"

infra-up:
	docker compose --env-file .env.example up -d

infra-down:
	docker compose --env-file .env.example down -v

superset-init:
	./scripts/superset_bootstrap.sh

superset-open:
	@echo "Superset URL: http://localhost:8088"
	@echo "Username: $${SUPERSET_ADMIN_USER:-admin}"
	@echo "Password: $${SUPERSET_ADMIN_PASSWORD:-admin}"

superset-health:
	curl -fsS http://localhost:8088/health

superset-bootstrap-assets: sync
	$(UV) run python scripts/superset_register_assets.py

seed-data: sync
	$(UV) run python scripts/seed_data.py

publish-events: sync
	$(UV) run python scripts/publish_events.py

validate: sync
	$(UV) run python -m py_compile airflow_demo/dags/minimal_etl_airflow.py airflow_demo/pipelines/advanced_batch_dag.py airflow_demo/pipelines/event_driven_incremental_dag.py prefect_demo/flow.py prefect_demo/pipelines/advanced_batch_flow.py prefect_demo/pipelines/event_driven_flow.py dagster_demo/definitions.py dagster_demo/pipelines/advanced_assets.py

verify-all: validate
	@test -f airflow_demo/data/advanced_batch_input.csv || (echo "Missing seed file: airflow_demo/data/advanced_batch_input.csv" && exit 1)
	@test -f prefect_demo/data/advanced_batch_input.csv || (echo "Missing seed file: prefect_demo/data/advanced_batch_input.csv" && exit 1)
	@test -f dagster_demo/data/advanced_batch_input.csv || (echo "Missing seed file: dagster_demo/data/advanced_batch_input.csv" && exit 1)
	@echo "Superset URL expected: http://localhost:8088"
	@echo "All verification checks passed"

clean-airflow:
	rm -rf "$(AIRFLOW_HOME)" "$(AIRFLOW_VENV)"
