.PHONY: setup run stop test lint minio buckets variables help

AIRFLOW_HOME := $(shell pwd)
VENV         := airflow_venv/bin/activate

help:
	@echo ""
	@echo "Weather Data Platform — Available commands"
	@echo "------------------------------------------"
	@echo "  make setup      Full project setup (venv, Airflow, variables)"
	@echo "  make run        Start Airflow scheduler + webserver"
	@echo "  make stop       Stop all Airflow processes"
	@echo "  make minio      Start MinIO container (Docker required)"
	@echo "  make buckets    Create MinIO buckets (meteo-bronze, meteo-silver)"
	@echo "  make variables  Load Airflow Variables from config/variables.json"
	@echo "  make test       Run all tests"
	@echo "  make lint       Check code style (flake8)"
	@echo ""

setup:
	bash scripts/setup.sh

run:
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	source $(VENV) && \
	nohup airflow scheduler > airflow-scheduler.out 2> airflow-scheduler.err & \
	echo $$! > airflow-scheduler.pid && \
	nohup airflow webserver --port 8081 > airflow-webserver.out 2> airflow-webserver.err & \
	echo $$! > airflow-webserver.pid && \
	echo "" && \
	echo "Airflow started." && \
	echo "  UI  : http://localhost:8081  (admin / admin)" && \
	echo "  Logs: airflow-scheduler.out / airflow-webserver.out"

stop:
	@if [ -f airflow-scheduler.pid ]; then kill $$(cat airflow-scheduler.pid) 2>/dev/null; rm airflow-scheduler.pid; fi
	@if [ -f airflow-webserver.pid ]; then kill $$(cat airflow-webserver.pid) 2>/dev/null; rm airflow-webserver.pid; fi
	@echo "Airflow stopped."

minio:
	docker run -d \
	  --name minio \
	  -p 9000:9000 \
	  -p 9001:9001 \
	  -e MINIO_ROOT_USER=minioadmin \
	  -e MINIO_ROOT_PASSWORD=minioadmin \
	  -v minio_data:/data \
	  minio/minio server /data --console-address ":9001" || \
	docker start minio
	@echo "MinIO started. Console: http://localhost:9001  (minioadmin / minioadmin)"

buckets:
	source $(VENV) && python3 scripts/create_buckets.py

variables:
	@source $(VENV) && export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	python3 -c "\
import json, subprocess; \
vars = json.load(open('config/variables.json')); \
[subprocess.run(['airflow','variables','set',k,v]) for k,v in vars.items()]"
	@echo "Variables loaded."

test:
	source $(VENV) && export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	pytest tests/ -v

lint:
	source $(VENV) && flake8 dags/ tests/
