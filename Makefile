.PHONY: setup run stop test lint \
        docker-up docker-down docker-build docker-logs \
        minio buckets variables help

AIRFLOW_HOME := $(shell pwd)
VENV         := airflow_venv/bin/activate
COMPOSE      := docker compose -f docker/docker-compose.yml

help:
	@echo ""
	@echo "Weather Data Platform — Commands"
	@echo "────────────────────────────────────────────────"
	@echo "  Local development (Python venv)"
	@echo "    make setup      Full local setup (venv, Airflow, variables)"
	@echo "    make run        Start Airflow scheduler + webserver (local)"
	@echo "    make stop       Stop local Airflow processes"
	@echo "    make minio      Start MinIO container (Docker)"
	@echo "    make buckets    Create MinIO buckets"
	@echo "    make variables  Load Variables from config/variables.json"
	@echo ""
	@echo "  Docker (full stack)"
	@echo "    make docker-up    Start full stack (Airflow + PG + MinIO + Redis)"
	@echo "    make docker-down  Stop and remove containers"
	@echo "    make docker-build Rebuild Docker image"
	@echo "    make docker-logs  Tail all container logs"
	@echo ""
	@echo "  Quality"
	@echo "    make test       Run all tests (pytest)"
	@echo "    make lint       Check code style (flake8)"
	@echo "────────────────────────────────────────────────"
	@echo "  UIs (Docker mode)"
	@echo "    Airflow  : http://localhost:8080  (admin / admin)"
	@echo "    MinIO    : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "    Flower   : http://localhost:5555"
	@echo ""

# ── Local development ──────────────────────────────────────────────────────

setup:
	bash scripts/setup.sh

run:
	@export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	source $(VENV) && \
	nohup airflow scheduler > airflow-scheduler.out 2> airflow-scheduler.err & \
	echo $$! > airflow-scheduler.pid && \
	nohup airflow webserver --port 8081 > airflow-webserver.out 2> airflow-webserver.err & \
	echo $$! > airflow-webserver.pid && \
	echo "Airflow started — http://localhost:8081  (admin / admin)"

stop:
	@if [ -f airflow-scheduler.pid ]; then kill $$(cat airflow-scheduler.pid) 2>/dev/null; rm -f airflow-scheduler.pid; fi
	@if [ -f airflow-webserver.pid ]; then kill $$(cat airflow-webserver.pid) 2>/dev/null; rm -f airflow-webserver.pid; fi
	@echo "Airflow stopped."

minio:
	docker run -d --name minio \
	  -p 9000:9000 -p 9001:9001 \
	  -e MINIO_ROOT_USER=minioadmin \
	  -e MINIO_ROOT_PASSWORD=minioadmin \
	  -v minio_data:/data \
	  minio/minio server /data --console-address ":9001" 2>/dev/null || \
	docker start minio
	@echo "MinIO — http://localhost:9001  (minioadmin / minioadmin)"

buckets:
	source $(VENV) && python3 scripts/create_buckets.py

variables:
	@source $(VENV) && export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	python3 -c "\
import json, subprocess; \
vars = json.load(open('config/variables.json')); \
[subprocess.run(['airflow','variables','set',k,v], capture_output=True) for k,v in vars.items()]; \
print('Variables loaded:', list(vars.keys()))"

# ── Docker full stack ──────────────────────────────────────────────────────

docker-up:
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Stack started. Waiting for services to be ready..."
	@echo "  Airflow  : http://localhost:8080  (admin / admin)"
	@echo "  MinIO    : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Flower   : http://localhost:5555"

docker-down:
	$(COMPOSE) down

docker-build:
	$(COMPOSE) build --no-cache

docker-logs:
	$(COMPOSE) logs -f

# ── Quality ────────────────────────────────────────────────────────────────

test:
	source $(VENV) && export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	pytest tests/ -v

lint:
	source $(VENV) && flake8 dags/ tests/ --max-line-length=100
