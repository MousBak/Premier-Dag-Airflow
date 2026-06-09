# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.0.0] — 2026-06-09

### Added
- **TP3** — Data Lake Medallion architecture: Bronze (raw JSON) → Silver (CSV) → Gold (PostgreSQL)
- MinIO integration via `boto3` for object storage (Bronze and Silver layers)
- `dags/common/config.py` — shared config, API helpers, client factories
- Professional project structure: `dags/`, `tests/`, `docker/`, `sql/migrations/`, `config/`, `scripts/`
- `Makefile` — `make setup`, `make run`, `make test`, `make minio`, `make lint`
- `pyproject.toml` — declarative dependency management
- `.env.example` — credentials template
- `config/variables.json` — Airflow Variables template
- `sql/migrations/002_add_indexes.sql` — performance indexes on `meteo_journaliere`
- `scripts/setup.sh` — one-command project setup
- `scripts/create_buckets.py` — MinIO bucket provisioning

### Changed
- DAGs moved to `dags/ingestion/` subfolder
- `docker-compose.yml` moved to `docker/`
- `sql/init_meteo_db.sql` renamed to `sql/migrations/001_init_tables.sql`

---

## [0.3.0] — 2026-06-09

### Added
- **TP2B** — Full pipeline with PostgreSQL loading and ingestion tracking
- `suivi_ingestion` table for run traceability
- All configuration via Airflow Variables (no hardcoded values)
- `ON CONFLICT DO UPDATE` upsert strategy

---

## [0.2.0] — 2026-06-09

### Added
- **TP2A** — Multi-city ingestion (Paris, Lyon, Marseille, Bordeaux)
- Strict separation of extraction and transformation logic
- Justified field selection (8 kept, 5 removed with rationale)

---

## [0.1.0] — 2026-06-08

### Added
- **TP2** — First Airflow DAG with 3 tasks and explicit dependencies
- Open-Meteo API integration (Paris, hourly → daily aggregation)
- XCom for inter-task data exchange
- `urllib.request` + `certifi` for macOS SSL compatibility
