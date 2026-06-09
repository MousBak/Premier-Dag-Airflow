# Architecture — Weather Data Platform

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Airflow 2.9.1                        │
│                  (Orchestrator & Scheduler)                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
              ┌─────────────▼──────────────┐
              │      Open-Meteo API        │
              │  (free, no auth required)  │
              └─────────────┬──────────────┘
                            │  JSON (4 cities × 7 days)
          ┌─────────────────▼──────────────────────┐
          │              BRONZE LAYER               │
          │         MinIO  meteo-bronze/            │
          │    YYYY-MM-DD/<city>.json               │
          │    Raw data — never modified            │
          └─────────────────┬──────────────────────┘
                            │  Transform: select fields, rename
          ┌─────────────────▼──────────────────────┐
          │              SILVER LAYER               │
          │         MinIO  meteo-silver/            │
          │    YYYY-MM-DD/meteo_villes.csv          │
          │    Clean, typed, structured             │
          └─────────────────┬──────────────────────┘
                            │  Load: upsert (ville, date)
          ┌─────────────────▼──────────────────────┐
          │               GOLD LAYER               │
          │      PostgreSQL  meteo_journaliere      │
          │    28 rows (4 cities × 7 days)         │
          │    Indexed, ready for analytics        │
          └─────────────────┬──────────────────────┘
                            │
          ┌─────────────────▼──────────────────────┐
          │            TRACEABILITY                 │
          │      PostgreSQL  suivi_ingestion        │
          │    1 row per DAG run                   │
          │    Bronze paths + Silver path + count  │
          └────────────────────────────────────────┘
```

## DAG Flow — tp3_data_lake

```
extraire_api
    └──→ stocker_bronze          (write raw JSON to MinIO)
              └──→ transformer_silver   (write CSV to MinIO)
                        └──→ charger_gold      (upsert PostgreSQL)
                                    └──→ ecrire_suivi   (audit log)
```

## Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Orchestration | Apache Airflow 2.9.1 | DAG scheduling, monitoring, retry logic |
| Data source | Open-Meteo API | Free weather API, no authentication required |
| Bronze / Silver storage | MinIO (S3-compatible) | Object storage for raw and transformed data |
| Gold storage | PostgreSQL 14 | Relational database for analytical queries |
| Python | 3.12 | DAG code, transformations, API calls |
| Containers | Docker | MinIO isolation |

## Data Model

### `meteo_journaliere` (Gold layer)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `ville` | VARCHAR(100) | City name |
| `date` | DATE | Observation date |
| `temp_max_c` | NUMERIC(5,1) | Max temperature (°C) |
| `temp_min_c` | NUMERIC(5,1) | Min temperature (°C) |
| `temp_moyenne_c` | NUMERIC(5,1) | Mean temperature (°C) |
| `precipitation_mm` | NUMERIC(6,1) | Total precipitation (mm) |
| `vent_max_kmh` | NUMERIC(6,1) | Max wind speed (km/h) |
| `code_meteo` | INTEGER | WMO weather code |
| `insere_le` | TIMESTAMP | Last upsert timestamp |

Unique constraint: `(ville, date)` — enables idempotent upserts.

### `suivi_ingestion` (Audit)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `dag_id` | VARCHAR | Airflow DAG identifier |
| `run_id` | VARCHAR | Airflow Run identifier |
| `ville` | VARCHAR | Cities processed |
| `nb_lignes` | INTEGER | Rows loaded |
| `statut` | VARCHAR | `success` or `failed` |
| `message` | TEXT | Bronze/Silver paths + row counts |
| `debut_ingestion` | TIMESTAMP | Pipeline start time |
| `fin_ingestion` | TIMESTAMP | Pipeline end time |
