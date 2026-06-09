"""
Weather Data Pipeline — Production DAG
Uses the TaskFlow API and Dynamic Task Mapping to run one task per city in parallel.

Architecture:
  get_cities()                     → returns list of 4 cities from Airflow Variables
  extraire_ville.expand(ville=...) → 4 parallel task instances (one per city)
  stocker_bronze.expand(...)       → 4 parallel task instances (one per city)
  transformer_silver(all_data)     → fan-in: collects all cities, writes 1 CSV to Silver
  charger_gold(rows)               → upsert into PostgreSQL meteo_journaliere
  dbt_run()                        → dbt run + dbt test (analytical views/tables)
  ecrire_suivi(count, paths)       → write audit record

Why Dynamic Task Mapping?
  - Each city is independently retried on failure (not the whole batch)
  - Parallelism is visible in the Airflow UI per city
  - Adding a new city requires no code change — just update METEO_VILLES Variable

Why dbt after load_gold?
  - dbt transforms the raw Gold table into analytical models (staging + marts)
  - dbt tests run automatically after each ingestion — data quality gate
  - If dbt tests fail, the audit task is skipped → alert is raised
"""

import io
import json
import csv
import ssl
import certifi
import urllib.request
import urllib.parse
import psycopg2
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from common.config import (
    get_config,
    get_s3_client,
    get_ssl_context,
    build_api_params,
    json_to_rows,
    COLONNES_TABLE,
)


# ── DAG definition ─────────────────────────────────────────────────────────

@dag(
    dag_id="weather_data_pipeline",
    description="Production weather pipeline — Medallion architecture with Dynamic Task Mapping",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # every day at 06:00
    catchup=False,
    tags=["production", "medallion", "dynamic-mapping", "minio", "postgresql"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
    },
    doc_md="""
## Weather Data Pipeline

Daily ingestion of weather data for French cities via [Open-Meteo](https://open-meteo.com/).

### Architecture
```
API → Bronze (MinIO, raw JSON) → Silver (MinIO, CSV) → Gold (PostgreSQL)
```

### Configuration
All parameters are read from Airflow Variables:
- `METEO_VILLES` — JSON list of cities (add/remove cities without touching the code)
- `METEO_PAST_DAYS` — lookback window in days
- `METEO_DB_*` — PostgreSQL connection
- `MINIO_*` — MinIO / S3 connection
    """,
)
def weather_data_pipeline():

    # ── Task 1 — City list ─────────────────────────────────────────────────
    @task
    def get_cities() -> list[dict]:
        """
        Read the city list from Airflow Variables.
        Returning a list here triggers Dynamic Task Mapping in the downstream tasks.
        """
        return get_config()["villes"]

    # ── Task 2 — Extract (1 instance per city) ────────────────────────────
    @task(task_id="extract_city")
    def extraire_ville(ville: dict) -> dict:
        """
        Call Open-Meteo for ONE city and return the raw JSON response.
        This task runs in parallel — one instance per city in the UI.
        Failure of one city does not affect the others.
        """
        config  = get_config()
        ssl_ctx = get_ssl_context()
        params  = build_api_params(ville, config["past_days"])
        url     = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)

        with urllib.request.urlopen(url, timeout=15, context=ssl_ctx) as rep:
            if rep.status != 200:
                raise RuntimeError(f"Open-Meteo returned HTTP {rep.status} for {ville['nom']}")
            json_brut = json.loads(rep.read().decode("utf-8"))

        nb_jours = len(json_brut["daily"]["time"])
        print(f"[{ville['nom']}] {nb_jours} days — "
              f"{json_brut['daily']['time'][0]} → {json_brut['daily']['time'][-1]}")

        return {
            "nom":      ville["nom"],
            "data":     json_brut,
            "date_run": datetime.now().strftime("%Y-%m-%d"),
        }

    # ── Task 3 — Bronze storage (1 instance per city) ─────────────────────
    @task(task_id="store_bronze")
    def stocker_bronze(extrait: dict) -> str:
        """
        Write the raw JSON for ONE city to MinIO Bronze layer.
        Returns the S3 key so the audit task can record it.
        Path: meteo-bronze/YYYY-MM-DD/<ville>.json
        """
        config = get_config()
        s3     = get_s3_client(config)

        chemin  = f"{extrait['date_run']}/{extrait['nom'].lower()}.json"
        contenu = json.dumps(extrait["data"], ensure_ascii=False, indent=2).encode("utf-8")

        s3.put_object(
            Bucket=config["minio"]["bronze"],
            Key=chemin,
            Body=contenu,
            ContentType="application/json",
        )
        print(f"[Bronze] {config['minio']['bronze']}/{chemin}  ({len(contenu):,} bytes)")
        return chemin

    # ── Task 4 — Silver transformation (fan-in: all cities → 1 CSV) ───────
    @task(task_id="transform_silver")
    def transformer_silver(tous_extraits: list[dict]) -> list[dict]:
        """
        Collect the raw data from ALL cities (fan-in) and write one CSV to Silver.
        tous_extraits is automatically populated by Airflow with the outputs of
        all extraire_ville instances.
        Path: meteo-silver/YYYY-MM-DD/meteo_villes.csv
        """
        config   = get_config()
        s3       = get_s3_client(config)
        date_run = tous_extraits[0]["date_run"]

        lignes = []
        for extrait in tous_extraits:
            lignes.extend(json_to_rows(extrait["nom"], extrait["data"]))

        # Write CSV to Silver
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=COLONNES_TABLE)
        writer.writeheader()
        writer.writerows(lignes)
        csv_bytes = buffer.getvalue().encode("utf-8")

        chemin_silver = f"{date_run}/meteo_villes.csv"
        s3.put_object(
            Bucket=config["minio"]["silver"],
            Key=chemin_silver,
            Body=csv_bytes,
            ContentType="text/csv",
        )
        print(f"[Silver] {config['minio']['silver']}/{chemin_silver}  "
              f"({len(lignes)} rows, {len(csv_bytes):,} bytes)")

        return lignes

    # ── Task 5 — Gold loading (PostgreSQL upsert) ─────────────────────────
    @task(task_id="load_gold")
    def charger_gold(lignes: list[dict]) -> int:
        """
        Upsert all rows into PostgreSQL meteo_journaliere.
        ON CONFLICT (ville, date) DO UPDATE ensures the pipeline is idempotent.
        """
        config = get_config()
        conn   = psycopg2.connect(**config["db"])
        cur    = conn.cursor()

        sql = """
            INSERT INTO meteo_journaliere
                (ville, date, temp_max_c, temp_min_c, temp_moyenne_c,
                 precipitation_mm, vent_max_kmh, code_meteo)
            VALUES
                (%(ville)s, %(date)s, %(temp_max_c)s, %(temp_min_c)s,
                 %(temp_moyenne_c)s, %(precipitation_mm)s, %(vent_max_kmh)s, %(code_meteo)s)
            ON CONFLICT (ville, date) DO UPDATE SET
                temp_max_c       = EXCLUDED.temp_max_c,
                temp_min_c       = EXCLUDED.temp_min_c,
                temp_moyenne_c   = EXCLUDED.temp_moyenne_c,
                precipitation_mm = EXCLUDED.precipitation_mm,
                vent_max_kmh     = EXCLUDED.vent_max_kmh,
                code_meteo       = EXCLUDED.code_meteo,
                insere_le        = NOW();
        """
        cur.executemany(sql, lignes)
        conn.commit()

        cur.execute(
            "SELECT ville, COUNT(*) FROM meteo_journaliere GROUP BY ville ORDER BY ville;"
        )
        print("[Gold] Current row counts:")
        for row in cur.fetchall():
            print(f"  {row[0]:<12} {row[1]:>4} days")

        cur.close()
        conn.close()
        return len(lignes)

    # ── Task 6 — dbt transformations (run + test) ─────────────────────────
    # Runs after Gold is loaded. Refreshes analytical views/tables and
    # runs all dbt tests as a data quality gate.
    # If dbt test fails → audit is skipped and an alert is raised.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd {{ var.value.get('AIRFLOW_HOME', '/opt/airflow') }}/../dbt && "
            "dbt run --profiles-dir . --project-dir . && "
            "dbt test --profiles-dir . --project-dir ."
        ),
    )

    # ── Task 7 — Audit record ─────────────────────────────────────────────
    @task(task_id="write_audit")
    def ecrire_suivi(nb_lignes: int, chemins_bronze: list[str], **context) -> None:
        """
        Write one audit row to suivi_ingestion with:
        - number of rows loaded into Gold
        - list of Bronze S3 keys written
        """
        config      = get_config()
        villes_str  = ", ".join(sorted({c.split("/")[-1].replace(".json", "") for c in chemins_bronze}))
        message     = (
            f"Bronze: {len(chemins_bronze)} JSON files | "
            f"Silver: 1 CSV | "
            f"Gold: {nb_lignes} rows"
        )

        conn = psycopg2.connect(**config["db"])
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO suivi_ingestion
                (dag_id, run_id, ville, nb_lignes, statut, message, debut_ingestion)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                context["dag"].dag_id,
                context["run_id"],
                villes_str,
                nb_lignes,
                "success",
                message,
                datetime.now(),
            ),
        )
        audit_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        print(f"[Audit] id={audit_id} — {message}")

    # ── DAG wiring ────────────────────────────────────────────────────────
    # Dynamic Task Mapping: one task instance per city for extract + store_bronze
    # Fan-in: transformer_silver collects all city outputs automatically
    # dbt_run runs after Gold load — refreshes analytical models + data quality gate

    villes   = get_cities()
    extraits = extraire_ville.expand(ville=villes)        # → 4 parallel instances
    chemins  = stocker_bronze.expand(extrait=extraits)    # → 4 parallel instances
    lignes   = transformer_silver(tous_extraits=extraits) # fan-in → 1 instance
    nb       = charger_gold(lignes=lignes)
    nb >> dbt_run                                         # dbt after Gold load
    ecrire_suivi(nb_lignes=nb, chemins_bronze=chemins) << dbt_run  # audit after dbt


# Instantiate the DAG
weather_data_pipeline()
