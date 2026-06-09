"""
TP3 — Architecture Data Lake Medallion
Pipeline : API Open-Meteo → MinIO Bronze → MinIO Silver → PostgreSQL Gold

Architecture :
  BRONZE : JSON brut stocké dans MinIO tel que reçu de l'API
  SILVER : CSV transformé stocké dans MinIO (champs sélectionnés)
  GOLD   : Données chargées dans PostgreSQL pour l'analyse

Tâches :
  1. extraire_api         → appel API Open-Meteo
  2. stocker_bronze       → JSON brut dans MinIO (meteo-bronze)
  3. transformer_silver   → CSV propre dans MinIO (meteo-silver)
  4. charger_gold         → insertion dans PostgreSQL
  5. ecrire_suivi         → traçabilité dans suivi_ingestion
"""

import io
import json
import csv
import ssl
import certifi
import urllib.request
import urllib.parse
import boto3
import psycopg2
from botocore.client import Config
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "etudiant",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

CHAMPS_API = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]


def get_config():
    return {
        "villes":    json.loads(Variable.get("METEO_VILLES")),
        "past_days": int(Variable.get("METEO_PAST_DAYS", default_var=7)),
        "minio": {
            "endpoint":   Variable.get("MINIO_ENDPOINT",      default_var="http://localhost:9000"),
            "access_key": Variable.get("MINIO_ACCESS_KEY",    default_var="minioadmin"),
            "secret_key": Variable.get("MINIO_SECRET_KEY",    default_var="minioadmin"),
            "bronze":     Variable.get("MINIO_BUCKET_BRONZE", default_var="meteo-bronze"),
            "silver":     Variable.get("MINIO_BUCKET_SILVER", default_var="meteo-silver"),
        },
        "db": {
            "host":     Variable.get("METEO_DB_HOST",     default_var="localhost"),
            "port":     int(Variable.get("METEO_DB_PORT", default_var=5432)),
            "dbname":   Variable.get("METEO_DB_NAME",     default_var="meteo_db"),
            "user":     Variable.get("METEO_DB_USER",     default_var="postgres"),
            "password": Variable.get("METEO_DB_PASSWORD", default_var="postgres"),
        },
    }


def get_s3_client(config):
    return boto3.client(
        "s3",
        endpoint_url=config["minio"]["endpoint"],
        aws_access_key_id=config["minio"]["access_key"],
        aws_secret_access_key=config["minio"]["secret_key"],
        config=Config(signature_version="s3v4"),
    )


# ---------------------------------------------------------------------------
# TÂCHE 1 — Extraction API
# Appelle Open-Meteo pour chaque ville, stocke le JSON brut en XCom.
# ---------------------------------------------------------------------------
def extraire_api(**context):
    config   = get_config()
    ssl_ctx  = ssl.create_default_context(cafile=certifi.where())
    base_url = "https://api.open-meteo.com/v1/forecast"
    resultats = {}

    print("=== [EXTRACTION] API Open-Meteo ===")
    for ville in config["villes"]:
        params = {
            "latitude":      ville["latitude"],
            "longitude":     ville["longitude"],
            "daily":         ",".join(CHAMPS_API),
            "past_days":     config["past_days"],
            "forecast_days": 0,
            "timezone":      "Europe/Paris",
        }
        url = base_url + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=15, context=ssl_ctx) as rep:
            resultats[ville["nom"]] = json.loads(rep.read().decode("utf-8"))
        print(f"  [{ville['nom']}] HTTP {rep.status} — {len(resultats[ville['nom']]['daily']['time'])} jours")

    context["ti"].xcom_push(key="donnees_brutes", value=resultats)
    context["ti"].xcom_push(key="debut_ingestion", value=datetime.now().isoformat())
    return f"{len(resultats)} villes extraites"


# ---------------------------------------------------------------------------
# TÂCHE 2 — Stockage Bronze
# Écrit le JSON brut dans MinIO bucket meteo-bronze.
# Chemin : meteo-bronze/YYYY-MM-DD/<ville>.json
# C'est la couche RAW : aucune transformation, données d'origine conservées.
# ---------------------------------------------------------------------------
def stocker_bronze(**context):
    config        = get_config()
    donnees_brutes = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_api")
    s3            = get_s3_client(config)
    date_run      = datetime.now().strftime("%Y-%m-%d")
    fichiers      = []

    print(f"=== [BRONZE] Stockage JSON brut → MinIO ({config['minio']['bronze']}) ===")

    for nom_ville, json_brut in donnees_brutes.items():
        chemin    = f"{date_run}/{nom_ville.lower()}.json"
        contenu   = json.dumps(json_brut, ensure_ascii=False, indent=2)
        taille    = len(contenu.encode("utf-8"))

        s3.put_object(
            Bucket=config["minio"]["bronze"],
            Key=chemin,
            Body=contenu.encode("utf-8"),
            ContentType="application/json",
        )
        fichiers.append(chemin)
        print(f"  Écrit : {config['minio']['bronze']}/{chemin} ({taille} octets)")

    context["ti"].xcom_push(key="chemins_bronze", value=fichiers)
    context["ti"].xcom_push(key="date_run", value=date_run)
    return f"{len(fichiers)} fichiers JSON écrits dans Bronze"


# ---------------------------------------------------------------------------
# TÂCHE 3 — Transformation Silver
# Lit le JSON depuis MinIO Bronze, sélectionne les champs utiles,
# produit un CSV et l'écrit dans MinIO bucket meteo-silver.
# Chemin : meteo-silver/YYYY-MM-DD/meteo_villes.csv
# ---------------------------------------------------------------------------
def transformer_silver(**context):
    config  = get_config()
    donnees = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_api")
    date_run = context["ti"].xcom_pull(key="date_run", task_ids="stocker_bronze")
    s3      = get_s3_client(config)

    print(f"=== [SILVER] Transformation + CSV → MinIO ({config['minio']['silver']}) ===")

    colonnes = ["ville", "date", "temp_max_c", "temp_min_c", "temp_moyenne_c",
                "precipitation_mm", "vent_max_kmh", "code_meteo"]
    lignes = []

    for nom_ville, json_brut in donnees.items():
        daily = json_brut["daily"]
        for i in range(len(daily["time"])):
            lignes.append({
                "ville":            nom_ville,
                "date":             daily["time"][i],
                "temp_max_c":       daily["temperature_2m_max"][i],
                "temp_min_c":       daily["temperature_2m_min"][i],
                "temp_moyenne_c":   daily["temperature_2m_mean"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
                "vent_max_kmh":     daily["windspeed_10m_max"][i],
                "code_meteo":       daily["weathercode"][i],
            })

    # Écriture CSV en mémoire puis upload dans MinIO
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=colonnes)
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

    taille = len(csv_bytes)
    print(f"  Écrit : {config['minio']['silver']}/{chemin_silver} ({taille} octets)")
    print(f"  Lignes : {len(lignes)} ({len(donnees)} villes × {len(lignes)//len(donnees)} jours)")

    context["ti"].xcom_push(key="lignes_silver", value=lignes)
    return f"{len(lignes)} lignes écrites dans Silver"


# ---------------------------------------------------------------------------
# TÂCHE 4 — Chargement Gold (PostgreSQL)
# Lit les données depuis le XCom Silver et les insère dans PostgreSQL.
# C'est la couche GOLD : données prêtes pour l'analyse et les dashboards.
# ---------------------------------------------------------------------------
def charger_gold(**context):
    config = get_config()
    lignes = context["ti"].xcom_pull(key="lignes_silver", task_ids="transformer_silver")

    print(f"=== [GOLD] Chargement PostgreSQL ({config['db']['dbname']}) ===")

    conn = psycopg2.connect(**config["db"])
    cur  = conn.cursor()

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

    print(f"  Lignes insérées/mises à jour : {len(lignes)}")
    cur.execute("""
        SELECT ville, COUNT(*) AS jours
        FROM meteo_journaliere
        GROUP BY ville ORDER BY ville;
    """)
    print("\n  Contenu actuel de meteo_journaliere :")
    print(f"  {'VILLE':<12} {'JOURS':>5}")
    print(f"  {'-'*20}")
    for row in cur.fetchall():
        print(f"  {row[0]:<12} {row[1]:>5}")

    cur.close()
    conn.close()

    context["ti"].xcom_push(key="nb_lignes_gold", value=len(lignes))
    return f"{len(lignes)} lignes chargées dans Gold (PostgreSQL)"


# ---------------------------------------------------------------------------
# TÂCHE 5 — Suivi d'ingestion
# Enregistre les métadonnées du run dans suivi_ingestion.
# Inclut les chemins Bronze et Silver pour la traçabilité complète.
# ---------------------------------------------------------------------------
def ecrire_suivi(**context):
    config        = get_config()
    nb_lignes     = context["ti"].xcom_pull(key="nb_lignes_gold",  task_ids="charger_gold")
    chemins_bronze = context["ti"].xcom_pull(key="chemins_bronze", task_ids="stocker_bronze")
    debut         = context["ti"].xcom_pull(key="debut_ingestion", task_ids="extraire_api")
    villes_str    = ", ".join([v["nom"] for v in config["villes"]])

    message = (
        f"Bronze: {len(chemins_bronze)} fichiers JSON | "
        f"Silver: 1 CSV | "
        f"Gold: {nb_lignes} lignes PostgreSQL"
    )

    print("=== [SUIVI] Traçabilité ===")

    conn = psycopg2.connect(**config["db"])
    cur  = conn.cursor()

    cur.execute("""
        INSERT INTO suivi_ingestion
            (dag_id, run_id, ville, nb_lignes, statut, message, debut_ingestion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        context["dag"].dag_id,
        context["run_id"],
        villes_str,
        nb_lignes,
        "success",
        message,
        debut,
    ))

    suivi_id = cur.fetchone()[0]
    conn.commit()

    print(f"  Suivi id={suivi_id}")
    print(f"  {message}")

    cur.execute("""
        SELECT id, dag_id, nb_lignes, statut, message, fin_ingestion
        FROM suivi_ingestion ORDER BY fin_ingestion DESC LIMIT 3;
    """)
    print("\n  Derniers runs :")
    for row in cur.fetchall():
        print(f"  [{row[0]}] {row[1]} — {row[2]} lignes — {row[3]} — {row[5]}")
        print(f"       {row[4]}")

    cur.close()
    conn.close()
    return f"Suivi enregistré (id={suivi_id})"


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tp3_data_lake",
    description="TP3 — Data Lake Medallion : API → Bronze (MinIO) → Silver (MinIO) → Gold (PostgreSQL)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp3", "datalake", "minio", "bronze", "silver", "gold", "postgresql"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_api",
        python_callable=extraire_api,
    )

    tache_bronze = PythonOperator(
        task_id="stocker_bronze",
        python_callable=stocker_bronze,
    )

    tache_silver = PythonOperator(
        task_id="transformer_silver",
        python_callable=transformer_silver,
    )

    tache_gold = PythonOperator(
        task_id="charger_gold",
        python_callable=charger_gold,
    )

    tache_suivi = PythonOperator(
        task_id="ecrire_suivi",
        python_callable=ecrire_suivi,
    )

    # Architecture Medallion : extraction → bronze → silver → gold → suivi
    tache_extraction >> tache_bronze >> tache_silver >> tache_gold >> tache_suivi
