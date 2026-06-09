"""
TP 2B — Fin d'après-midi
Pipeline complet API → transformation → PostgreSQL

Tâches :
  1. extraire_donnees_brutes      : appel API Open-Meteo (paramétrable)
  2. transformer_donnees          : sélection et structuration des champs
  3. charger_postgresql           : insertion dans meteo_journaliere
  4. ecrire_suivi_ingestion       : traçabilité dans suivi_ingestion
"""

import json
import ssl
import certifi
import urllib.request
import urllib.parse
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "etudiant",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# --- Paramètres lus depuis les Variables Airflow (pas de hardcode) ---
def get_config():
    return {
        "villes":    json.loads(Variable.get("METEO_VILLES")),
        "past_days": int(Variable.get("METEO_PAST_DAYS", default_var=7)),
        "db": {
            "host":     Variable.get("METEO_DB_HOST",     default_var="localhost"),
            "port":     int(Variable.get("METEO_DB_PORT", default_var=5432)),
            "dbname":   Variable.get("METEO_DB_NAME",     default_var="meteo_db"),
            "user":     Variable.get("METEO_DB_USER",     default_var="postgres"),
            "password": Variable.get("METEO_DB_PASSWORD", default_var="postgres"),
        },
    }

CHAMPS_API = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]


# ---------------------------------------------------------------------------
# TÂCHE 1 — Extraction brute
# Appelle l'API pour chaque ville configurée dans la Variable METEO_VILLES.
# Sauvegarde le JSON brut sans aucune transformation.
# ---------------------------------------------------------------------------
def extraire_donnees_brutes(**context):
    config   = get_config()
    ssl_ctx  = ssl.create_default_context(cafile=certifi.where())
    base_url = "https://api.open-meteo.com/v1/forecast"
    resultats_bruts = {}

    print("=== EXTRACTION — API Open-Meteo ===")
    print(f"Villes    : {[v['nom'] for v in config['villes']]}")
    print(f"Période   : {config['past_days']} derniers jours\n")

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
            donnees_json = json.loads(rep.read().decode("utf-8"))

        resultats_bruts[ville["nom"]] = donnees_json
        nb = len(donnees_json["daily"]["time"])
        print(f"  [{ville['nom']}] HTTP {rep.status} — {nb} jours "
              f"({donnees_json['daily']['time'][0]} → {donnees_json['daily']['time'][-1]})")

    print(f"\nExtraction terminée : {len(resultats_bruts)} villes")
    context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)
    return f"{len(resultats_bruts)} villes extraites"


# ---------------------------------------------------------------------------
# TÂCHE 2 — Transformation
# Sélectionne uniquement les champs utiles et construit la structure
# correspondant exactement à la table meteo_journaliere.
# ---------------------------------------------------------------------------
def transformer_donnees(**context):
    donnees_brutes = context["ti"].xcom_pull(
        key="donnees_brutes", task_ids="extraire_donnees_brutes"
    )

    print("=== TRANSFORMATION — Structuration pour PostgreSQL ===")
    lignes = []

    for nom_ville, json_brut in donnees_brutes.items():
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
        print(f"  [{nom_ville}] {len(daily['time'])} lignes structurées")

    print(f"\nTotal : {len(lignes)} lignes prêtes pour PostgreSQL")
    context["ti"].xcom_push(key="lignes_transformees", value=lignes)
    context["ti"].xcom_push(key="debut_ingestion", value=datetime.now().isoformat())
    return f"{len(lignes)} lignes transformées"


# ---------------------------------------------------------------------------
# TÂCHE 3 — Chargement PostgreSQL
# Insère les données dans meteo_journaliere.
# ON CONFLICT (ville, date) DO UPDATE = mise à jour si la ligne existe déjà.
# ---------------------------------------------------------------------------
def charger_postgresql(**context):
    config = get_config()
    lignes = context["ti"].xcom_pull(
        key="lignes_transformees", task_ids="transformer_donnees"
    )

    print("=== CHARGEMENT — PostgreSQL (meteo_journaliere) ===")
    print(f"Base : {config['db']['dbname']} @ {config['db']['host']}:{config['db']['port']}")

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

    print(f"Lignes insérées/mises à jour : {cur.rowcount}")
    print("\nAperçu des données chargées :")
    cur.execute("""
        SELECT ville, date, temp_max_c, temp_min_c, precipitation_mm
        FROM meteo_journaliere
        ORDER BY ville, date DESC
        LIMIT 8;
    """)
    print(f"  {'VILLE':<12} {'DATE':<12} {'MAX':>6} {'MIN':>6} {'PLUIE':>8}")
    print(f"  {'-'*48}")
    for row in cur.fetchall():
        print(f"  {row[0]:<12} {str(row[1]):<12} {str(row[2]):>5}°C {str(row[3]):>5}°C {str(row[4]):>6}mm")

    cur.close()
    conn.close()

    context["ti"].xcom_push(key="nb_lignes_chargees", value=len(lignes))
    return f"{len(lignes)} lignes chargées dans PostgreSQL"


# ---------------------------------------------------------------------------
# TÂCHE 4 — Suivi d'ingestion
# Écrit une ligne dans suivi_ingestion pour tracer chaque exécution du DAG.
# ---------------------------------------------------------------------------
def ecrire_suivi_ingestion(**context):
    config        = get_config()
    nb_lignes     = context["ti"].xcom_pull(key="nb_lignes_chargees", task_ids="charger_postgresql")
    debut         = context["ti"].xcom_pull(key="debut_ingestion",    task_ids="transformer_donnees")
    run_id        = context["run_id"]
    dag_id        = context["dag"].dag_id
    villes_str    = ", ".join([v["nom"] for v in config["villes"]])

    print("=== SUIVI D'INGESTION ===")

    conn = psycopg2.connect(**config["db"])
    cur  = conn.cursor()

    cur.execute("""
        INSERT INTO suivi_ingestion
            (dag_id, run_id, ville, nb_lignes, statut, message, debut_ingestion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        dag_id,
        run_id,
        villes_str,
        nb_lignes,
        "success",
        f"Pipeline exécuté avec succès — {nb_lignes} lignes chargées pour : {villes_str}",
        debut,
    ))

    suivi_id = cur.fetchone()[0]
    conn.commit()

    print(f"Ligne de suivi créée (id={suivi_id})")
    print(f"  dag_id   : {dag_id}")
    print(f"  run_id   : {run_id}")
    print(f"  villes   : {villes_str}")
    print(f"  nb_lignes: {nb_lignes}")
    print(f"  statut   : success")

    print("\nHistorique des ingestions :")
    cur.execute("""
        SELECT id, dag_id, ville, nb_lignes, statut, fin_ingestion
        FROM suivi_ingestion
        ORDER BY fin_ingestion DESC
        LIMIT 5;
    """)
    print(f"  {'ID':>3} {'DAG':<28} {'VILLES':<40} {'LIGNES':>7} {'STATUT':<10} FIN")
    print(f"  {'-'*100}")
    for row in cur.fetchall():
        print(f"  {row[0]:>3} {row[1]:<28} {row[2]:<40} {str(row[3]):>7} {row[4]:<10} {row[5]}")

    cur.close()
    conn.close()
    return f"Suivi enregistré (id={suivi_id})"


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tp2b_pipeline_postgresql",
    description="TP2B — Pipeline complet API Open-Meteo → transformation → PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2b", "etl", "postgresql", "meteo"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_donnees_brutes",
        python_callable=extraire_donnees_brutes,
    )

    tache_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    tache_chargement = PythonOperator(
        task_id="charger_postgresql",
        python_callable=charger_postgresql,
    )

    tache_suivi = PythonOperator(
        task_id="ecrire_suivi_ingestion",
        python_callable=ecrire_suivi_ingestion,
    )

    # Séparation claire : extraction → transformation → chargement → suivi
    tache_extraction >> tache_transformation >> tache_chargement >> tache_suivi
