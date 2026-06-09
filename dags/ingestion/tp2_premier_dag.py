import os
import csv
import json
import ssl
import certifi
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "etudiant",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

VILLE = "Paris"
LATITUDE = 48.8566
LONGITUDE = 2.3522
OUTPUT_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "."), "data")


def extraire_donnees(**context):
    """
    Tâche 1 — Extraction
    Appelle l'API Open-Meteo pour récupérer les températures horaires
    de Paris sur les 7 derniers jours (données réelles, sans clé API).
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,precipitation",
        "past_days": 7,
        "forecast_days": 0,
        "timezone": "Europe/Paris",
    }

    url_complete = url + "?" + urllib.parse.urlencode(params)

    print(f"=== EXTRACTION — Météo de {VILLE} ===")
    print(f"Appel API : {url}")
    print(f"Paramètres : latitude={LATITUDE}, longitude={LONGITUDE}, past_days=7")

    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(url_complete, timeout=15, context=ssl_ctx) as reponse:
        statut = reponse.status
        donnees = json.loads(reponse.read().decode("utf-8"))

    releves = donnees["hourly"]
    nb = len(releves["time"])

    print(f"Statut HTTP      : {statut}")
    print(f"Relevés reçus    : {nb} points horaires")
    print(f"Période couverte : {releves['time'][0]}  →  {releves['time'][-1]}")
    print(f"Exemple          : {releves['time'][0]} → {releves['temperature_2m'][0]}°C")

    context["ti"].xcom_push(key="donnees_brutes", value=releves)
    return f"{nb} relevés horaires extraits pour {VILLE}"


def transformer_donnees(**context):
    """
    Tâche 2 — Transformation
    Agrège les données horaires en statistiques journalières :
    température min, max, moyenne et précipitations totales par jour.
    Filtre les valeurs nulles (données manquantes).
    """
    releves = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees")

    print(f"=== TRANSFORMATION — Agrégation par jour ===")

    # Regrouper par date
    par_jour = {}
    for heure, temp, pluie in zip(
        releves["time"],
        releves["temperature_2m"],
        releves["precipitation"],
    ):
        if temp is None:
            continue
        date = heure[:10]
        if date not in par_jour:
            par_jour[date] = {"temperatures": [], "precipitations": []}
        par_jour[date]["temperatures"].append(temp)
        if pluie is not None:
            par_jour[date]["precipitations"].append(pluie)

    # Calculer les statistiques par jour
    stats_journalieres = []
    for date in sorted(par_jour):
        temps = par_jour[date]["temperatures"]
        pluies = par_jour[date]["precipitations"]
        stats = {
            "date": date,
            "temp_min": round(min(temps), 1),
            "temp_max": round(max(temps), 1),
            "temp_moyenne": round(sum(temps) / len(temps), 1),
            "precipitation_mm": round(sum(pluies), 1),
            "nb_releves": len(temps),
        }
        stats_journalieres.append(stats)
        print(
            f"  {date} | "
            f"min: {stats['temp_min']}°C | "
            f"max: {stats['temp_max']}°C | "
            f"moy: {stats['temp_moyenne']}°C | "
            f"pluie: {stats['precipitation_mm']}mm"
        )

    nb_valides = sum(s["nb_releves"] for s in stats_journalieres)
    nb_total = len(releves["time"])
    print(f"\nRelevés valides  : {nb_valides} / {nb_total}")
    print(f"Jours agrégés    : {len(stats_journalieres)}")

    context["ti"].xcom_push(key="stats_journalieres", value=stats_journalieres)
    return f"{len(stats_journalieres)} jours de données transformées"


def charger_donnees(**context):
    """
    Tâche 3 — Chargement
    Exporte les statistiques journalières dans un fichier CSV horodaté
    dans le dossier data/ (simule un chargement en base de données).
    """
    stats = context["ti"].xcom_pull(key="stats_journalieres", task_ids="transformer_donnees")

    print(f"=== CHARGEMENT — Export CSV ===")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    nom_fichier = f"meteo_{VILLE.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    chemin = os.path.join(OUTPUT_DIR, nom_fichier)

    colonnes = ["date", "temp_min", "temp_max", "temp_moyenne", "precipitation_mm", "nb_releves"]
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colonnes)
        writer.writeheader()
        writer.writerows(stats)

    print(f"Fichier créé     : {chemin}")
    print(f"Lignes écrites   : {len(stats)}")
    print(f"\nAperçu du fichier :")
    print(f"  {'DATE':<12} {'MIN':>6} {'MAX':>6} {'MOY':>6} {'PLUIE':>8}")
    print(f"  {'-'*44}")
    for s in stats:
        print(
            f"  {s['date']:<12} "
            f"{s['temp_min']:>5}°C "
            f"{s['temp_max']:>5}°C "
            f"{s['temp_moyenne']:>5}°C "
            f"{s['precipitation_mm']:>6}mm"
        )

    return f"Fichier exporté : {nom_fichier}"


with DAG(
    dag_id="tp2_pipeline_etl_simple",
    description="TP2 — Pipeline ETL réel : météo Paris via API Open-Meteo → CSV",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2", "etl", "api", "meteo"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire_donnees,
    )

    tache_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    tache_chargement = PythonOperator(
        task_id="charger_donnees",
        python_callable=charger_donnees,
    )

    # Dépendances explicites : extraction → transformation → chargement
    tache_extraction >> tache_transformation >> tache_chargement
