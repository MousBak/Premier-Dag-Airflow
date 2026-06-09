"""
TP 2A — Fin de matinée
Préparer une ingestion API météo

Sujet  : Récupérer des données Open-Meteo pour plusieurs villes.
DAG    : tp2a_ingestion_meteo
Tâches : extraire_donnees_brutes → preparer_donnees_pipeline → charger_table_cible

Champs retenus pour la table cible (justification en bas de fichier).
"""

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

# --- Configuration des villes ---
VILLES = [
    {"nom": "Paris",     "latitude": 48.8566, "longitude":  2.3522},
    {"nom": "Lyon",      "latitude": 45.7640, "longitude":  4.8357},
    {"nom": "Marseille", "latitude": 43.2965, "longitude":  5.3698},
    {"nom": "Bordeaux",  "latitude": 44.8378, "longitude": -0.5792},
]

# Champs demandés à l'API (endpoint /forecast daily)
CHAMPS_API = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]

OUTPUT_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "."), "data")


# ---------------------------------------------------------------------------
# TÂCHE 1 — Extraction brute
# Rôle : appeler l'API et sauvegarder la réponse JSON telle quelle.
# Ne fait AUCUNE transformation : on garde ce qui vient de l'API, rien de plus.
# ---------------------------------------------------------------------------
def extraire_donnees_brutes(**context):
    base_url = "https://api.open-meteo.com/v1/forecast"
    ssl_ctx  = ssl.create_default_context(cafile=certifi.where())
    resultats_bruts = {}

    print("=== EXTRACTION BRUTE — API Open-Meteo ===")
    print(f"Villes ciblées : {[v['nom'] for v in VILLES]}")
    print(f"Champs API demandés : {CHAMPS_API}\n")

    for ville in VILLES:
        params = {
            "latitude":      ville["latitude"],
            "longitude":     ville["longitude"],
            "daily":         ",".join(CHAMPS_API),
            "past_days":     7,
            "forecast_days": 0,
            "timezone":      "Europe/Paris",
        }
        url = base_url + "?" + urllib.parse.urlencode(params)

        with urllib.request.urlopen(url, timeout=15, context=ssl_ctx) as reponse:
            donnees_json = json.loads(reponse.read().decode("utf-8"))

        # Stockage brut : on garde TOUT ce que l'API renvoie
        resultats_bruts[ville["nom"]] = donnees_json

        nb_jours = len(donnees_json["daily"]["time"])
        print(f"  [{ville['nom']}] HTTP {reponse.status} — {nb_jours} jours récupérés "
              f"({donnees_json['daily']['time'][0]} → {donnees_json['daily']['time'][-1]})")

    print(f"\nExtraction terminée : {len(resultats_bruts)} villes")
    context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)
    return f"{len(resultats_bruts)} villes extraites"


# ---------------------------------------------------------------------------
# TÂCHE 2 — Préparation pour le pipeline
# Rôle : sélectionner uniquement les champs utiles, renommer, structurer
#        en vue de la table cible. Aucun appel réseau ici.
#
# Champs RETENUS et justification :
#   - ville            : clé de regroupement géographique
#   - date             : dimension temporelle obligatoire
#   - temp_max_c       : indicateur thermique journalier haut
#   - temp_min_c       : indicateur thermique journalier bas
#   - temp_moyenne_c   : synthèse journalière, utile pour les moyennes
#   - precipitation_mm : volume de pluie (alertes, irrigation, etc.)
#   - vent_max_kmh     : vitesse max du vent (sécurité, événements)
#   - code_meteo       : code WMO (permet de catégoriser le temps : soleil/pluie/neige)
#
# Champs NON RETENUS :
#   - latitude/longitude   : redondant avec le nom de ville
#   - elevation            : inutile pour l'analyse météo quotidienne
#   - timezone/utc_offset  : on fixe Europe/Paris en amont
#   - generationtime_ms    : métadonnée technique de l'API, sans valeur métier
# ---------------------------------------------------------------------------
def preparer_donnees_pipeline(**context):
    donnees_brutes = context["ti"].xcom_pull(key="donnees_brutes",
                                              task_ids="extraire_donnees_brutes")

    print("=== PRÉPARATION POUR LE PIPELINE ===")
    print("Sélection des champs utiles / renommage / structuration\n")

    table_cible = []

    for nom_ville, json_brut in donnees_brutes.items():
        daily = json_brut["daily"]
        nb    = len(daily["time"])

        for i in range(nb):
            ligne = {
                "ville":            nom_ville,
                "date":             daily["time"][i],
                "temp_max_c":       daily["temperature_2m_max"][i],
                "temp_min_c":       daily["temperature_2m_min"][i],
                "temp_moyenne_c":   daily["temperature_2m_mean"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
                "vent_max_kmh":     daily["windspeed_10m_max"][i],
                "code_meteo":       daily["weathercode"][i],
            }
            table_cible.append(ligne)

        print(f"  [{nom_ville}] {nb} lignes préparées")
        # Aperçu du premier jour
        apercu = table_cible[-(nb)]
        print(f"    → {apercu['date']} | "
              f"max: {apercu['temp_max_c']}°C | "
              f"min: {apercu['temp_min_c']}°C | "
              f"moy: {apercu['temp_moyenne_c']}°C | "
              f"pluie: {apercu['precipitation_mm']}mm | "
              f"vent: {apercu['vent_max_kmh']}km/h | "
              f"code: {apercu['code_meteo']}")

    total = len(table_cible)
    print(f"\nTotal : {total} lignes prêtes pour la table cible "
          f"({len(donnees_brutes)} villes × 7 jours)")

    context["ti"].xcom_push(key="table_cible", value=table_cible)
    return f"{total} lignes préparées"


# ---------------------------------------------------------------------------
# TÂCHE 3 — Chargement dans la table cible
# Rôle : écrire le résultat final dans un CSV structuré.
#        Chaque ligne = 1 jour pour 1 ville (format tabulaire, prêt pour BDD).
# ---------------------------------------------------------------------------
def charger_table_cible(**context):
    table_cible = context["ti"].xcom_pull(key="table_cible",
                                           task_ids="preparer_donnees_pipeline")

    print("=== CHARGEMENT — Table cible ===")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    nom_fichier = f"meteo_villes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    chemin      = os.path.join(OUTPUT_DIR, nom_fichier)

    colonnes = ["ville", "date", "temp_max_c", "temp_min_c", "temp_moyenne_c",
                "precipitation_mm", "vent_max_kmh", "code_meteo"]

    with open(chemin, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colonnes)
        writer.writeheader()
        writer.writerows(table_cible)

    print(f"Fichier créé  : {chemin}")
    print(f"Lignes écrites: {len(table_cible)}")
    print(f"\nAperçu (5 premières lignes) :")
    print(f"  {'VILLE':<12} {'DATE':<12} {'MAX':>6} {'MIN':>6} {'MOY':>6} {'PLUIE':>7} {'VENT':>8} {'CODE':>5}")
    print(f"  {'-'*65}")
    for ligne in table_cible[:5]:
        print(f"  {ligne['ville']:<12} {ligne['date']:<12} "
              f"{str(ligne['temp_max_c']):>5}°C "
              f"{str(ligne['temp_min_c']):>5}°C "
              f"{str(ligne['temp_moyenne_c']):>5}°C "
              f"{str(ligne['precipitation_mm']):>6}mm "
              f"{str(ligne['vent_max_kmh']):>6}km/h "
              f"{str(ligne['code_meteo']):>5}")

    return f"Table cible exportée : {nom_fichier}"


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tp2a_ingestion_meteo",
    description="TP2A — Ingestion API météo Open-Meteo pour 4 villes françaises",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2a", "etl", "api", "meteo", "multi-villes"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_donnees_brutes",
        python_callable=extraire_donnees_brutes,
    )

    tache_preparation = PythonOperator(
        task_id="preparer_donnees_pipeline",
        python_callable=preparer_donnees_pipeline,
    )

    tache_chargement = PythonOperator(
        task_id="charger_table_cible",
        python_callable=charger_table_cible,
    )

    # Séparation explicite : récupération → préparation → chargement
    tache_extraction >> tache_preparation >> tache_chargement
