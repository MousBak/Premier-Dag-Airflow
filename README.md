# TP2 — Premier DAG Airflow : Pipeline ETL avec API réelle

## Présentation

Ce projet est réalisé dans le cadre du **TP2 — Fin d'après-midi** sur Apache Airflow.  
L'objectif est de créer un premier DAG qui traduit un workflow ETL (Extract, Transform, Load) en tâches Airflow ordonnées avec des dépendances explicites, en utilisant une **API réelle** comme source de données.

> **Source de données** : [Open-Meteo](https://open-meteo.com/) — API météo gratuite, sans clé API, données réelles en temps réel.

---

## Qu'est-ce qu'un DAG ?

Un **DAG** (Directed Acyclic Graph) est le concept central d'Airflow.  
C'est un graphe orienté sans cycle qui représente un workflow : un ensemble de tâches à exécuter dans un ordre précis.

- **Dirigé** → chaque tâche a une direction (A doit finir avant B)
- **Acyclique** → pas de boucle possible (A ne peut pas dépendre de lui-même)
- **Graphe** → les tâches sont des nœuds reliés par des flèches

```
extraire_donnees  ──→  transformer_donnees  ──→  charger_donnees
```

---

## Structure du projet

```
Airflow/
├── dags/
│   └── tp2_premier_dag.py     # Le DAG Python (fichier principal)
├── data/
│   └── meteo_paris_*.csv      # Fichiers CSV générés à chaque exécution
├── logs/                      # Logs générés automatiquement par Airflow
├── plugins/                   # Dossier pour les plugins personnalisés (vide ici)
├── airflow_venv/              # Environnement Python virtuel avec Airflow installé
├── airflow.db                 # Base de données SQLite d'Airflow
├── docker-compose.yml         # Configuration Docker alternative
└── README.md                  # Ce fichier
```

---

## Le DAG : `tp2_pipeline_etl_simple`

### Vue d'ensemble

Le DAG implémente un pipeline **ETL réel** (Extract → Transform → Load) sur des données météo de Paris :

| Étape | Tâche Airflow | Ce qu'elle fait |
|-------|--------------|-----------------|
| **Extract** | `extraire_donnees` | Appelle l'API Open-Meteo et récupère **168 relevés horaires** de température et de précipitations sur les 7 derniers jours |
| **Transform** | `transformer_donnees` | Agrège les données horaires en **7 résumés journaliers** (min, max, moyenne, précipitations) et filtre les valeurs nulles |
| **Load** | `charger_donnees` | Exporte les statistiques dans un **fichier CSV horodaté** dans le dossier `data/` |

### Dépendances entre tâches

Les dépendances sont définies **explicitement** avec l'opérateur `>>` :

```python
tache_extraction >> tache_transformation >> tache_chargement
```

Cela signifie :
- `transformer_donnees` ne démarre **que si** `extraire_donnees` a réussi
- `charger_donnees` ne démarre **que si** `transformer_donnees` a réussi

Si une tâche échoue, les suivantes sont automatiquement bloquées.

### Passage de données entre tâches — XCom

Les tâches s'échangent les données via **XCom** (Cross-Communication), le mécanisme natif d'Airflow :

```python
# Tâche 1 : pousse les données brutes
context["ti"].xcom_push(key="donnees_brutes", value=releves)

# Tâche 2 : récupère les données de la tâche précédente
releves = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees")
```

---

## L'API utilisée : Open-Meteo

| Propriété | Valeur |
|-----------|--------|
| URL | `https://api.open-meteo.com/v1/forecast` |
| Authentification | Aucune (gratuite et publique) |
| Données récupérées | Température horaire + précipitations |
| Localisation | Paris (latitude 48.8566, longitude 2.3522) |
| Période | 7 derniers jours |

**Exemple de réponse brute (simplifié) :**
```json
{
  "hourly": {
    "time": ["2026-06-01T00:00", "2026-06-01T01:00", ...],
    "temperature_2m": [18.9, 18.2, 17.8, ...],
    "precipitation": [0.0, 0.0, 0.2, ...]
  }
}
```

---

## Code annoté

```python
import os, csv, json, ssl, certifi
import urllib.request, urllib.parse
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Paramètres par défaut appliqués à toutes les tâches du DAG
default_args = {
    "owner": "etudiant",
    "retries": 1,                        # 1 retry automatique en cas d'échec
    "retry_delay": timedelta(minutes=2), # Attente 2 min avant de retenter
}

def extraire_donnees(**context):
    # Appel HTTP réel vers l'API Open-Meteo (urllib = bibliothèque standard Python)
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(url_complete, timeout=15, context=ssl_ctx) as reponse:
        donnees = json.loads(reponse.read().decode("utf-8"))
    # Partage les données brutes avec la tâche suivante via XCom
    context["ti"].xcom_push(key="donnees_brutes", value=donnees["hourly"])

def transformer_donnees(**context):
    # Récupère les données de la tâche précédente via XCom
    releves = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees")
    # Agrège 168 points horaires en 7 résumés journaliers
    ...
    context["ti"].xcom_push(key="stats_journalieres", value=stats_journalieres)

def charger_donnees(**context):
    # Récupère les données transformées et les écrit dans un CSV
    stats = context["ti"].xcom_pull(key="stats_journalieres", task_ids="transformer_donnees")
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[...])
        writer.writeheader()
        writer.writerows(stats)

with DAG(
    dag_id="tp2_pipeline_etl_simple",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Déclenchement manuel uniquement
    catchup=False,            # Ne pas rattraper les runs passés
    tags=["tp2", "etl", "api", "meteo"],
) as dag:
    tache_extraction    = PythonOperator(task_id="extraire_donnees",    python_callable=extraire_donnees)
    tache_transformation = PythonOperator(task_id="transformer_donnees", python_callable=transformer_donnees)
    tache_chargement    = PythonOperator(task_id="charger_donnees",     python_callable=charger_donnees)

    tache_extraction >> tache_transformation >> tache_chargement
```

---

## Installation et lancement

### Prérequis

- Python 3.12+
- macOS / Linux
- Connexion internet (pour l'appel API Open-Meteo)

### Étape 1 — Créer l'environnement virtuel

```bash
python3 -m venv airflow_venv
source airflow_venv/bin/activate
```

### Étape 2 — Installer Apache Airflow et les dépendances

```bash
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"
pip install "apache-airflow==2.9.1" --constraint "${CONSTRAINT_URL}"
pip install certifi
```

### Étape 3 — Initialiser la base de données

```bash
export AIRFLOW_HOME=$(pwd)
airflow db migrate
```

### Étape 4 — Créer un utilisateur administrateur

```bash
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Étape 5 — Lancer le scheduler et le webserver

```bash
# Terminal 1 — Scheduler
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow scheduler

# Terminal 2 — Webserver
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow webserver --port 8081
```

### Étape 6 — Exécuter le DAG

```bash
# Option A : via l'interface web
# Ouvrir http://localhost:8081 → activer le DAG → cliquer ▶

# Option B : via la ligne de commande (mode test, processus unique)
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2_pipeline_etl_simple
```

---

## Résultat d'une exécution réelle

### Logs de `extraire_donnees`
```
=== EXTRACTION — Météo de Paris ===
Appel API : https://api.open-meteo.com/v1/forecast
Statut HTTP      : 200
Relevés reçus    : 168 points horaires
Période couverte : 2026-06-01T00:00  →  2026-06-07T23:00
Exemple          : 2026-06-01T00:00 → 18.9°C
```

### Logs de `transformer_donnees`
```
=== TRANSFORMATION — Agrégation par jour ===
  2026-06-01 | min: 15.0°C | max: 26.3°C | moy: 20.7°C | pluie: 0.0mm
  2026-06-02 | min: 17.3°C | max: 22.7°C | moy: 19.5°C | pluie: 5.6mm
  2026-06-03 | min: 14.3°C | max: 20.6°C | moy: 17.4°C | pluie: 0.0mm
  2026-06-04 | min: 15.3°C | max: 20.5°C | moy: 17.4°C | pluie: 2.1mm
  2026-06-05 | min: 12.2°C | max: 19.3°C | moy: 15.9°C | pluie: 0.0mm
  2026-06-06 | min: 14.0°C | max: 21.1°C | moy: 16.9°C | pluie: 1.7mm
  2026-06-07 | min: 12.5°C | max: 23.3°C | moy: 17.9°C | pluie: 0.0mm
Relevés valides  : 168 / 168
Jours agrégés    : 7
```

### Fichier CSV généré (`data/meteo_paris_20260608_142728.csv`)
```
date,temp_min,temp_max,temp_moyenne,precipitation_mm,nb_releves
2026-06-01,15.0,26.3,20.7,0.0,24
2026-06-02,17.3,22.7,19.5,5.6,24
2026-06-03,14.3,20.6,17.4,0.0,24
2026-06-04,15.3,20.5,17.4,2.1,24
2026-06-05,12.2,19.3,15.9,0.0,24
2026-06-06,14.0,21.1,16.9,1.7,24
2026-06-07,12.5,23.3,17.9,0.0,24
```

---

## Utilisation de l'interface Airflow

### Déclencher le DAG manuellement

1. Aller sur **http://localhost:8081**
2. Repérer `tp2_pipeline_etl_simple` dans la liste
3. Activer le **toggle** à gauche (bleu = actif)
4. Cliquer sur l'icône **▶** → **Trigger DAG**

### Lire les couleurs de la Grid

| Couleur | État | Signification |
|---------|------|---------------|
| Vert | `success` | Tâche terminée avec succès |
| Vert clair (bordure) | `running` | En cours d'exécution |
| Orange | `up_for_retry` | Échec → retry automatique en attente |
| Rouge | `failed` | Tous les retries épuisés |
| Blanc / vide | `None` | Pas encore planifiée |

### Consulter les logs d'une tâche

1. Onglet **Graph** → cliquer sur une boîte de tâche
2. Popup → cliquer sur **Log**
3. Les `print()` du code Python apparaissent comme lignes `INFO`

### Vérifier les données échangées entre tâches

- Onglet **XCom** → voir les valeurs transmises entre `extraire_donnees` → `transformer_donnees` → `charger_donnees`

---

## Concepts clés retenus

| Concept | Explication |
|---------|-------------|
| **DAG** | Graphe de tâches ordonnées, sans cycle |
| **PythonOperator** | Opérateur qui exécute une fonction Python comme tâche |
| **XCom** | Mécanisme d'échange de données entre tâches d'un même DAG |
| **Task Instance** | Une exécution concrète d'une tâche à une date donnée |
| **DAG Run** | Une exécution complète du DAG (toutes les tâches) |
| **scheduler** | Processus Airflow qui surveille et déclenche les tâches |
| **`>>`** | Opérateur de dépendance : `A >> B` = "B démarre après A" |
| **`retries`** | Nombre de tentatives automatiques en cas d'échec |
| **`schedule_interval=None`** | Le DAG ne se lance que manuellement |
| **`catchup=False`** | Airflow n'exécute pas les runs manqués au démarrage |

---

## Auteur

- **Nom** : Bakayoko Moussa
- **TP** : TP2 — Fin d'après-midi
- **Outil** : Apache Airflow 2.9.1
- **API** : Open-Meteo (open-meteo.com)
- **Date** : Juin 2026
