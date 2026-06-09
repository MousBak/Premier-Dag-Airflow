# Travaux Pratiques Airflow — Bakayoko Moussa

> **Outil** : Apache Airflow 2.9.1 · **API** : Open-Meteo · **Langage** : Python 3.12

---

## Qu'est-ce qu'un DAG ?

Un **DAG** (Directed Acyclic Graph) est le concept central d'Airflow.
C'est un graphe orienté sans cycle qui représente un workflow : un ensemble de tâches à exécuter dans un ordre précis.

- **Dirigé** → chaque tâche a une direction (A doit finir avant B)
- **Acyclique** → pas de boucle possible (A ne peut pas dépendre de lui-même)
- **Graphe** → les tâches sont des nœuds reliés par des flèches

---

## Structure du projet

```
Airflow/
├── dags/
│   ├── tp2_premier_dag.py        # TP2  — Pipeline ETL Paris (1 ville, données horaires)
│   └── tp2a_ingestion_meteo.py   # TP2A — Ingestion multi-villes (4 villes, données journalières)
├── data/
│   ├── meteo_paris_*.csv         # Sorties TP2
│   └── meteo_villes_*.csv        # Sorties TP2A
├── logs/                         # Logs générés automatiquement par Airflow
├── plugins/                      # Plugins personnalisés (vide)
├── airflow_venv/                 # Environnement Python virtuel
├── airflow.db                    # Base de données SQLite d'Airflow
├── docker-compose.yml            # Configuration Docker alternative
└── README.md                     # Ce fichier
```

---

---

# TP2 — Fin d'après-midi : Premier DAG Airflow

## Objectif

Créer un premier DAG simple avec 3 tâches, des dépendances explicites, et une source de données réelle.

## DAG : `tp2_pipeline_etl_simple`

```
extraire_donnees  ──→  transformer_donnees  ──→  charger_donnees
```

| Tâche | Rôle |
|-------|------|
| `extraire_donnees` | Appelle l'API Open-Meteo, récupère **168 relevés horaires** de Paris sur 7 jours |
| `transformer_donnees` | Agrège les relevés horaires en **7 résumés journaliers** (min/max/moyenne/pluie) |
| `charger_donnees` | Exporte les statistiques dans un **fichier CSV horodaté** |

## Dépendances explicites

```python
tache_extraction >> tache_transformation >> tache_chargement
```

Si une tâche échoue, les suivantes sont automatiquement bloquées.

## Résultat d'exécution

### Logs de `extraire_donnees`
```
=== EXTRACTION — Météo de Paris ===
Statut HTTP      : 200
Relevés reçus    : 168 points horaires
Période couverte : 2026-06-01T00:00  →  2026-06-07T23:00
Exemple          : 2026-06-01T00:00 → 18.9°C
```

### Logs de `transformer_donnees`
```
2026-06-01 | min: 15.0°C | max: 26.3°C | moy: 20.7°C | pluie: 0.0mm
2026-06-02 | min: 17.3°C | max: 22.7°C | moy: 19.5°C | pluie: 5.6mm
2026-06-03 | min: 14.3°C | max: 20.6°C | moy: 17.4°C | pluie: 0.0mm
2026-06-04 | min: 15.3°C | max: 20.5°C | moy: 17.4°C | pluie: 2.1mm
2026-06-05 | min: 12.2°C | max: 19.3°C | moy: 15.9°C | pluie: 0.0mm
2026-06-06 | min: 14.0°C | max: 21.1°C | moy: 16.9°C | pluie: 1.7mm
2026-06-07 | min: 12.5°C | max: 23.3°C | moy: 17.9°C | pluie: 0.0mm
Relevés valides  : 168 / 168
```

### Fichier CSV généré (`data/meteo_paris_20260608_142728.csv`)
```
date,temp_min,temp_max,temp_moyenne,precipitation_mm,nb_releves
2026-06-01,15.0,26.3,20.7,0.0,24
2026-06-02,17.3,22.7,19.5,5.6,24
...
```

## Lancer ce DAG

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2_pipeline_etl_simple
```

---

---

# TP2A — Fin de matinée : Préparer une ingestion API météo

## Objectif

Récupérer des données Open-Meteo pour **plusieurs villes**, en séparant strictement la logique de récupération de la logique de transformation, et en ne gardant que les champs utiles justifiés.

## DAG : `tp2a_ingestion_meteo`

```
extraire_donnees_brutes  ──→  preparer_donnees_pipeline  ──→  charger_table_cible
```

| Tâche | Rôle |
|-------|------|
| `extraire_donnees_brutes` | Appelle l'API pour **4 villes**, sauvegarde le JSON **tel quel** — aucune transformation |
| `preparer_donnees_pipeline` | Sélectionne les champs utiles, renomme, structure en vue de la table cible |
| `charger_table_cible` | Exporte les **28 lignes** (4 villes × 7 jours) dans un CSV structuré |

## Villes couvertes

| Ville | Latitude | Longitude |
|-------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Lyon | 45.7640 | 4.8357 |
| Marseille | 43.2965 | 5.3698 |
| Bordeaux | 44.8378 | -0.5792 |

## Séparation récupération / transformation

La consigne imposait de **séparer** ces deux logiques. Voici comment :

**Tâche 1 — extraction brute** : appel API uniquement, JSON stocké sans modification via XCom
```python
# On garde TOUT ce que l'API renvoie, sans rien toucher
resultats_bruts[ville["nom"]] = donnees_json
context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)
```

**Tâche 2 — préparation** : aucun appel réseau, seulement la sélection et le renommage
```python
releves = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees_brutes")
# On construit la structure cible ici
```

## Champs retenus — justification

| Champ dans la table | Champ API d'origine | Pourquoi retenu |
|---------------------|--------------------|--------------------|
| `ville` | *(ajouté)* | Clé géographique indispensable |
| `date` | `time` | Dimension temporelle obligatoire |
| `temp_max_c` | `temperature_2m_max` | Indicateur thermique journalier haut |
| `temp_min_c` | `temperature_2m_min` | Indicateur thermique journalier bas |
| `temp_moyenne_c` | `temperature_2m_mean` | Synthèse utile pour les moyennes |
| `precipitation_mm` | `precipitation_sum` | Volume de pluie (alertes, irrigation) |
| `vent_max_kmh` | `windspeed_10m_max` | Conditions extrêmes, sécurité |
| `code_meteo` | `weathercode` | Code WMO pour catégoriser le temps |

## Champs supprimés — justification

| Champ supprimé | Raison |
|----------------|--------|
| `latitude` / `longitude` | Redondant avec le nom de ville |
| `elevation` | Inutile pour l'analyse météo quotidienne |
| `timezone` / `utc_offset` | Fixé à Europe/Paris en amont |
| `generationtime_ms` | Métadonnée technique de l'API, sans valeur métier |

## Aperçu des données préparées

```
VILLE        DATE            MAX    MIN    MOY   PLUIE     VENT  CODE
-----------------------------------------------------------------
Paris        2026-06-02    22.7°C  17.3°C  19.5°C    5.6mm   19.9km/h    96
Paris        2026-06-03    20.6°C  14.3°C  17.4°C    0.0mm   22.2km/h     3
Lyon         2026-06-02    24.2°C  17.6°C  20.4°C   32.3mm   16.8km/h    99
Marseille    2026-06-02    25.7°C  21.7°C  23.6°C    0.0mm   19.4km/h    51
Bordeaux     2026-06-02    21.8°C  17.2°C  19.0°C    0.3mm   21.5km/h    80
```

**Total : 28 lignes** (4 villes × 7 jours) — fichier : `data/meteo_villes_20260609_095418.csv`

## Lancer ce DAG

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2a_ingestion_meteo
```

---

---

# Installation complète

## Prérequis

- Python 3.12+
- macOS / Linux
- Connexion internet (appels API Open-Meteo)

## Étapes

```bash
# 1. Environnement virtuel
python3 -m venv airflow_venv
source airflow_venv/bin/activate

# 2. Installer Airflow + dépendances
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"
pip install "apache-airflow==2.9.1" --constraint "${CONSTRAINT_URL}"
pip install certifi

# 3. Initialiser la base de données
export AIRFLOW_HOME=$(pwd)
airflow db migrate

# 4. Créer l'utilisateur admin
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com

# 5. Lancer le scheduler (Terminal 1)
airflow scheduler

# 6. Lancer le webserver (Terminal 2)
airflow webserver --port 8081
```

Interface web : **http://localhost:8081** — identifiants : `admin` / `admin`

---

# Concepts clés Airflow

| Concept | Explication |
|---------|-------------|
| **DAG** | Graphe de tâches ordonnées, sans cycle |
| **PythonOperator** | Exécute une fonction Python comme tâche |
| **XCom** | Échange de données entre tâches d'un même DAG |
| **Task Instance** | Une exécution concrète d'une tâche à une date donnée |
| **DAG Run** | Une exécution complète du DAG |
| **scheduler** | Processus qui surveille et déclenche les tâches |
| **`>>`** | Dépendance : `A >> B` = B démarre après A |
| **`retries`** | Tentatives automatiques en cas d'échec |
| **`schedule_interval=None`** | Déclenchement manuel uniquement |
| **`catchup=False`** | Ne rattrape pas les runs manqués au démarrage |

## Couleurs de la Grid Airflow

| Couleur | État | Signification |
|---------|------|---------------|
| Vert | `success` | Tâche terminée avec succès |
| Vert clair (bordure) | `running` | En cours d'exécution |
| Orange | `up_for_retry` | Échec → retry automatique en attente |
| Rouge | `failed` | Tous les retries épuisés |
| Blanc / vide | `None` | Pas encore planifiée |

---

## Auteur

- **Nom** : Bakayoko Moussa
- **TP2** : Fin d'après-midi — Premier DAG Airflow
- **TP2A** : Fin de matinée — Ingestion API météo multi-villes
- **Outil** : Apache Airflow 2.9.1
- **API** : Open-Meteo (open-meteo.com)
- **Date** : Juin 2026
