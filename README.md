# Weather Data Platform — Bakayoko Moussa

[![CI](https://github.com/MousBak/Premier-Dag-Airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/MousBak/Premier-Dag-Airflow/actions/workflows/ci.yml)
[![dbt Validate](https://github.com/MousBak/Premier-Dag-Airflow/actions/workflows/dbt-validate.yml/badge.svg)](https://github.com/MousBak/Premier-Dag-Airflow/actions/workflows/dbt-validate.yml)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-4169E1?logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-C72E49?logo=minio&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?logo=dbt&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-73%20passed-brightgreen)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

> Pipeline de données météo production-ready — orchestration Airflow, architecture Medallion
> (Bronze/Silver/Gold), Dynamic Task Mapping, modèles analytiques dbt, CI/CD GitHub Actions.

---

## Table des matières

1. [Contexte du projet](#1-contexte-du-projet)
2. [Technologies utilisées](#2-technologies-utilisées)
3. [Structure du projet](#3-structure-du-projet)
4. [Quick Start — Docker](#4-quick-start--docker)
5. [DAG Production — weather_data_pipeline](#5-dag-production--weather_data_pipeline)
6. [Tests automatisés](#6-tests-automatisés)
7. [Modèles dbt](#7-modèles-dbt)
8. [CI/CD — GitHub Actions](#8-cicd--github-actions)
9. [Dashboard Grafana](#9-dashboard-grafana)
10. [Concepts clés Airflow](#10-concepts-clés-airflow)
11. [TP2 — Premier DAG Airflow](#11-tp2--premier-dag-airflow)
12. [TP2A — Ingestion multi-villes](#12-tp2a--ingestion-multi-villes)
13. [TP2B — Pipeline complet vers PostgreSQL](#13-tp2b--pipeline-complet-vers-postgresql)
14. [TP3 — Data Lake Medallion](#14-tp3--data-lake-medallion)
15. [Installation locale](#15-installation-locale)
16. [Guide de vérification](#16-guide-de-vérification)
17. [Auteur](#17-auteur)

---

## 1. Contexte du projet

Ce dépôt est parti de travaux pratiques Airflow et a été transformé en **projet portfolio data engineering complet**.

Le pipeline collecte des données météo réelles depuis l'API Open-Meteo pour 4 villes françaises,
les stocke dans un Data Lake (MinIO), les transforme, les charge dans PostgreSQL,
puis produit des modèles analytiques via dbt.

### Progression des TPs → projet professionnel

| Étape | Ce qui a été ajouté |
|-------|---------------------|
| **TP2** | Premier DAG, 3 tâches, API → CSV |
| **TP2A** | 4 villes, séparation extraction/transformation |
| **TP2B** | Chargement PostgreSQL, Variables Airflow, traçabilité |
| **TP3** | Architecture Medallion : Bronze (MinIO) → Silver (MinIO) → Gold (PostgreSQL) |
| **Pro 1** | Structure professionnelle : `dags/common/`, `tests/`, `docker/`, `sql/migrations/`, `Makefile` |
| **Pro 2** | Docker Compose complet : Airflow + PostgreSQL + MinIO + Redis + Flower + Grafana |
| **Pro 3** | 73 tests pytest : unit, integration, DAG integrity |
| **Pro 4** | Dynamic Task Mapping : une tâche Airflow par ville, parallèles |
| **Pro 5** | dbt : staging + 2 marts analytiques + tests de qualité |
| **Pro 6** | GitHub Actions CI/CD : lint + tests + dbt compile à chaque push |

**Source de données** : [Open-Meteo](https://open-meteo.com/) — API météo gratuite, sans clé.

---

## 2. Technologies utilisées

| Technologie | Version | Rôle |
|-------------|---------|------|
| **Apache Airflow** | 2.9.1 | Orchestration : scheduler, UI, XCom, Variables |
| **Python** | 3.12 | Langage des DAGs, transformations, tests |
| **Open-Meteo API** | — | Source météo gratuite, sans clé d'authentification |
| **PostgreSQL** | 14 | Couche Gold : table analytique + suivi d'ingestion |
| **MinIO** | latest | Stockage objet S3-compatible — couches Bronze et Silver |
| **dbt** | 1.x | Transformations SQL analytiques sur la couche Gold |
| **Redis** | 7 | Broker de messages pour CeleryExecutor (parallélisme réel) |
| **Grafana** | 10.4 | Dashboard météo — visualisation temps réel depuis PostgreSQL |
| **Docker / Compose** | — | Stack complète en une commande |
| **boto3** | — | Client Python pour MinIO/S3 |
| **psycopg2** | — | Connecteur Python → PostgreSQL |
| **pytest** | 8+ | 73 tests automatisés (unit + integration + DAG integrity) |
| **GitHub Actions** | — | CI/CD : lint + tests + dbt compile à chaque push |

---

## 3. Structure du projet

```
weather-data-platform/
│
├── .github/workflows/
│   ├── ci.yml                          # Lint + tests à chaque push
│   └── dbt-validate.yml                # dbt compile quand dbt/ change
│
├── dags/
│   ├── common/
│   │   └── config.py                   # Config partagée : get_config(), get_s3_client(), json_to_rows()
│   ├── ingestion/
│   │   ├── weather_pipeline_dag.py     # ★ DAG production (TaskFlow + Dynamic Task Mapping)
│   │   ├── tp2_premier_dag.py          # TP2 — 3 tâches, Paris, CSV
│   │   ├── tp2a_ingestion_meteo.py     # TP2A — 4 villes, séparation E/T
│   │   ├── tp2b_pipeline_postgresql.py # TP2B — PostgreSQL, Variables, suivi
│   │   └── tp3_data_lake.py            # TP3 — Medallion Bronze → Silver → Gold
│   └── transformation/                 # Réservé pour les DAGs dbt
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml             # Déclaration source meteo_journaliere
│   │   │   ├── stg_weather.sql         # Vue : colonnes EN + weather_description
│   │   │   └── schema.yml              # Tests colonnes staging
│   │   └── marts/
│   │       ├── daily_weather.sql       # Table : catégories pluie/vent, dimensions temps
│   │       ├── city_monthly_stats.sql  # Table : agrégats mensuels par ville
│   │       └── schema.yml
│   ├── tests/
│   │   └── assert_temp_max_greater_than_min.sql
│   ├── dbt_project.yml
│   ├── profiles.yml                    # Profils dev + docker
│   └── packages.yml
│
├── tests/
│   ├── unit/
│   │   └── test_transformations.py     # 19 tests — logique pure, sans I/O
│   ├── integration/
│   │   └── test_pipeline.py            # 10 tests — API + MinIO + PG mockés
│   └── dags/
│       └── test_dag_integrity.py       # 44 tests — import, structure, config
│
├── docker/
│   ├── docker-compose.yml              # Stack : Airflow + PG + MinIO + Redis + Flower + Grafana
│   ├── Dockerfile                      # Image Airflow custom
│   ├── init-db.sh                      # Crée meteo_db au démarrage PostgreSQL
│   └── grafana/
│       └── provisioning/
│           ├── datasources/postgres.yml # Auto-configure la connexion PostgreSQL
│           └── dashboards/
│               ├── provider.yml        # Chargement automatique des dashboards
│               └── weather_dashboard.json  # Dashboard météo (7 panels)
│
├── sql/migrations/
│   ├── 001_init_tables.sql             # Création des tables
│   └── 002_add_indexes.sql             # Index de performance
│
├── config/
│   ├── variables.json                  # Template Variables Airflow
│   └── connections.json                # Template Connections Airflow
│
├── scripts/
│   ├── setup.sh                        # Installation en une commande
│   └── create_buckets.py               # Création des buckets MinIO
│
├── docs/
│   ├── architecture.md                 # Schéma Medallion + modèle de données
│   └── adr/001_why_minio.md            # Architecture Decision Record
│
├── .env.example                        # Template credentials
├── Makefile                            # Toutes les commandes du projet
├── pyproject.toml                      # Dépendances Python
└── CHANGELOG.md
```

---

## 4. Quick Start — Docker

Lance la stack complète en **une seule commande** :

```bash
git clone https://github.com/MousBak/Premier-Dag-Airflow.git
cd Premier-Dag-Airflow
make docker-up
```

Cette commande démarre automatiquement :

| Service | URL | Identifiants |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| **Grafana** | **http://localhost:3000** | **admin / admin** |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Flower (workers) | http://localhost:5555 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

Au premier démarrage, `airflow-init` :
- Migre la base de données Airflow
- Crée l'utilisateur admin
- Crée les tables `meteo_journaliere` et `suivi_ingestion`
- Charge toutes les Variables Airflow
- Crée les buckets MinIO `meteo-bronze` et `meteo-silver`

### Commandes Makefile disponibles

```bash
make docker-up      # Démarrer la stack complète
make docker-down    # Arrêter et supprimer les conteneurs
make docker-logs    # Voir les logs en temps réel

make test           # Lancer les 73 tests pytest
make lint           # Vérifier le style du code (flake8)

make dbt-run        # Exécuter les modèles dbt
make dbt-test       # Lancer les tests dbt
make dbt-docs       # Générer et servir la documentation dbt (port 8082)
```

---

## 5. DAG Production — weather_data_pipeline

Le DAG production utilise le **TaskFlow API** et le **Dynamic Task Mapping** d'Airflow 2.3+.

### Architecture du pipeline

```
get_cities
    │
    ├── extract_city[Paris]      ─┐
    ├── extract_city[Lyon]       ─┤  4 tâches en parallèle
    ├── extract_city[Marseille]  ─┤  (une par ville)
    └── extract_city[Bordeaux]   ─┘
              │
    ┌─────────┴──────────┐
    │                    │
    ├── store_bronze[Paris]      ─┐
    ├── store_bronze[Lyon]       ─┤  4 tâches en parallèle
    ├── store_bronze[Marseille]  ─┤  (JSON brut → MinIO)
    └── store_bronze[Bordeaux]   ─┘
              │
         transform_silver        ←  fan-in : collecte les 4 villes → 1 CSV dans MinIO
              │
          load_gold              ←  upsert PostgreSQL (28 lignes)
              │
           dbt_run               ←  dbt run + dbt test (gate de qualité)
              │
         write_audit             ←  traçabilité dans suivi_ingestion
```

### Pourquoi le Dynamic Task Mapping ?

| Avant (TP3) | Après (production) |
|-------------|-------------------|
| 1 tâche boucle sur 4 villes | 4 tâches indépendantes dans l'UI |
| Si Lyon échoue → tout le batch retente | Si Lyon échoue → seul Lyon retente |
| Pas de parallélisme visible | Parallélisme visible ville par ville |
| Ajouter une ville = modifier le code | Ajouter une ville = modifier la Variable `METEO_VILLES` |

### Schedule et retry

```python
schedule_interval = "0 6 * * *"        # tous les jours à 06:00
retries = 2
retry_exponential_backoff = True        # évite de saturer l'API en cas d'erreur
```

### Lancer le DAG production en local

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test weather_data_pipeline
```

---

## 6. Tests automatisés

**73 tests** organisés en 3 suites. Aucun service externe requis — tout est mocké.

```bash
make test
# ou
AIRFLOW_HOME=$(pwd) airflow_venv/bin/pytest tests/ -v
```

### Suite 1 — Unit tests (`tests/unit/`) — 19 tests

Teste la logique de transformation pure, sans appel réseau ni base de données.

| Test | Ce qui est vérifié |
|------|-------------------|
| `test_returns_correct_number_of_rows` | json_to_rows produit N lignes pour N jours |
| `test_temp_max_greater_than_min` | Invariant : temp_max > temp_min sur chaque ligne |
| `test_no_extra_columns` | Aucune colonne inattendue dans la sortie |
| `test_empty_days_returns_empty_list` | Comportement sur données vides |
| `test_forecast_days_is_zero` | L'API n'est jamais appelée avec des jours futurs |
| ... | 14 autres tests |

### Suite 2 — Integration tests (`tests/integration/`) — 10 tests

Teste le pipeline complet avec API, MinIO et PostgreSQL **mockés**.

| Test | Ce qui est vérifié |
|------|-------------------|
| `test_all_cities_produce_rows` | Chaque ville produit des lignes en sortie |
| `test_bronze_keys_follow_naming_convention` | Chemin S3 = `YYYY-MM-DD/<ville>.json` |
| `test_bronze_stores_raw_json_unchanged` | Le JSON brut n'est pas modifié avant stockage |
| `test_executemany_called_once` | PostgreSQL reçoit une seule requête batch |
| `test_commit_is_called` | La transaction est bien commitée |
| ... | 5 autres tests |

### Suite 3 — DAG integrity (`tests/dags/`) — 44 tests

Vérifie que chaque DAG respecte les standards de qualité.

| Test | Ce qui est vérifié |
|------|-------------------|
| `test_no_import_errors` | Tous les DAGs s'importent sans exception |
| `test_dag_is_present` | Chaque DAG attendu est dans le DagBag |
| `test_catchup_is_disabled` | `catchup=False` sur tous les DAGs |
| `test_all_tasks_have_retries` | Chaque tâche a au moins 1 retry |
| `test_production_dag_has_schedule` | Le DAG production a un vrai cron |
| `test_production_dag_has_exponential_backoff` | Backoff exponentiel activé |
| ... | 38 autres tests |

---

## 7. Modèles dbt

dbt transforme la couche Gold brute (`meteo_journaliere`) en modèles analytiques prêts à l'emploi.

### Architecture dbt

```
meteo_journaliere (PostgreSQL Gold)
        │
        ▼
   stg_weather          ← Vue : colonnes renommées EN, weather_description, temp_range_c
        │
        ├──→  daily_weather         ← Table : catégories pluie/vent, dimensions temps
        └──→  city_monthly_stats    ← Table : agrégats mensuels par ville
```

### Modèles

**`stg_weather`** (Vue)
- Renomme les colonnes françaises en anglais (`ville` → `city`, `date` → `observation_date`…)
- Ajoute `weather_description` depuis le code WMO (`0` → "Clear sky", `61` → "Rain"…)
- Calcule `temp_range_c` = `temp_max_c` - `temp_min_c`

**`daily_weather`** (Table)
- Hérite de `stg_weather`
- Ajoute `precipitation_category` (Dry / Light rain / Moderate / Heavy)
- Ajoute `wind_category` (Calm / Moderate / Strong / Storm)
- Ajoute les dimensions temps : `week_start`, `month_start`, `day_of_week`

**`city_monthly_stats`** (Table)
- Hérite de `daily_weather`
- Une ligne = une ville × un mois calendaire
- Colonnes : `avg_temp_c`, `max_temp_c`, `total_precipitation_mm`, `rainy_days`, `rainy_days_pct`, `dominant_weather`

### Tests dbt

```yaml
# Exemples de tests déclarés dans schema.yml
- city: not_null, accepted_values [Paris, Lyon, Marseille, Bordeaux]
- observation_date: not_null
- precipitation_category: accepted_values [Dry, Light rain, Moderate rain, Heavy rain]
```

```sql
-- Test custom : temp_max doit toujours être > temp_min
-- dbt/tests/assert_temp_max_greater_than_min.sql
SELECT city, observation_date, temp_max_c, temp_min_c
FROM {{ ref('stg_weather') }}
WHERE temp_max_c < temp_min_c   -- dbt attend 0 ligne ici
```

### Lancer dbt

```bash
make dbt-run    # Exécute les modèles
make dbt-test   # Lance les tests de qualité
make dbt-docs   # Documentation interactive (http://localhost:8082)
```

---

## 8. CI/CD — GitHub Actions

Deux workflows se déclenchent automatiquement à chaque push.

### `ci.yml` — Pipeline CI complète

Déclenché sur chaque push et pull request vers `main`.

```
push / PR
    │
    ├── Job 1: lint (flake8)          ~30s  — bloque rapidement sur les erreurs de style
    ├── Job 2: unit + integration     ~45s  — sans Airflow, rapide
    └── Job 3: DAG integrity          ~5min — avec Airflow 2.9.1, résultat caché

    └── Job 4: all-tests-pass         — check global requis pour merger
```

### `dbt-validate.yml` — Validation dbt

Déclenché uniquement quand des fichiers dans `dbt/` changent.

```
dbt compile   — vérifie la syntaxe SQL et les ref() sans connexion DB
```

### Voir les résultats

Onglet **Actions** sur GitHub → chaque commit affiche le statut détaillé de chaque job.

---

## 9. Dashboard Grafana

Grafana visualise les données météo **en temps réel** depuis PostgreSQL.
Le dashboard est **provisionné automatiquement** au démarrage de `make docker-up` — aucune configuration manuelle.

### Accès

```
http://localhost:3000   (admin / admin)
```

Le dashboard "Weather Data Platform" est chargé par défaut à l'ouverture.

### Panels du dashboard

| Panel | Type | Ce qui est affiché |
|-------|------|--------------------|
| Enregistrements en base | Stat | Nombre total de relevés dans PostgreSQL |
| Dernière ingestion | Stat | Heure du dernier run Airflow (temps relatif) |
| Temp max du dernier relevé | Stat | Température maximale — couleur selon seuil (bleu < vert < jaune < rouge) |
| Température moyenne par ville | Time series | Courbe des 4 villes sur 7 jours, avec min/max/moyenne |
| Précipitations journalières | Bar chart | Barres groupées par ville et par jour |
| Vitesse max du vent | Time series | Vent max km/h par ville |
| Suivi des ingestions | Table | 15 derniers runs Airflow — statut coloré (vert/rouge/jaune) |

### Actualisation automatique

Le dashboard se rafraîchit toutes les **5 minutes** — les données du pipeline de 06:00 apparaissent
automatiquement sans aucune action.

---

## 10. Concepts clés Airflow

### Qu'est-ce qu'un DAG ?

Un **DAG** (Directed Acyclic Graph — Graphe Orienté Acyclique) représente un workflow :
un ensemble de tâches à exécuter dans un ordre précis, sans boucle possible.

```python
tache_a >> tache_b >> tache_c  # b démarre après a, c démarre après b
```

### Glossaire

| Concept | Définition |
|---------|-----------|
| **DAG** | Graphe de tâches ordonnées, sans cycle |
| **PythonOperator** | Exécute une fonction Python comme tâche |
| **TaskFlow API** | Syntaxe moderne avec `@task` — XCom géré automatiquement |
| **Dynamic Task Mapping** | Crée N instances d'une tâche depuis une liste (`.expand()`) |
| **XCom** | Échange de données entre tâches via la base de données Airflow |
| **Variable Airflow** | Valeur configurable depuis l'UI sans toucher au code |
| **CeleryExecutor** | Exécuteur distribué — plusieurs workers en parallèle |
| **`catchup=False`** | Airflow ne rattrape pas les runs manqués au démarrage |
| **`ON CONFLICT DO UPDATE`** | Upsert SQL : insère ou met à jour si la ligne existe |

### Couleurs des tâches dans l'UI

| Couleur | État | Signification |
|---------|------|---------------|
| Vert foncé | `success` | Terminée avec succès |
| Vert clair | `running` | En cours d'exécution |
| Orange | `up_for_retry` | Échec → retry en attente |
| Rouge | `failed` | Tous les retries épuisés |
| Blanc / gris | `none` | Pas encore planifiée |

---

## 11. TP2 — Premier DAG Airflow

### Objectif

Créer un premier DAG avec **3 tâches**, des dépendances explicites et une source réelle.

### Pipeline

```
extraire_donnees  ──→  transformer_donnees  ──→  charger_donnees
```

| Tâche | Rôle |
|-------|------|
| `extraire_donnees` | Appelle Open-Meteo → 168 relevés horaires de Paris sur 7 jours |
| `transformer_donnees` | Agrège en 7 résumés journaliers (min/max/moyenne/pluie) |
| `charger_donnees` | Exporte dans un fichier CSV horodaté |

### Résultat

```
2026-06-01 | min: 15.0°C | max: 26.3°C | moy: 20.7°C | pluie: 0.0mm
2026-06-02 | min: 17.3°C | max: 22.7°C | moy: 19.5°C | pluie: 5.6mm
...
```

```bash
airflow dags test tp2_pipeline_etl_simple
```

---

## 12. TP2A — Ingestion multi-villes

### Objectif

4 villes françaises, **séparation stricte** extraction / transformation, champs justifiés.

### Pipeline

```
extraire_donnees_brutes  ──→  preparer_donnees_pipeline  ──→  charger_table_cible
```

### Villes couvertes

| Ville | Latitude | Longitude |
|-------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Lyon | 45.7640 | 4.8357 |
| Marseille | 43.2965 | 5.3698 |
| Bordeaux | 44.8378 | -0.5792 |

### Champs retenus

| Champ | Source API | Justification |
|-------|-----------|---------------|
| `ville` | *(ajouté)* | Clé géographique |
| `date` | `time` | Dimension temporelle obligatoire |
| `temp_max_c` | `temperature_2m_max` | Indicateur thermique haut |
| `temp_min_c` | `temperature_2m_min` | Indicateur thermique bas |
| `temp_moyenne_c` | `temperature_2m_mean` | Synthèse journalière |
| `precipitation_mm` | `precipitation_sum` | Volume de pluie |
| `vent_max_kmh` | `windspeed_10m_max` | Conditions extrêmes |
| `code_meteo` | `weathercode` | Code WMO (soleil/pluie/neige…) |

### Champs supprimés

| Champ | Raison |
|-------|--------|
| `latitude` / `longitude` | Redondant avec le nom de ville |
| `elevation` | Inutile pour l'analyse quotidienne |
| `timezone` / `utc_offset` | Fixé à Europe/Paris en amont |
| `generationtime_ms` | Métadonnée interne de l'API |

**Total : 28 lignes** (4 villes × 7 jours)

```bash
airflow dags test tp2a_ingestion_meteo
```

---

## 13. TP2B — Pipeline complet vers PostgreSQL

### Objectif

Chargement PostgreSQL, traçabilité complète, DAG **100% paramétrable** via Variables Airflow.

### Pipeline

```
extraire_donnees_brutes ──→ transformer_donnees ──→ charger_postgresql ──→ ecrire_suivi_ingestion
```

### Variables Airflow

| Variable | Valeur par défaut | Description |
|----------|------------------|-------------|
| `METEO_VILLES` | JSON des 4 villes | Liste des villes (modifiable sans toucher au code) |
| `METEO_PAST_DAYS` | `7` | Jours d'historique |
| `METEO_DB_HOST` | `localhost` | Hôte PostgreSQL |
| `METEO_DB_NAME` | `meteo_db` | Nom de la base |
| `METEO_DB_USER` | `postgres` | Utilisateur |
| `METEO_DB_PASSWORD` | `postgres` | Mot de passe |

### Schéma PostgreSQL (`sql/migrations/001_init_tables.sql`)

```sql
CREATE TABLE IF NOT EXISTS meteo_journaliere (
    id               SERIAL PRIMARY KEY,
    ville            VARCHAR(100)  NOT NULL,
    date             DATE          NOT NULL,
    temp_max_c       NUMERIC(5,1),
    temp_min_c       NUMERIC(5,1),
    temp_moyenne_c   NUMERIC(5,1),
    precipitation_mm NUMERIC(6,1),
    vent_max_kmh     NUMERIC(6,1),
    code_meteo       INTEGER,
    insere_le        TIMESTAMP     DEFAULT NOW(),
    UNIQUE (ville, date)
);

CREATE TABLE IF NOT EXISTS suivi_ingestion (
    id               SERIAL PRIMARY KEY,
    dag_id           VARCHAR(100)  NOT NULL,
    run_id           VARCHAR(200)  NOT NULL,
    ville            VARCHAR(100),
    nb_lignes        INTEGER       DEFAULT 0,
    statut           VARCHAR(20)   NOT NULL,
    message          TEXT,
    debut_ingestion  TIMESTAMP     NOT NULL,
    fin_ingestion    TIMESTAMP     DEFAULT NOW()
);
```

### Preuve de chargement

```
 ville     | date       | temp_max_c | temp_min_c
-----------+------------+------------+------------
 Bordeaux  | 2026-06-02 |       21.8 |       17.2
 Lyon      | 2026-06-02 |       24.2 |       17.6
 Marseille | 2026-06-02 |       25.7 |       21.7
 Paris     | 2026-06-02 |       22.7 |       17.3
(28 rows)
```

```bash
airflow dags test tp2b_pipeline_postgresql
```

---

## 14. TP3 — Data Lake Medallion

### Architecture

```
API Open-Meteo
      │
      ▼
  BRONZE — MinIO meteo-bronze/YYYY-MM-DD/<ville>.json  ← JSON brut, jamais modifié
      │
      ▼
  SILVER — MinIO meteo-silver/YYYY-MM-DD/meteo_villes.csv  ← CSV transformé
      │
      ▼
  GOLD   — PostgreSQL meteo_journaliere  ← prêt pour l'analyse
      │
      ▼
  suivi_ingestion  ← traçabilité complète
```

### Tâches

| Tâche | Couche | Rôle |
|-------|--------|------|
| `extraire_api` | — | Appelle l'API pour les 4 villes |
| `stocker_bronze` | BRONZE | JSON brut → MinIO `meteo-bronze` |
| `transformer_silver` | SILVER | CSV structuré → MinIO `meteo-silver` |
| `charger_gold` | GOLD | Upsert → PostgreSQL |
| `ecrire_suivi` | — | Audit : chemins Bronze + Silver + nb lignes |

### Preuve d'exécution

```
meteo-bronze/2026-06-09/paris.json       1303 octets
meteo-bronze/2026-06-09/lyon.json        1308 octets
meteo-bronze/2026-06-09/marseille.json   1298 octets
meteo-bronze/2026-06-09/bordeaux.json    1302 octets
meteo-silver/2026-06-09/meteo_villes.csv 1375 octets  ← 28 lignes
Gold PostgreSQL : 28 lignes (4 villes × 7 jours)
```

```bash
airflow dags test tp3_data_lake
```

---

## 15. Installation locale

### Prérequis

- Python 3.12+, Docker, PostgreSQL 14+

### Option A — Docker (recommandé)

```bash
git clone https://github.com/MousBak/Premier-Dag-Airflow.git
cd Premier-Dag-Airflow
make docker-up
```

Tout est configuré automatiquement. Accès sur http://localhost:8080.

### Option B — Installation locale (venv)

```bash
git clone https://github.com/MousBak/Premier-Dag-Airflow.git
cd Premier-Dag-Airflow

# Installation complète en une commande
make setup

# Démarrer Airflow (port 8081)
make run

# MinIO (Docker requis)
make minio
make buckets
```

### Charger les Variables Airflow

```bash
make variables
```

---

## 16. Guide de vérification

```bash
# Lancer tous les tests
make test

# Tester chaque DAG individuellement
export AIRFLOW_HOME=$(pwd) && source airflow_venv/bin/activate
airflow dags test tp2_pipeline_etl_simple
airflow dags test tp2a_ingestion_meteo
airflow dags test tp2b_pipeline_postgresql
airflow dags test tp3_data_lake
airflow dags test weather_data_pipeline

# Vérifier PostgreSQL
psql -U postgres -d meteo_db -c "SELECT ville, COUNT(*) FROM meteo_journaliere GROUP BY ville;"
psql -U postgres -d meteo_db -c "SELECT dag_id, nb_lignes, statut FROM suivi_ingestion ORDER BY fin_ingestion DESC;"

# Vérifier MinIO
python3 scripts/create_buckets.py   # liste les buckets existants

# Lancer dbt
make dbt-run && make dbt-test
```

---

## 17. Auteur

| | |
|--|--|
| **Nom** | Bakayoko Moussa |
| **Projet** | Weather Data Platform — pipeline data engineering complet |
| **Stack** | Airflow 2.9.1 · Python 3.12 · PostgreSQL 14 · MinIO · dbt · Docker · GitHub Actions |
| **API** | [Open-Meteo](https://open-meteo.com/) — données météo gratuites, sans clé |
| **Dépôt** | [github.com/MousBak/Premier-Dag-Airflow](https://github.com/MousBak/Premier-Dag-Airflow) |
| **Date** | Juin 2026 |
