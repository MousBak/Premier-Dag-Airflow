# Travaux Pratiques Airflow — Bakayoko Moussa

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-4169E1?logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-Data%20Lake-C72E49?logo=minio&logoColor=white)
![Status](https://img.shields.io/badge/Tous%20les%20TPs-✅%20success-brightgreen)

> Pipeline ETL orchestré avec Apache Airflow, données météo temps réel via l'API Open-Meteo,
> stockage objet MinIO et base de données PostgreSQL.

---

## Table des matières

1. [Contexte du projet](#1-contexte-du-projet)
2. [Technologies utilisées](#2-technologies-utilisées)
3. [Structure du projet](#3-structure-du-projet)
4. [Concepts clés Airflow](#4-concepts-clés-airflow)
5. [TP2 — Premier DAG Airflow](#5-tp2--premier-dag-airflow)
6. [TP2A — Ingestion multi-villes](#6-tp2a--ingestion-multi-villes)
7. [TP2B — Pipeline complet vers PostgreSQL](#7-tp2b--pipeline-complet-vers-postgresql)
8. [TP3 — Data Lake Medallion](#8-tp3--data-lake-medallion)
9. [Installation complète](#9-installation-complète)
10. [Guide de vérification](#10-guide-de-vérification)
11. [Auteur](#11-auteur)

---

## 1. Contexte du projet

Ce dépôt regroupe l'ensemble des travaux pratiques Airflow réalisés dans le cadre du cours
d'orchestration de pipelines de données.

L'objectif global est de construire, étape par étape, un **pipeline ETL complet et industrialisable** :

```
API publique → Extraction → Transformation → Stockage objet → Base de données relationnelle
```

Chaque TP ajoute un niveau de complexité supplémentaire :

| TP | Nouveauté introduite | Destination finale |
|----|---------------------|--------------------|
| **TP2** | Premier DAG, 3 tâches, dépendances explicites | Fichier CSV local |
| **TP2A** | Multi-villes, séparation extraction/transformation | Fichier CSV structuré |
| **TP2B** | Chargement PostgreSQL, traçabilité, paramétrage via Variables | Base de données |
| **TP3** | Architecture Data Lake Medallion, stockage objet MinIO | MinIO + PostgreSQL |

**Source de données** : [Open-Meteo](https://open-meteo.com/) — API météo gratuite, sans clé,
fournissant des données journalières et horaires pour n'importe quel point GPS.

---

## 2. Technologies utilisées

| Technologie | Version | Rôle |
|-------------|---------|------|
| **Apache Airflow** | 2.9.1 | Orchestrateur de pipelines (scheduler, UI, XCom) |
| **Python** | 3.12 | Langage des DAGs et des tâches |
| **Open-Meteo API** | — | Source de données météo (gratuite, pas de clé) |
| **PostgreSQL** | 14 | Base de données relationnelle (couche Gold) |
| **MinIO** | latest | Stockage objet S3-compatible (couches Bronze et Silver) |
| **boto3** | — | Client Python pour MinIO/S3 |
| **psycopg2** | — | Connecteur Python pour PostgreSQL |
| **certifi** | — | Certificats SSL pour les appels HTTPS sur macOS |
| **Docker** | — | Conteneur MinIO |

---

## 3. Structure du projet

```
Airflow/
│
├── dags/                               # Tous les DAGs Airflow
│   ├── tp2_premier_dag.py              # TP2  — 3 tâches, Paris, CSV
│   ├── tp2a_ingestion_meteo.py         # TP2A — 4 villes, séparation E/T, CSV
│   ├── tp2b_pipeline_postgresql.py     # TP2B — PostgreSQL, Variables, suivi
│   └── tp3_data_lake.py               # TP3  — Medallion : Bronze → Silver → Gold
│
├── sql/
│   └── init_meteo_db.sql              # Script SQL de création des tables PostgreSQL
│
├── data/                              # Fichiers CSV générés par TP2 et TP2A
│   ├── meteo_paris_*.csv              # Sorties TP2
│   └── meteo_villes_*.csv            # Sorties TP2A
│
├── logs/                              # Logs Airflow (générés automatiquement)
├── plugins/                           # Plugins personnalisés (vide pour ces TPs)
├── airflow_venv/                      # Environnement Python virtuel (non versionné)
└── README.md                          # Ce fichier
```

---

## 4. Concepts clés Airflow

### Qu'est-ce qu'un DAG ?

Un **DAG** (Directed Acyclic Graph — Graphe Orienté Acyclique) est le concept central d'Airflow.
Il représente un workflow : un ensemble de tâches à exécuter dans un ordre précis.

- **Dirigé** → chaque tâche a une direction (A doit finir avant B)
- **Acyclique** → pas de boucle (A ne peut pas dépendre de lui-même)
- **Graphe** → les tâches sont des nœuds reliés par des dépendances

Les dépendances s'écrivent avec l'opérateur `>>` en Python :
```python
tache_a >> tache_b >> tache_c  # b démarre après a, c démarre après b
```

### Glossaire des concepts utilisés dans ces TPs

| Concept | Définition |
|---------|-----------|
| **DAG** | Graphe de tâches ordonnées, sans cycle |
| **PythonOperator** | Exécute une fonction Python comme tâche Airflow |
| **XCom** | Mécanisme d'échange de données entre tâches (via la base de données Airflow) |
| **DAG Run** | Une exécution complète d'un DAG à un instant donné |
| **Task Instance** | Une exécution concrète d'une tâche dans un DAG Run |
| **scheduler** | Processus Airflow qui surveille et déclenche les tâches |
| **Variable Airflow** | Valeur configurable depuis l'UI (Admin → Variables), sans toucher au code |
| **`schedule_interval=None`** | Le DAG ne se déclenche que manuellement |
| **`catchup=False`** | Airflow ne rattrape pas les exécutions manquées au démarrage |
| **`retries`** | Nombre de tentatives automatiques si une tâche échoue |
| **`ON CONFLICT DO UPDATE`** | Upsert SQL : insère une ligne ou la met à jour si elle existe déjà |

### Couleurs des tâches dans l'interface Airflow

| Couleur | État | Signification |
|---------|------|---------------|
| Vert foncé | `success` | Tâche terminée avec succès |
| Jaune / vert clair | `running` | En cours d'exécution |
| Orange | `up_for_retry` | Echec → retry automatique en attente |
| Rouge | `failed` | Tous les retries épuisés, tâche en erreur |
| Blanc / gris | `none` | Pas encore planifiée |

---

## 5. TP2 — Premier DAG Airflow

### Objectif

Créer un premier DAG simple avec **3 tâches**, des **dépendances explicites**, et une **source de données réelle**.
Utiliser l'API Open-Meteo pour récupérer les données météo de Paris sur 7 jours.

### Pipeline

```
extraire_donnees  ──→  transformer_donnees  ──→  charger_donnees
```

### Description des tâches

| Tâche | Rôle |
|-------|------|
| `extraire_donnees` | Appelle Open-Meteo, récupère **168 relevés horaires** de Paris sur 7 jours, pousse le JSON brut via XCom |
| `transformer_donnees` | Récupère le JSON depuis XCom, agrège les relevés horaires en **7 résumés journaliers** (min/max/moyenne/pluie) |
| `charger_donnees` | Récupère les données transformées depuis XCom, exporte dans un **fichier CSV horodaté** |

### Pourquoi XCom ?

Les tâches Airflow s'exécutent dans des processus séparés. Pour qu'une tâche transmette des données
à la suivante, on utilise **XCom** (Cross-Communication) : la tâche productrice appelle
`xcom_push()` et la tâche consommatrice appelle `xcom_pull()`. Les données transitent par
la base de données d'Airflow.

```python
# Tâche 1 — on envoie les données
context["ti"].xcom_push(key="donnees_brutes", value=donnees)

# Tâche 2 — on les récupère
donnees = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees")
```

### Résultat d'exécution

**Logs de `extraire_donnees` :**
```
=== EXTRACTION — Météo de Paris ===
Statut HTTP      : 200
Relevés reçus    : 168 points horaires
Période couverte : 2026-06-01T00:00  →  2026-06-07T23:00
```

**Logs de `transformer_donnees` :**
```
2026-06-01 | min: 15.0°C | max: 26.3°C | moy: 20.7°C | pluie: 0.0mm
2026-06-02 | min: 17.3°C | max: 22.7°C | moy: 19.5°C | pluie: 5.6mm
2026-06-03 | min: 14.3°C | max: 20.6°C | moy: 17.4°C | pluie: 0.0mm
2026-06-04 | min: 15.3°C | max: 20.5°C | moy: 17.4°C | pluie: 2.1mm
2026-06-05 | min: 12.2°C | max: 19.3°C | moy: 15.9°C | pluie: 0.0mm
2026-06-06 | min: 14.0°C | max: 21.1°C | moy: 16.9°C | pluie: 1.7mm
2026-06-07 | min: 12.5°C | max: 23.3°C | moy: 17.9°C | pluie: 0.0mm
```

**Fichier CSV généré** (`data/meteo_paris_20260609_153120.csv`) :
```
date,temp_min,temp_max,temp_moyenne,precipitation_mm,nb_releves
2026-06-01,15.0,26.3,20.7,0.0,24
2026-06-02,17.3,22.7,19.5,5.6,24
...
```

### Commande de test

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2_pipeline_etl_simple
```

---

## 6. TP2A — Ingestion multi-villes

### Objectif

Étendre le pipeline à **4 villes françaises**, en imposant une **séparation stricte** entre la logique
d'extraction (appel réseau) et la logique de transformation (sélection, renommage).
Justifier les champs conservés et ceux supprimés.

### Pipeline

```
extraire_donnees_brutes  ──→  preparer_donnees_pipeline  ──→  charger_table_cible
```

### Description des tâches

| Tâche | Rôle |
|-------|------|
| `extraire_donnees_brutes` | Appelle l'API pour **4 villes**, stocke le JSON **tel quel** — aucune transformation, aucun filtre |
| `preparer_donnees_pipeline` | **Aucun appel réseau** — sélection des champs, renommage, structuration pour la table cible |
| `charger_table_cible` | Exporte les **28 lignes** (4 villes × 7 jours) dans un fichier CSV structuré |

### Villes couvertes

| Ville | Latitude | Longitude |
|-------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Lyon | 45.7640 | 4.8357 |
| Marseille | 43.2965 | 5.3698 |
| Bordeaux | 44.8378 | -0.5792 |

### Pourquoi séparer extraction et transformation ?

La séparation respecte le principe de **responsabilité unique** (chaque tâche a un seul rôle) :

- Si l'API est indisponible, seule la tâche d'extraction échoue. Airflow peut la retenter
  automatiquement sans relancer la transformation.
- Si la logique de transformation change, on ne touche pas au code d'extraction.
- Les données brutes sont conservées dans XCom pour audit ou rejeu.

```python
# Tâche 1 — extraction pure : on ne touche à RIEN
resultats_bruts[ville["nom"]] = donnees_json   # JSON brut stocké tel quel
context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)

# Tâche 2 — transformation pure : AUCUN appel réseau ici
donnees_brutes = context["ti"].xcom_pull(key="donnees_brutes", task_ids="extraire_donnees_brutes")
# On sélectionne et structure uniquement ici
```

### Champs retenus — justification

| Champ dans la table | Champ API d'origine | Pourquoi retenu |
|---------------------|--------------------|--------------------|
| `ville` | *(ajouté)* | Clé géographique indispensable pour identifier la source |
| `date` | `time` | Dimension temporelle obligatoire |
| `temp_max_c` | `temperature_2m_max` | Indicateur thermique journalier maximal |
| `temp_min_c` | `temperature_2m_min` | Indicateur thermique journalier minimal |
| `temp_moyenne_c` | `temperature_2m_mean` | Synthèse journalière, utile pour calculer des moyennes |
| `precipitation_mm` | `precipitation_sum` | Volume de pluie cumulé (alertes, agriculture) |
| `vent_max_kmh` | `windspeed_10m_max` | Vitesse de vent maximale (sécurité, événements extrêmes) |
| `code_meteo` | `weathercode` | Code WMO : catégorise le temps (soleil, pluie, neige, orage…) |

### Champs supprimés — justification

| Champ supprimé | Raison |
|----------------|--------|
| `latitude` / `longitude` | Redondant avec le nom de ville, augmente inutilement la taille des données |
| `elevation` | Altitude fixe de la ville, sans valeur pour l'analyse météo quotidienne |
| `timezone` / `utc_offset_seconds` | Fixé à `Europe/Paris` en amont, valeur constante sans intérêt |
| `generationtime_ms` | Métadonnée interne de l'API (temps de génération), aucune valeur métier |

### Aperçu des données produites

```
VILLE        DATE          MAX    MIN    MOY   PLUIE     VENT   CODE
-------------------------------------------------------------------
Paris        2026-06-02   22.7°C 17.3°C 19.5°C   5.6mm  19.9km/h   96
Lyon         2026-06-02   24.2°C 17.6°C 20.4°C  32.3mm  16.8km/h   99
Marseille    2026-06-02   25.7°C 21.7°C 23.6°C   0.0mm  19.4km/h   51
Bordeaux     2026-06-02   21.8°C 17.2°C 19.0°C   0.3mm  21.5km/h   80
```

**Total : 28 lignes** (4 villes × 7 jours) dans `data/meteo_villes_*.csv`

### Commande de test

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2a_ingestion_meteo
```

---

## 7. TP2B — Pipeline complet vers PostgreSQL

### Objectif

Construire un pipeline de production complet en ajoutant :
- Le **chargement dans PostgreSQL** (avec gestion des doublons)
- Une **table de suivi d'ingestion** pour la traçabilité
- Un **DAG entièrement paramétrable** via les Variables Airflow (sans toucher au code)

### Pipeline

```
extraire_donnees_brutes ──→ transformer_donnees ──→ charger_postgresql ──→ ecrire_suivi_ingestion
```

### Description des tâches

| Tâche | Rôle |
|-------|------|
| `extraire_donnees_brutes` | Appelle l'API pour les villes définies dans la Variable `METEO_VILLES` |
| `transformer_donnees` | Sélectionne et structure les champs pour correspondre à la table `meteo_journaliere` |
| `charger_postgresql` | Insère les données avec `ON CONFLICT (ville, date) DO UPDATE` (upsert) |
| `ecrire_suivi_ingestion` | Enregistre les métadonnées du run dans `suivi_ingestion` |

### Pourquoi `ON CONFLICT DO UPDATE` ?

Si on relance le DAG pour les mêmes dates, on ne veut pas de doublons dans la base.
L'instruction `ON CONFLICT` d'PostgreSQL gère ce cas : si la paire `(ville, date)` existe déjà,
la ligne est **mise à jour** au lieu de provoquer une erreur.

```sql
INSERT INTO meteo_journaliere (ville, date, temp_max_c, ...)
VALUES (%(ville)s, %(date)s, %(temp_max_c)s, ...)
ON CONFLICT (ville, date) DO UPDATE SET
    temp_max_c = EXCLUDED.temp_max_c,
    ...
    insere_le  = NOW();
```

### Paramétrage via Variables Airflow

Aucune valeur n'est codée en dur dans le DAG. Tout est lu depuis **Admin → Variables** dans l'UI.
Cela permet de changer la liste des villes ou les paramètres de connexion **sans modifier le code**.

| Variable | Valeur | Description |
|----------|--------|-------------|
| `METEO_VILLES` | JSON des 4 villes | Liste des villes à ingérer |
| `METEO_PAST_DAYS` | `7` | Nombre de jours d'historique |
| `METEO_DB_HOST` | `localhost` | Hôte PostgreSQL |
| `METEO_DB_PORT` | `5432` | Port PostgreSQL |
| `METEO_DB_NAME` | `meteo_db` | Nom de la base de données |
| `METEO_DB_USER` | `postgres` | Utilisateur PostgreSQL |
| `METEO_DB_PASSWORD` | `postgres` | Mot de passe PostgreSQL |

```python
# Le DAG lit toujours depuis les Variables — jamais de valeurs en dur
def get_config():
    return {
        "villes":    json.loads(Variable.get("METEO_VILLES")),
        "past_days": int(Variable.get("METEO_PAST_DAYS", default_var=7)),
        "db": {
            "host":     Variable.get("METEO_DB_HOST",     default_var="localhost"),
            ...
        },
    }
```

### Schéma PostgreSQL (`sql/init_meteo_db.sql`)

```sql
-- Table principale : une ligne = un jour pour une ville
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
    UNIQUE (ville, date)              -- empêche les doublons, permet le ON CONFLICT
);

-- Table de suivi : une ligne = une exécution du DAG
CREATE TABLE IF NOT EXISTS suivi_ingestion (
    id               SERIAL PRIMARY KEY,
    dag_id           VARCHAR(100)  NOT NULL,
    run_id           VARCHAR(200)  NOT NULL,
    ville            VARCHAR(100),
    nb_lignes        INTEGER       DEFAULT 0,
    statut           VARCHAR(20)   NOT NULL,   -- 'success' ou 'failed'
    message          TEXT,
    debut_ingestion  TIMESTAMP     NOT NULL,
    fin_ingestion    TIMESTAMP     DEFAULT NOW()
);
```

### Preuve de chargement

**Table `meteo_journaliere` — 28 lignes insérées (4 villes × 7 jours) :**
```
 ville     | date       | temp_max_c | temp_min_c | precipitation_mm
-----------+------------+------------+------------+------------------
 Bordeaux  | 2026-06-02 |       21.8 |       17.2 |              0.3
 Lyon      | 2026-06-02 |       24.2 |       17.6 |             32.3
 Marseille | 2026-06-02 |       25.7 |       21.7 |              0.0
 Paris     | 2026-06-02 |       22.7 |       17.3 |              5.6
 ...
(28 rows)
```

**Table `suivi_ingestion` :**
```
 id |          dag_id          |              ville               | nb_lignes | statut
----+--------------------------+----------------------------------+-----------+---------
  1 | tp2b_pipeline_postgresql | Paris, Lyon, Marseille, Bordeaux |        28 | success
```

### Initialisation PostgreSQL

```bash
# Créer la base de données
psql -U postgres -c "CREATE DATABASE meteo_db;"

# Créer les tables
psql -U postgres -d meteo_db -f sql/init_meteo_db.sql

# Définir les Variables Airflow
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate

airflow variables set METEO_VILLES '[{"nom":"Paris","latitude":48.8566,"longitude":2.3522},{"nom":"Lyon","latitude":45.7640,"longitude":4.8357},{"nom":"Marseille","latitude":43.2965,"longitude":5.3698},{"nom":"Bordeaux","latitude":44.8378,"longitude":-0.5792}]'
airflow variables set METEO_PAST_DAYS 7
airflow variables set METEO_DB_HOST localhost
airflow variables set METEO_DB_PORT 5432
airflow variables set METEO_DB_NAME meteo_db
airflow variables set METEO_DB_USER postgres
airflow variables set METEO_DB_PASSWORD postgres
```

### Commande de test

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp2b_pipeline_postgresql
```

---

## 8. TP3 — Data Lake Medallion

### Objectif

Implémenter une **architecture Data Lake Medallion** complète avec trois couches :
- **Bronze** : conservation des données brutes telles que reçues de l'API
- **Silver** : données transformées et structurées
- **Gold** : données optimisées pour l'analyse dans PostgreSQL

Chaque couche a un rôle distinct. Les données sont traçables de bout en bout.

### Qu'est-ce que l'architecture Medallion ?

L'architecture Medallion (ou Delta Lake) est un standard pour organiser un Data Lake.
Elle résout un problème fondamental : comment stocker des données brutes tout en ayant
des données propres pour l'analyse, sans jamais perdre les données d'origine ?

```
API
 │
 ▼
BRONZE ── JSON brut, tel que reçu, jamais modifié
 │           └─ Intérêt : rejouer le pipeline sans rappeler l'API, audit complet
 ▼
SILVER ── CSV transformé, champs sélectionnés, types normalisés
 │           └─ Intérêt : données utilisables par d'autres outils sans passer par PostgreSQL
 ▼
GOLD ──── Table PostgreSQL, indexée, optimisée pour les requêtes et les dashboards
             └─ Intérêt : performances maximales pour l'analyse
```

### Rôle de MinIO

**MinIO** est un système de stockage objet compatible avec l'API Amazon S3.
Il joue le rôle d'un "Google Drive" pour les pipelines de données :
- Stocke des fichiers (JSON, CSV, Parquet…) organisés en **buckets** (compartiments)
- Accessible depuis Python avec la bibliothèque `boto3`
- Les couches Bronze et Silver y sont stockées sous forme de fichiers horodatés

```
MinIO
├── meteo-bronze/                   ← bucket Bronze
│   └── 2026-06-09/
│       ├── paris.json              ← JSON brut de l'API pour Paris
│       ├── lyon.json
│       ├── marseille.json
│       └── bordeaux.json
└── meteo-silver/                   ← bucket Silver
    └── 2026-06-09/
        └── meteo_villes.csv        ← CSV transformé (4 villes, 28 lignes)
```

### Pipeline

```
extraire_api ──→ stocker_bronze ──→ transformer_silver ──→ charger_gold ──→ ecrire_suivi
```

### Description des tâches

| Tâche | Couche | Rôle |
|-------|--------|------|
| `extraire_api` | — | Appelle l'API Open-Meteo pour les 4 villes, pousse le JSON brut en XCom |
| `stocker_bronze` | **BRONZE** | Écrit le JSON brut de chaque ville dans MinIO `meteo-bronze/YYYY-MM-DD/<ville>.json` |
| `transformer_silver` | **SILVER** | Transforme le JSON en CSV structuré, écrit dans MinIO `meteo-silver/YYYY-MM-DD/meteo_villes.csv` |
| `charger_gold` | **GOLD** | Lit les données transformées depuis XCom, insère dans PostgreSQL `meteo_journaliere` |
| `ecrire_suivi` | — | Enregistre les métadonnées du run (chemins Bronze, Silver, nombre de lignes Gold) |

### Connexion à MinIO depuis Python

```python
import boto3
from botocore.client import Config

def get_s3_client(config):
    return boto3.client(
        "s3",
        endpoint_url=config["minio"]["endpoint"],       # http://localhost:9000
        aws_access_key_id=config["minio"]["access_key"],
        aws_secret_access_key=config["minio"]["secret_key"],
        config=Config(signature_version="s3v4"),        # requis pour MinIO
    )

# Écrire un fichier dans MinIO
s3.put_object(
    Bucket="meteo-bronze",
    Key="2026-06-09/paris.json",
    Body=json.dumps(donnees).encode("utf-8"),
    ContentType="application/json",
)
```

### Variables Airflow pour MinIO

| Variable | Valeur | Description |
|----------|--------|-------------|
| `MINIO_ENDPOINT` | `http://localhost:9000` | URL d'accès à MinIO |
| `MINIO_ACCESS_KEY` | `minioadmin` | Identifiant MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Mot de passe MinIO |
| `MINIO_BUCKET_BRONZE` | `meteo-bronze` | Bucket de la couche Bronze |
| `MINIO_BUCKET_SILVER` | `meteo-silver` | Bucket de la couche Silver |

### Mise en place de MinIO

**Lancer MinIO via Docker :**
```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio_data:/data \
  minio/minio server /data --console-address ":9001"
```

Console web MinIO : **http://localhost:9001** — identifiants : `minioadmin` / `minioadmin`

**Créer les buckets :**
```bash
source airflow_venv/bin/activate
python3 - <<'EOF'
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"))
for b in ["meteo-bronze", "meteo-silver"]:
    s3.create_bucket(Bucket=b)
    print(f"Bucket créé : {b}")
EOF
```

**Définir les Variables Airflow MinIO :**
```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow variables set MINIO_ENDPOINT http://localhost:9000
airflow variables set MINIO_ACCESS_KEY minioadmin
airflow variables set MINIO_SECRET_KEY minioadmin
airflow variables set MINIO_BUCKET_BRONZE meteo-bronze
airflow variables set MINIO_BUCKET_SILVER meteo-silver
```

### Preuve d'exécution

**Fichiers Bronze et Silver dans MinIO après un run :**
```
meteo-bronze/2026-06-09/bordeaux.json     1302 octets  ← JSON brut de l'API
meteo-bronze/2026-06-09/lyon.json         1308 octets
meteo-bronze/2026-06-09/marseille.json    1298 octets
meteo-bronze/2026-06-09/paris.json        1303 octets

meteo-silver/2026-06-09/meteo_villes.csv  1375 octets  ← CSV transformé, 28 lignes
```

**Gold — PostgreSQL `meteo_journaliere` :**
```
  ville    | nb_jours
-----------+---------
 Bordeaux  |       7
 Lyon      |       7
 Marseille |       7
 Paris     |       7
(4 rows, total : 28 lignes)
```

**Suivi d'ingestion :**
```
 id |   dag_id    | nb_lignes | statut  | message
----+-------------+-----------+---------+-----------------------------------------
  3 | tp3_data_lake |       28 | success | Bronze: 4 fichiers JSON | Silver: 1 CSV | Gold: 28 lignes
```

### Commande de test

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate
airflow dags test tp3_data_lake
```

---

## 9. Installation complète

### Prérequis système

- macOS ou Linux
- Python 3.12+
- PostgreSQL 14+ installé et démarré (pour TP2B et TP3)
- Docker (pour MinIO, TP3 uniquement)
- Connexion internet (appels API Open-Meteo)

### Installation pas à pas

```bash
# 1. Cloner le dépôt
git clone https://github.com/MousBak/Premier-Dag-Airflow.git
cd Premier-Dag-Airflow

# 2. Créer l'environnement virtuel Python
python3 -m venv airflow_venv
source airflow_venv/bin/activate

# 3. Installer Apache Airflow 2.9.1 avec ses contraintes de dépendances
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"
pip install "apache-airflow==2.9.1" --constraint "${CONSTRAINT_URL}"

# 4. Installer les bibliothèques supplémentaires
pip install certifi psycopg2-binary boto3

# 5. Initialiser la base de données Airflow (SQLite par défaut)
export AIRFLOW_HOME=$(pwd)
airflow db migrate

# 6. Créer un utilisateur admin pour l'interface web
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Démarrer Airflow

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate

# Terminal 1 — scheduler
airflow scheduler

# Terminal 2 — interface web
airflow webserver --port 8081
```

Interface web : **http://localhost:8081** — identifiants : `admin` / `admin`

### Variables Airflow à créer (TP2B et TP3)

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate

# Variables communes TP2B et TP3
airflow variables set METEO_VILLES '[{"nom":"Paris","latitude":48.8566,"longitude":2.3522},{"nom":"Lyon","latitude":45.7640,"longitude":4.8357},{"nom":"Marseille","latitude":43.2965,"longitude":5.3698},{"nom":"Bordeaux","latitude":44.8378,"longitude":-0.5792}]'
airflow variables set METEO_PAST_DAYS 7
airflow variables set METEO_DB_HOST localhost
airflow variables set METEO_DB_PORT 5432
airflow variables set METEO_DB_NAME meteo_db
airflow variables set METEO_DB_USER postgres
airflow variables set METEO_DB_PASSWORD postgres

# Variables MinIO (TP3 uniquement)
airflow variables set MINIO_ENDPOINT http://localhost:9000
airflow variables set MINIO_ACCESS_KEY minioadmin
airflow variables set MINIO_SECRET_KEY minioadmin
airflow variables set MINIO_BUCKET_BRONZE meteo-bronze
airflow variables set MINIO_BUCKET_SILVER meteo-silver
```

---

## 10. Guide de vérification

### Tester tous les DAGs

```bash
export AIRFLOW_HOME=$(pwd)
source airflow_venv/bin/activate

airflow dags test tp2_pipeline_etl_simple
airflow dags test tp2a_ingestion_meteo
airflow dags test tp2b_pipeline_postgresql
airflow dags test tp3_data_lake
```

Chaque commande doit se terminer par `state=success`.

### Vérifier les fichiers CSV (TP2 et TP2A)

```bash
ls data/
# meteo_paris_*.csv      → produit par TP2
# meteo_villes_*.csv     → produit par TP2A
```

### Vérifier PostgreSQL (TP2B et TP3)

```bash
# Nombre de lignes dans la table principale
psql -U postgres -d meteo_db -c "SELECT COUNT(*) FROM meteo_journaliere;"
# Résultat attendu : 28

# Répartition par ville
psql -U postgres -d meteo_db -c \
  "SELECT ville, COUNT(*) AS nb_jours FROM meteo_journaliere GROUP BY ville ORDER BY ville;"

# Historique des ingestions
psql -U postgres -d meteo_db -c \
  "SELECT id, dag_id, nb_lignes, statut, message FROM suivi_ingestion ORDER BY fin_ingestion DESC;"
```

### Vérifier MinIO (TP3)

```bash
source airflow_venv/bin/activate
python3 - <<'EOF'
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"))
for bucket in ["meteo-bronze", "meteo-silver"]:
    print(f"\n=== Bucket : {bucket} ===")
    for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
        print(f"  {obj['Key']}  ({obj['Size']} octets)")
EOF
```

---

## 11. Auteur

| | |
|--|--|
| **Nom** | Bakayoko Moussa |
| **Cours** | Orchestration de pipelines de données — Apache Airflow |
| **Outils** | Apache Airflow 2.9.1 · Python 3.12 · PostgreSQL 14 · MinIO · Docker |
| **API** | [Open-Meteo](https://open-meteo.com/) — données météo gratuites, sans clé d'authentification |
| **Dépôt** | [github.com/MousBak/Premier-Dag-Airflow](https://github.com/MousBak/Premier-Dag-Airflow) |
| **Date** | Juin 2026 |
