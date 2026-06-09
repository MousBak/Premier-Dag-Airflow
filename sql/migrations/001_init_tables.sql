-- TP 2B — Script SQL d'initialisation
-- Base : meteo_db
-- Tables : meteo_journaliere + suivi_ingestion

-- Table principale : données météo journalières par ville
CREATE TABLE IF NOT EXISTS meteo_journaliere (
    id               SERIAL PRIMARY KEY,
    ville            VARCHAR(100)   NOT NULL,
    date             DATE           NOT NULL,
    temp_max_c       NUMERIC(5, 1),
    temp_min_c       NUMERIC(5, 1),
    temp_moyenne_c   NUMERIC(5, 1),
    precipitation_mm NUMERIC(6, 1),
    vent_max_kmh     NUMERIC(6, 1),
    code_meteo       INTEGER,
    insere_le        TIMESTAMP      DEFAULT NOW(),
    UNIQUE (ville, date)
);

-- Table de suivi : une ligne par exécution du DAG (traçabilité)
CREATE TABLE IF NOT EXISTS suivi_ingestion (
    id               SERIAL PRIMARY KEY,
    dag_id           VARCHAR(100)   NOT NULL,
    run_id           VARCHAR(200)   NOT NULL,
    ville            VARCHAR(100),
    nb_lignes        INTEGER        DEFAULT 0,
    statut           VARCHAR(20)    NOT NULL,  -- 'success' ou 'failed'
    message          TEXT,
    debut_ingestion  TIMESTAMP      NOT NULL,
    fin_ingestion    TIMESTAMP      DEFAULT NOW()
);
