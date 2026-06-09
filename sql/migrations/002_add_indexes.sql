-- Migration 002 — performance indexes
-- Run after 001_init_tables.sql

-- Speed up queries filtering by city or date range
CREATE INDEX IF NOT EXISTS idx_meteo_ville      ON meteo_journaliere (ville);
CREATE INDEX IF NOT EXISTS idx_meteo_date       ON meteo_journaliere (date DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_ville_date ON meteo_journaliere (ville, date DESC);

-- Speed up ingestion audit queries
CREATE INDEX IF NOT EXISTS idx_suivi_dag_id     ON suivi_ingestion (dag_id);
CREATE INDEX IF NOT EXISTS idx_suivi_fin        ON suivi_ingestion (fin_ingestion DESC);
