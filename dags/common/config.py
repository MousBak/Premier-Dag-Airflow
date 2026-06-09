"""
Shared configuration for all weather DAGs.
Centralizes API fields, city list, and client factories to avoid duplication.
"""

import json
import ssl
import certifi
import boto3
from botocore.client import Config
from airflow.models import Variable


CHAMPS_API = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]

COLONNES_TABLE = [
    "ville", "date", "temp_max_c", "temp_min_c",
    "temp_moyenne_c", "precipitation_mm", "vent_max_kmh", "code_meteo",
]


def get_ssl_context():
    return ssl.create_default_context(cafile=certifi.where())


def get_config() -> dict:
    """Load all pipeline configuration from Airflow Variables."""
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


def get_s3_client(config: dict):
    """Return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=config["minio"]["endpoint"],
        aws_access_key_id=config["minio"]["access_key"],
        aws_secret_access_key=config["minio"]["secret_key"],
        config=Config(signature_version="s3v4"),
    )


def build_api_params(ville: dict, past_days: int) -> dict:
    """Build Open-Meteo query parameters for a given city."""
    return {
        "latitude":      ville["latitude"],
        "longitude":     ville["longitude"],
        "daily":         ",".join(CHAMPS_API),
        "past_days":     past_days,
        "forecast_days": 0,
        "timezone":      "Europe/Paris",
    }


def json_to_rows(nom_ville: str, json_brut: dict) -> list[dict]:
    """Convert raw API JSON for one city into a list of flat row dicts."""
    daily = json_brut["daily"]
    return [
        {
            "ville":            nom_ville,
            "date":             daily["time"][i],
            "temp_max_c":       daily["temperature_2m_max"][i],
            "temp_min_c":       daily["temperature_2m_min"][i],
            "temp_moyenne_c":   daily["temperature_2m_mean"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "vent_max_kmh":     daily["windspeed_10m_max"][i],
            "code_meteo":       daily["weathercode"][i],
        }
        for i in range(len(daily["time"]))
    ]
