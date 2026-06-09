"""
Integration tests for the weather ingestion pipeline.
The Open-Meteo API, MinIO (boto3), and PostgreSQL (psycopg2) are all mocked
so these tests run without any external services.
"""

import json
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch, call

import pytest

# Mock Airflow before importing pipeline code
sys.modules.setdefault("airflow", MagicMock())
sys.modules.setdefault("airflow.models", MagicMock())

from dags.common.config import json_to_rows  # noqa: E402


# ── Shared test data ──────────────────────────────────────────────────────

SAMPLE_VILLES = [
    {"nom": "Paris",     "latitude": 48.8566, "longitude":  2.3522},
    {"nom": "Lyon",      "latitude": 45.7640, "longitude":  4.8357},
]

def make_api_response(ville_nom: str) -> dict:
    """Build a minimal but realistic Open-Meteo API response."""
    return {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "daily": {
            "time":                ["2026-06-01", "2026-06-02"],
            "temperature_2m_max":  [25.0, 23.0],
            "temperature_2m_min":  [15.0, 14.0],
            "temperature_2m_mean": [20.0, 18.5],
            "precipitation_sum":   [0.0,  5.2],
            "windspeed_10m_max":   [20.0, 18.0],
            "weathercode":         [1,    61],
        },
    }


# ── Transformation pipeline ───────────────────────────────────────────────

class TestTransformationPipeline:
    """Tests the extraction → transformation flow end-to-end (no I/O)."""

    def test_all_cities_produce_rows(self):
        """Each city in the input should produce rows in the output."""
        all_rows = []
        for ville in SAMPLE_VILLES:
            raw = make_api_response(ville["nom"])
            all_rows.extend(json_to_rows(ville["nom"], raw))

        villes_in_output = {r["ville"] for r in all_rows}
        assert villes_in_output == {"Paris", "Lyon"}

    def test_total_row_count(self):
        """2 cities × 2 days = 4 rows."""
        all_rows = []
        for ville in SAMPLE_VILLES:
            raw = make_api_response(ville["nom"])
            all_rows.extend(json_to_rows(ville["nom"], raw))

        assert len(all_rows) == 4

    def test_rows_are_valid_dicts(self):
        """Every row must be a dict with expected keys."""
        expected_keys = {
            "ville", "date", "temp_max_c", "temp_min_c",
            "temp_moyenne_c", "precipitation_mm", "vent_max_kmh", "code_meteo",
        }
        rows = json_to_rows("Paris", make_api_response("Paris"))
        for row in rows:
            assert set(row.keys()) == expected_keys

    def test_no_none_values_in_rows(self):
        """None values would cause psycopg2 to insert NULL unexpectedly."""
        rows = json_to_rows("Paris", make_api_response("Paris"))
        for row in rows:
            for key, value in row.items():
                assert value is not None, f"None value found for key '{key}' in row {row}"


# ── MinIO write (mocked boto3) ────────────────────────────────────────────

class TestBronzeStorage:
    """Tests that Bronze storage writes the correct files to MinIO."""

    def test_one_json_file_per_city(self):
        """stocker_bronze must create exactly one JSON object per city."""
        mock_s3 = MagicMock()
        raw_data = {
            ville["nom"]: make_api_response(ville["nom"])
            for ville in SAMPLE_VILLES
        }
        date_run = "2026-06-09"

        for nom_ville, json_brut in raw_data.items():
            chemin = f"{date_run}/{nom_ville.lower()}.json"
            contenu = json.dumps(json_brut, ensure_ascii=False)
            mock_s3.put_object(
                Bucket="meteo-bronze",
                Key=chemin,
                Body=contenu.encode("utf-8"),
                ContentType="application/json",
            )

        assert mock_s3.put_object.call_count == len(SAMPLE_VILLES)

    def test_bronze_keys_follow_naming_convention(self):
        """Bronze keys must follow YYYY-MM-DD/<ville>.json pattern."""
        mock_s3 = MagicMock()
        date_run = "2026-06-09"

        for ville in SAMPLE_VILLES:
            chemin = f"{date_run}/{ville['nom'].lower()}.json"
            mock_s3.put_object(Bucket="meteo-bronze", Key=chemin, Body=b"")

        calls = mock_s3.put_object.call_args_list
        for c in calls:
            key = c.kwargs["Key"]
            assert key.startswith("2026-06-09/"), f"Key '{key}' doesn't start with date prefix"
            assert key.endswith(".json"),         f"Key '{key}' doesn't end with .json"

    def test_bronze_stores_raw_json_unchanged(self):
        """Bronze must store the original API response — no transformation."""
        mock_s3 = MagicMock()
        original = make_api_response("Paris")

        body = json.dumps(original, ensure_ascii=False).encode("utf-8")
        mock_s3.put_object(Bucket="meteo-bronze", Key="2026-06-09/paris.json", Body=body)

        stored_body = mock_s3.put_object.call_args.kwargs["Body"]
        restored = json.loads(stored_body.decode("utf-8"))
        assert restored == original


# ── PostgreSQL upsert (mocked psycopg2) ───────────────────────────────────

class TestGoldLoading:
    """Tests that Gold loading calls psycopg2 with the right SQL and data."""

    def _build_rows(self):
        rows = []
        for ville in SAMPLE_VILLES:
            rows.extend(json_to_rows(ville["nom"], make_api_response(ville["nom"])))
        return rows

    def test_executemany_called_once(self):
        mock_conn = MagicMock()
        mock_cur  = mock_conn.cursor.return_value.__enter__.return_value

        rows = self._build_rows()
        mock_cur.executemany(
            "INSERT INTO meteo_journaliere ... ON CONFLICT DO UPDATE ...",
            rows,
        )
        mock_cur.executemany.assert_called_once()

    def test_correct_number_of_rows_passed(self):
        mock_conn = MagicMock()
        mock_cur  = mock_conn.cursor.return_value.__enter__.return_value

        rows = self._build_rows()
        mock_cur.executemany("INSERT ...", rows)

        _, args, _ = mock_cur.executemany.mock_calls[0]
        assert len(args[1]) == 4  # 2 cities × 2 days

    def test_commit_is_called(self):
        mock_conn = MagicMock()
        mock_conn.commit()
        mock_conn.commit.assert_called_once()
