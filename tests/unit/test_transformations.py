"""
Unit tests for dags/common/config.py transformation functions.
These tests run without a real Airflow database — Airflow is mocked.
"""

import sys
from unittest.mock import MagicMock

# Mock Airflow before importing our module so Variable.get() doesn't need a DB
sys.modules.setdefault("airflow", MagicMock())
sys.modules.setdefault("airflow.models", MagicMock())

from dags.common.config import json_to_rows, build_api_params, CHAMPS_API, COLONNES_TABLE  # noqa: E402


# ── Fixtures ──────────────────────────────────────────────────────────────

SAMPLE_API_JSON = {
    "daily": {
        "time":                ["2026-06-01", "2026-06-02", "2026-06-03"],
        "temperature_2m_max":  [25.0, 23.5, 20.1],
        "temperature_2m_min":  [15.0, 14.2, 12.8],
        "temperature_2m_mean": [20.0, 18.5, 16.4],
        "precipitation_sum":   [0.0,  5.2,  0.0],
        "windspeed_10m_max":   [20.0, 18.3, 22.1],
        "weathercode":         [1,    61,   3],
    }
}

SAMPLE_VILLE = {"nom": "Paris", "latitude": 48.8566, "longitude": 2.3522}


# ── json_to_rows ──────────────────────────────────────────────────────────

class TestJsonToRows:

    def test_returns_correct_number_of_rows(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        assert len(rows) == 3

    def test_ville_is_set_on_every_row(self):
        rows = json_to_rows("Lyon", SAMPLE_API_JSON)
        assert all(r["ville"] == "Lyon" for r in rows)

    def test_dates_are_mapped_correctly(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        assert rows[0]["date"] == "2026-06-01"
        assert rows[1]["date"] == "2026-06-02"
        assert rows[2]["date"] == "2026-06-03"

    def test_temperature_fields_are_renamed(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        row = rows[0]
        assert row["temp_max_c"]     == 25.0
        assert row["temp_min_c"]     == 15.0
        assert row["temp_moyenne_c"] == 20.0

    def test_max_is_always_greater_than_min(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        for row in rows:
            assert row["temp_max_c"] > row["temp_min_c"], (
                f"temp_max_c {row['temp_max_c']} <= temp_min_c {row['temp_min_c']} on {row['date']}"
            )

    def test_all_expected_columns_present(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        for col in COLONNES_TABLE:
            assert col in rows[0], f"Column '{col}' missing from row"

    def test_no_extra_columns(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        assert set(rows[0].keys()) == set(COLONNES_TABLE)

    def test_precipitation_is_non_negative(self):
        rows = json_to_rows("Paris", SAMPLE_API_JSON)
        assert all(r["precipitation_mm"] >= 0 for r in rows)

    def test_empty_days_returns_empty_list(self):
        empty_json = {
            "daily": {
                "time": [], "temperature_2m_max": [], "temperature_2m_min": [],
                "temperature_2m_mean": [], "precipitation_sum": [],
                "windspeed_10m_max": [], "weathercode": [],
            }
        }
        rows = json_to_rows("Paris", empty_json)
        assert rows == []

    def test_multiple_cities_do_not_share_state(self):
        rows_paris = json_to_rows("Paris", SAMPLE_API_JSON)
        rows_lyon  = json_to_rows("Lyon",  SAMPLE_API_JSON)
        assert all(r["ville"] == "Paris" for r in rows_paris)
        assert all(r["ville"] == "Lyon"  for r in rows_lyon)


# ── build_api_params ──────────────────────────────────────────────────────

class TestBuildApiParams:

    def test_returns_dict(self):
        params = build_api_params(SAMPLE_VILLE, past_days=7)
        assert isinstance(params, dict)

    def test_coordinates_match_city(self):
        params = build_api_params(SAMPLE_VILLE, past_days=7)
        assert params["latitude"]  == 48.8566
        assert params["longitude"] == 2.3522

    def test_past_days_is_set(self):
        params = build_api_params(SAMPLE_VILLE, past_days=14)
        assert params["past_days"] == 14

    def test_forecast_days_is_zero(self):
        # We only want historical data, never future forecasts
        params = build_api_params(SAMPLE_VILLE, past_days=7)
        assert params["forecast_days"] == 0

    def test_timezone_is_europe_paris(self):
        params = build_api_params(SAMPLE_VILLE, past_days=7)
        assert params["timezone"] == "Europe/Paris"

    def test_all_champs_api_are_requested(self):
        params = build_api_params(SAMPLE_VILLE, past_days=7)
        requested = params["daily"].split(",")
        for champ in CHAMPS_API:
            assert champ in requested, f"API field '{champ}' not in request params"


# ── CHAMPS_API / COLONNES_TABLE constants ────────────────────────────────

class TestConstants:

    def test_champs_api_not_empty(self):
        assert len(CHAMPS_API) > 0

    def test_colonnes_table_not_empty(self):
        assert len(COLONNES_TABLE) > 0

    def test_colonnes_table_includes_ville_and_date(self):
        assert "ville" in COLONNES_TABLE
        assert "date"  in COLONNES_TABLE
