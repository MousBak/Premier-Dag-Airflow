"""
DAG integrity tests.
Verifies that all DAGs can be imported cleanly and meet structural requirements.
These tests catch broken imports, missing dependencies, and configuration errors
before they reach the Airflow scheduler.
"""

import os
import pytest
from airflow.models import DagBag

DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "dags",
)

EXPECTED_DAG_IDS = [
    "tp2_pipeline_etl_simple",
    "tp2a_ingestion_meteo",
    "tp2b_pipeline_postgresql",
    "tp3_data_lake",
]


@pytest.fixture(scope="module")
def dag_bag():
    """Load all DAGs once for the entire test module."""
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


# ── Import health ─────────────────────────────────────────────────────────

def test_no_import_errors(dag_bag):
    """All DAG files must import without raising exceptions."""
    errors = dag_bag.import_errors
    assert errors == {}, (
        "DAG import errors detected:\n"
        + "\n".join(f"  {path}:\n    {err}" for path, err in errors.items())
    )


# ── DAG presence ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_is_present(dag_bag, dag_id):
    """Every expected DAG must be found in the DagBag."""
    assert dag_id in dag_bag.dags, (
        f"DAG '{dag_id}' not found. Available: {list(dag_bag.dags.keys())}"
    )


# ── DAG configuration ─────────────────────────────────────────────────────

@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_has_description(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    assert dag.description, f"DAG '{dag_id}' is missing a description"


@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_has_tags(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    assert dag.tags, f"DAG '{dag_id}' has no tags — add tags for UI filtering"


@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_catchup_is_disabled(dag_bag, dag_id):
    """catchup=True can create thousands of backfill runs unintentionally."""
    dag = dag_bag.dags[dag_id]
    assert not dag.catchup, f"DAG '{dag_id}' has catchup=True — set catchup=False"


@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_has_owner(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    assert dag.owner not in ("", "airflow"), (
        f"DAG '{dag_id}' uses default owner '{dag.owner}' — set a real owner"
    )


# ── Task structure ────────────────────────────────────────────────────────

@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_has_at_least_two_tasks(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    assert len(dag.tasks) >= 2, (
        f"DAG '{dag_id}' has only {len(dag.tasks)} task(s) — expected at least 2"
    )


@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_all_tasks_have_retries(dag_bag, dag_id):
    """Every task should have at least 1 retry to handle transient failures."""
    dag = dag_bag.dags[dag_id]
    for task in dag.tasks:
        assert task.retries >= 1, (
            f"Task '{task.task_id}' in DAG '{dag_id}' has retries=0"
        )
