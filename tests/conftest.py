"""
Shared pytest configuration.
Sets AIRFLOW_HOME and Python path before any test imports Airflow.
"""

import os
import sys

# Must be set before any airflow import
os.environ.setdefault(
    "AIRFLOW_HOME",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")

# Make dags/ importable as a package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
