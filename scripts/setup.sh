#!/usr/bin/env bash
# One-command project setup.
# Usage: bash scripts/setup.sh

set -euo pipefail

AIRFLOW_VERSION="2.9.1"
PYTHON_VERSION="3.12"
VENV_DIR="airflow_venv"

echo "=== Weather Data Platform — Setup ==="
echo ""

# 1. Virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "[1/5] Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "[1/5] Virtual environment already exists — skipping"
fi

source "$VENV_DIR/bin/activate"

# 2. Install dependencies
echo "[2/5] Installing Airflow $AIRFLOW_VERSION and dependencies..."
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install --quiet "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install --quiet certifi psycopg2-binary boto3 pandas pyarrow pytest pytest-mock

# 3. Initialize Airflow database
echo "[3/5] Initializing Airflow metadata database..."
export AIRFLOW_HOME=$(pwd)
airflow db migrate

# 4. Create admin user
echo "[4/5] Creating Airflow admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || echo "    Admin user already exists"

# 5. Load Airflow Variables from config
echo "[5/5] Loading Airflow Variables from config/variables.json..."
python3 - <<'PYEOF'
import json, subprocess, sys
with open("config/variables.json") as f:
    variables = json.load(f)
for key, value in variables.items():
    subprocess.run(["airflow", "variables", "set", key, value], capture_output=True)
    print(f"    Set: {key}")
PYEOF

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config/variables.json and update CHANGE_ME values"
echo "  2. Start MinIO:  make minio"
echo "  3. Start Airflow: make run"
echo "  4. Open UI:       http://localhost:8081  (admin / admin)"
