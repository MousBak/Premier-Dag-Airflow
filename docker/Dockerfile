FROM apache/airflow:2.9.1-python3.12

# Install project dependencies on top of the base Airflow image
USER airflow
RUN pip install --no-cache-dir \
    certifi \
    psycopg2-binary \
    boto3 \
    pandas \
    pyarrow
