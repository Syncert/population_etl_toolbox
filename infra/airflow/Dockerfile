ARG AIRFLOW_UID=50000
FROM apache/airflow:2.9.3

ARG AIRFLOW_UID=50000

USER root
# Keep root install surface minimal; include build essentials for native wheels if needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/data_ingestion_toolbox

COPY --chown=airflow:root . /opt/data_ingestion_toolbox

RUN pip install --no-cache-dir -e /opt/data_ingestion_toolbox[airflow]
