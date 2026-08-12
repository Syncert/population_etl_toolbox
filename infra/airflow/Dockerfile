ARG AIRFLOW_UID=50000
ARG INSTALL_EXTRAS=airflow
FROM apache/airflow:2.9.3-python3.11@sha256:cc5fcb91e93e4dfe4fd8b1b53a9155dfa2670fb829891a9658a0f36ac55f67ef

ARG AIRFLOW_UID=50000
ARG INSTALL_EXTRAS

USER root
# Keep root install surface minimal; include build essentials for native wheels if needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/population_etl_toolbox

COPY --chown=airflow:root . /opt/population_etl_toolbox

RUN pip install --no-cache-dir -e "/opt/population_etl_toolbox[${INSTALL_EXTRAS}]"
