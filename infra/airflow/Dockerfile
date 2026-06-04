FROM apache/airflow:2.10.2

USER root
WORKDIR /opt/airflow
COPY pyproject.toml /opt/airflow/pyproject.toml
COPY src /opt/airflow/src
COPY census_acs /opt/airflow/census_acs
COPY bls /opt/airflow/bls
COPY fred /opt/airflow/fred
COPY silver_ref /opt/airflow/silver_ref
COPY utility /opt/airflow/utility
RUN pip install --no-cache-dir -e /opt/airflow
USER airflow
