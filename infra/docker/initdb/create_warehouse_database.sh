#!/bin/sh
# Create the warehouse database beside Airflow's metadata database.
#
# The Airflow-only stack runs one Postgres cluster, and every DAG resolves
# PostgresHook(postgres_conn_id="public_data") to write raw_capture, control,
# silver_*, and gold_*. Pointing that connection at Airflow's own metadata
# database puts a 100-million-row warehouse inside the database Airflow needs
# to schedule, and makes the documented reset -- `DROP DATABASE <warehouse>`,
# BETA_RESET_REINGESTION.md section 2 -- drop Airflow with it. So the
# warehouse gets its own database in the same cluster, and this refuses to
# start rather than silently accepting a name that would recreate the defect.
set -eu

: "${WAREHOUSE_DB_NAME:?WAREHOUSE_DB_NAME must name the warehouse database}"

if [ "${WAREHOUSE_DB_NAME}" = "${POSTGRES_DB}" ]; then
    echo "WAREHOUSE_DB_NAME is ${WAREHOUSE_DB_NAME}, which is Airflow's own" \
         "metadata database. Ingestion must not write there; set" \
         "PUBLIC_DATA_DB_NAME to a different database." >&2
    exit 1
fi

createdb --username "${POSTGRES_USER}" --owner "${POSTGRES_USER}" \
    "${WAREHOUSE_DB_NAME}"
echo "Created warehouse database ${WAREHOUSE_DB_NAME}"
