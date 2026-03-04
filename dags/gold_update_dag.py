# dags/gold_update_dag.py
#
# NOTE: Gold processing is now handled at the tail end of each subject-specific
# ingest DAG (acs_ingest_dag.py, bls_ingest_dag.py, fred_ingest_dag.py).
# This standalone DAG is no longer used.
