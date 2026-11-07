"""Population data ingestion and transformation package."""

from data_ingestion_toolbox.log_redaction import install_log_redaction

# Every DAG and script imports this package before it makes a request, so
# provider keys sent as query parameters never reach a log (see log_redaction).
install_log_redaction()
