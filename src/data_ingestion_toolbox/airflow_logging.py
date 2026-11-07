"""Airflow logging configuration that never writes provider credentials.

Selected by ``AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=
data_ingestion_toolbox.airflow_logging.LOGGING_CONFIG`` (see
``infra/airflow/airflow.env.example``), so it applies to the scheduler, the
DAG processor, the webserver and every task process before any DAG is parsed.

It is Airflow's own default configuration plus:

* ``httpx`` and ``httpcore`` held at WARNING, so the INFO ``HTTP Request: GET
  <url>`` line carrying ``key=``/``api_key=`` is never emitted;
* :class:`RedactSecretsFilter` on every handler, masking secret URL/JSON
  parameters and credential environment values in whatever reaches a handler;
* the process-wide redacting log-record factory from
  :mod:`data_ingestion_toolbox.log_redaction`.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

from data_ingestion_toolbox.log_redaction import (
    QUIET_HTTP_LOGGERS,
    install_log_redaction,
    redact_secrets,
)

FILTER_NAME = "redact_provider_secrets"


class RedactSecretsFilter(logging.Filter):
    """Mask secrets in a record a handler is about to write."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        redacted = redact_secrets(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text)
        return True


def build_logging_config(base: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return ``base`` (Airflow's default) with credential redaction added."""
    config = copy.deepcopy(DEFAULT_LOGGING_CONFIG if base is None else base)
    config.setdefault("filters", {})[FILTER_NAME] = {
        "()": f"{__name__}.RedactSecretsFilter"
    }
    for handler in config.get("handlers", {}).values():
        filters = handler.setdefault("filters", [])
        if FILTER_NAME not in filters:
            filters.append(FILTER_NAME)
    loggers = config.setdefault("loggers", {})
    for name in QUIET_HTTP_LOGGERS:
        loggers[name] = {"level": "WARNING", "propagate": True, "handlers": []}
    return config


install_log_redaction()
LOGGING_CONFIG = build_logging_config()
