"""Keep provider credentials out of every log line.

Several providers authenticate with a query parameter (Census ``key=``, EIA
``api_key=``, FRED ``api_key=``, Socrata ``$$app_token=``), and Dataverse
answers a guestbook with a signed download URL. ``httpx`` logs every request
at INFO as ``HTTP Request: GET <full url>``, and ``raise_for_status`` puts the
same URL in its exception message, so a key reaches Airflow task logs unless
something stops it.

Two layers, both installed by :func:`install_log_redaction`:

1. ``httpx`` and ``httpcore`` request logging is raised to WARNING, so the
   per-request INFO line is never emitted.
2. A log-record factory masks secret query/JSON parameters and the values of
   credential environment variables in the message, its arguments and any
   formatted traceback before a handler sees the record. A record factory runs
   for every logger in the process, Airflow's task handler included, so the
   mask does not depend on which handlers a DAG run happens to configure.

The install is idempotent and runs on package import, and again from the
Airflow logging configuration (:mod:`data_ingestion_toolbox.airflow_logging`).
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Callable, Iterable, Mapping

REDACTED = "REDACTED"

#: Loggers that render full request URLs at INFO/DEBUG.
QUIET_HTTP_LOGGERS = ("httpx", "httpcore")

#: Query, form and JSON parameter names whose value is a credential.
SECRET_PARAMETER_NAMES = (
    "key",
    "api_key",
    "apikey",
    "api-key",
    "registrationkey",
    "token",
    "access_token",
    "api_token",
    "app_token",
    "$$app_token",
    "%24%24app_token",
    "password",
    "secret",
    "client_secret",
    "signature",
    "sig",
    "x-amz-signature",
    "x-amz-credential",
    "x-amz-security-token",
    "x-goog-signature",
    "x-goog-credential",
)

#: Environment variables whose value is a credential or the operator's
#: identity (the Dataverse guestbook answers). Their literal values are masked
#: wherever they appear, whatever surrounds them.
SECRET_ENVIRONMENT_VARIABLES = (
    "CENSUS_API_KEY",
    "BLS_API_KEY",
    "EIA_API_KEY",
    "FRED_API_KEY",
    "FBI_CDE_API_KEY",
    "USDA_NASS_API_KEY",
    "CDC_SOCRATA_APP_TOKEN",
    "DATAVERSE_API_TOKEN",
    "FCC_BDC_API_TOKEN",
    "FCC_BDC_USERNAME",
    "HUD_USER_API_TOKEN",
    "ELECTIONS_GUESTBOOK_NAME",
    "ELECTIONS_GUESTBOOK_EMAIL",
)
_SECRET_ENVIRONMENT_SUFFIXES = ("_API_KEY", "_API_TOKEN", "_APP_TOKEN")
# Shorter values are too likely to be ordinary words to mask everywhere.
_MIN_MASKED_VALUE_LENGTH = 4

_NAMES = "|".join(re.escape(name) for name in SECRET_PARAMETER_NAMES)
# ?key=VALUE / &api_key=VALUE / ;sig=VALUE in a URL or form body.
_QUERY_PARAMETER = re.compile(rf"(?i)([?&;](?:{_NAMES})=)([^&;\s'\"#<>]+)")
# 'key': 'VALUE' / "api_key": "VALUE" in a logged dict or JSON body.
_QUOTED_PARAMETER = re.compile(rf"(?i)(['\"](?:{_NAMES})['\"]\s*:\s*['\"])([^'\"]+)")

_lock = threading.Lock()
_installed_factory: Callable[..., logging.LogRecord] | None = None
_masked_values: tuple[str, ...] = ()


def secret_environment_values(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Credential values present in ``environ``, longest first."""
    environ = os.environ if environ is None else environ
    names = set(SECRET_ENVIRONMENT_VARIABLES)
    names.update(
        name for name in environ if name.upper().endswith(_SECRET_ENVIRONMENT_SUFFIXES)
    )
    values = {
        environ[name].strip()
        for name in names
        if name in environ and len(environ[name].strip()) >= _MIN_MASKED_VALUE_LENGTH
    }
    return tuple(sorted(values, key=len, reverse=True))


def redact_secrets(text: str, values: Iterable[str] | None = None) -> str:
    """Return ``text`` with secret parameters and known secret values masked.

    ``values`` defaults to the credential environment values captured when
    redaction was installed (or read now, if it was not).
    """
    if not text:
        return text
    text = _QUERY_PARAMETER.sub(lambda m: m.group(1) + REDACTED, text)
    text = _QUOTED_PARAMETER.sub(lambda m: m.group(1) + REDACTED, text)
    masked = _masked_values if values is None and _installed_factory else values
    if masked is None:
        masked = secret_environment_values()
    for value in masked:
        if value and value in text:
            text = text.replace(value, REDACTED)
    return text


def _redact_record(record: logging.LogRecord) -> None:
    """Mask ``record`` in place; leave it untouched when nothing is secret."""
    try:
        message = record.getMessage()
    except Exception:  # a malformed record is the handler's problem, not ours
        message = None
    if message is not None:
        redacted = redact_secrets(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
    if record.exc_info and not record.exc_text:
        try:
            formatted = logging.Formatter().formatException(record.exc_info)
        except Exception:
            formatted = None
        if formatted is not None:
            redacted = redact_secrets(formatted)
            if redacted != formatted:
                # Formatter.format() uses a preset exc_text instead of
                # re-rendering exc_info, so the masked text is what is written.
                record.exc_text = redacted
    if record.stack_info:
        record.stack_info = redact_secrets(record.stack_info)


def install_log_redaction(environ: Mapping[str, str] | None = None) -> None:
    """Quiet URL-logging HTTP loggers and mask secrets in every log record.

    Safe to call repeatedly; later calls refresh the masked environment values.
    """
    global _installed_factory, _masked_values

    for name in QUIET_HTTP_LOGGERS:
        logger = logging.getLogger(name)
        if logger.level == logging.NOTSET or logger.level < logging.WARNING:
            logger.setLevel(logging.WARNING)

    with _lock:
        _masked_values = secret_environment_values(environ)
        current = logging.getLogRecordFactory()
        if current is _installed_factory:
            return
        previous = current

        def redacting_record_factory(*args, **kwargs) -> logging.LogRecord:
            record = previous(*args, **kwargs)
            _redact_record(record)
            return record

        _installed_factory = redacting_record_factory
        logging.setLogRecordFactory(redacting_record_factory)


def redaction_installed() -> bool:
    """Whether this process's log-record factory is the redacting one."""
    return _installed_factory is not None and (
        logging.getLogRecordFactory() is _installed_factory
    )


def scrub_log_file(path: os.PathLike[str] | str, *, apply: bool) -> bool:
    """Mask secrets already written to ``path``; return whether any were found.

    With ``apply`` the file is rewritten through a temporary sibling and an
    atomic rename, keeping its mode; otherwise it is only read.
    """
    path = os.fspath(path)
    with open(path, "rb") as handle:
        raw = handle.read()
    if b"\0" in raw[:4096]:
        return False  # binary, not a log
    text = raw.decode("utf-8", errors="surrogateescape")
    redacted = redact_secrets(text)
    if redacted == text:
        return False
    if apply:
        temporary = f"{path}.redacting"
        with open(temporary, "wb") as handle:
            handle.write(redacted.encode("utf-8", errors="surrogateescape"))
        os.chmod(temporary, os.stat(path).st_mode & 0o7777)
        os.replace(temporary, path)
    return True
