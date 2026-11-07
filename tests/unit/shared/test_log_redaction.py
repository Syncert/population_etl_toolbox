"""Provider credentials never reach a log line, whichever logger writes it."""

from __future__ import annotations

import io
import logging

import pytest

from data_ingestion_toolbox import log_redaction
from data_ingestion_toolbox.log_redaction import (
    REDACTED,
    install_log_redaction,
    redact_secrets,
    redaction_installed,
    secret_environment_values,
)
from data_ingestion_toolbox.normalization import sanitize_error_message

pytestmark = pytest.mark.unit

# Synthetic stand-ins; no real credential appears in this repository.
FAKE_CENSUS_KEY = "fake0census0key0value0000000000000000000"
FAKE_EIA_KEY = "fakeEIAkeyValue123"


@pytest.fixture
def captured_logger():
    """A logger writing through a plain handler into a buffer."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    logger = logging.getLogger("tests.log_redaction")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield logger, stream
    logger.removeHandler(handler)


def test_package_import_installs_redaction_and_quiets_httpx() -> None:
    """Covers: ETL-038 — HTTP access logs that render request URLs stay above INFO."""
    assert redaction_installed()
    for name in ("httpx", "httpcore"):
        assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING


@pytest.mark.parametrize(
    ("url", "secret"),
    [
        (
            "https://api.census.gov/data/2023/acs/acs5?get=NAME&for=county:*"
            f"&in=state:55&key={FAKE_CENSUS_KEY}",
            FAKE_CENSUS_KEY,
        ),
        (
            "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
            f"?api_key={FAKE_EIA_KEY}&frequency=weekly",
            FAKE_EIA_KEY,
        ),
        (f"https://example.test/x?$$app_token={FAKE_EIA_KEY}", FAKE_EIA_KEY),
        (
            f"https://bucket.example/f.tab?X-Amz-Signature={FAKE_EIA_KEY}&x=1",
            FAKE_EIA_KEY,
        ),
    ],
)
def test_logged_request_url_never_contains_the_key(
    captured_logger, url: str, secret: str
) -> None:
    """Covers: ETL-038, DAG-014 — a logged request URL carries no key value."""
    logger, stream = captured_logger
    install_log_redaction(environ={})

    logger.info('HTTP Request: %s %s "%s"', "GET", url, "HTTP/1.1 200 OK")

    written = stream.getvalue()
    assert secret not in written
    assert REDACTED in written
    assert "HTTP Request: GET https://" in written


def test_credential_environment_values_are_masked_anywhere(
    captured_logger,
) -> None:
    """Covers: ETL-038, DAG-014 — a key value is masked even outside a URL."""
    logger, stream = captured_logger
    install_log_redaction(
        environ={
            "CENSUS_API_KEY": FAKE_CENSUS_KEY,
            "SOME_NEW_API_KEY": FAKE_EIA_KEY,
            "ELECTIONS_GUESTBOOK_EMAIL": "operator@example.test",
            "POSTGRES_PASSWORD": "airflow",
        }
    )
    try:
        logger.info("posting %s with key %s", "operator@example.test", FAKE_CENSUS_KEY)
        logger.info("new source %s", FAKE_EIA_KEY)
        logger.info("airflow is not a secret here")
    finally:
        install_log_redaction(environ={})

    written = stream.getvalue()
    assert FAKE_CENSUS_KEY not in written
    assert FAKE_EIA_KEY not in written
    assert "operator@example.test" not in written
    assert "airflow is not a secret here" in written


def test_traceback_naming_the_url_is_masked(captured_logger) -> None:
    """Covers: ETL-038, DAG-014 — raise_for_status URLs in tracebacks are masked."""
    logger, stream = captured_logger
    install_log_redaction(environ={})
    try:
        raise RuntimeError(
            "Client error '403 Forbidden' for url "
            f"'https://api.census.gov/data?get=NAME&key={FAKE_CENSUS_KEY}'"
        )
    except RuntimeError:
        logger.exception("request failed")

    written = stream.getvalue()
    assert "403 Forbidden" in written
    assert FAKE_CENSUS_KEY not in written


def test_records_without_secrets_are_left_untouched() -> None:
    """Covers: ETL-038 — redaction does not rewrite ordinary records."""
    install_log_redaction(environ={})
    record = logging.getLogRecordFactory()(
        "x", logging.INFO, __file__, 1, "%s rows for %s", (12, "key metrics"), None
    )
    assert record.msg == "%s rows for %s"
    assert record.args == (12, "key metrics")


def test_install_is_idempotent() -> None:
    """Covers: ETL-038 — repeated installs wrap the record factory once."""
    install_log_redaction(environ={})
    factory = logging.getLogRecordFactory()
    install_log_redaction(environ={})
    assert logging.getLogRecordFactory() is factory
    assert log_redaction.redaction_installed()


def test_short_or_unlisted_environment_values_are_not_masked() -> None:
    """Covers: ETL-038 — only credential variables are masked by value."""
    values = secret_environment_values(
        {"CENSUS_API_KEY": "abc", "HOME": "/home/someone", "BLS_API_KEY": " x123 "}
    )
    assert values == ("x123",)


def test_quoted_parameters_are_masked() -> None:
    """Covers: ETL-038 — a logged params dict carries no key value."""
    text = redact_secrets(
        f"params={{'get': 'NAME', 'key': '{FAKE_CENSUS_KEY}'}}", values=()
    )
    assert FAKE_CENSUS_KEY not in text
    assert "'get': 'NAME'" in text


def test_ledger_error_messages_mask_query_keys() -> None:
    """Covers: DAG-014, RES-002 — a persisted HTTP error keeps no Census key."""
    message = sanitize_error_message(
        "Client error '400 Bad Request' for url "
        f"'https://api.census.gov/data?get=NAME&key={FAKE_CENSUS_KEY}'"
    )
    assert FAKE_CENSUS_KEY not in message
    assert "400 Bad Request" in message


def test_scrub_rewrites_only_files_carrying_secrets(tmp_path) -> None:
    """Covers: ETL-038 — already-written task logs can be scrubbed in place."""
    from data_ingestion_toolbox.log_redaction import scrub_log_file

    install_log_redaction(environ={})
    leaked = tmp_path / "attempt=1.log"
    leaked.write_text(
        "[2026-11-07] INFO - HTTP Request: GET "
        f"https://api.census.gov/data?get=NAME&key={FAKE_CENSUS_KEY} 200\n"
    )
    clean = tmp_path / "clean.log"
    clean.write_text("[2026-11-07] INFO - 12 rows\n")
    clean_mtime = clean.stat().st_mtime_ns

    assert scrub_log_file(leaked, apply=False)
    assert FAKE_CENSUS_KEY in leaked.read_text()  # report-only leaves it
    assert scrub_log_file(leaked, apply=True)
    assert FAKE_CENSUS_KEY not in leaked.read_text()
    assert "HTTP Request: GET https://api.census.gov" in leaked.read_text()
    assert not scrub_log_file(clean, apply=True)
    assert clean.stat().st_mtime_ns == clean_mtime
    assert sorted(p.name for p in tmp_path.iterdir()) == ["attempt=1.log", "clean.log"]
