"""
Unit tests for Census PEP ingest module.

Covers:
- URL construction for PEP API endpoints
- HTTP retry logic with SequencedHttpClient
- Capture orchestration with mocked HTTP and DB
- ingest_census_pep entry point with full chain
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

from data_ingestion_toolbox.census_pep import ingest
from data_ingestion_toolbox.census_pep.config import CONFIG
from tests.support.http import SequencedHttpClient, response

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# URL construction tests
# ---------------------------------------------------------------------------


class TestBuildUrls:
    """Verify _build_urls generates correct Census PEP API endpoint URLs."""

    def test_default_years_and_file_types(self) -> None:
        """Default range produces 7 years × 2 file_types = 14 URLs.

        Covers: PEP-001.1
        """
        urls = ingest._build_urls()
        assert len(urls) == 14
        # First URL should be 2020/pep/ansfile.json
        assert urls[0] == "https://api.census.gov/data/2020/pep/ansfile.json"
        # Last URL should be 2026/pep/intlfile.json
        assert urls[-1] == "https://api.census.gov/data/2026/pep/intlfile.json"

    def test_custom_years_single_file_type(self) -> None:
        """Custom year range and file types produce expected count."""
        urls = ingest._build_urls(years=range(2020, 2023), file_types=("ansfile",))
        assert len(urls) == 3
        assert urls[0] == "https://api.census.gov/data/2020/pep/ansfile.json"
        assert urls[2] == "https://api.census.gov/data/2022/pep/ansfile.json"

    def test_custom_years_both_file_types(self) -> None:
        """Custom years with both file types."""
        urls = ingest._build_urls(years=range(2023, 2025), file_types=("ansfile", "intlfile"))
        assert len(urls) == 4
        # Year ordering: all file types for 2023, then 2024
        assert "2023" in urls[0]
        assert "2024" in urls[2]

    def test_international_file_type(self) -> None:
        """intlfile URLs are constructed correctly."""
        urls = ingest._build_urls(years=range(2024, 2025), file_types=("intlfile",))
        assert urls[0] == "https://api.census.gov/data/2024/pep/intlfile.json"


# ---------------------------------------------------------------------------
# HTTP retry tests
# ---------------------------------------------------------------------------


class TestFetchWithRetry:
    """Verify _fetch_with_retry retry behavior and error handling."""

    def test_success_no_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful first call returns content immediately."""
        expected = b'{"data":[1,2,3]}'
        client = SequencedHttpClient([response(200, json.loads(expected))])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)

        result = ingest._fetch_with_retry("https://test.example.com/api", max_retries=3)
        assert result == expected
        assert client.calls == 1

    def test_retry_on_503(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Transient 503 triggers retry; success on second attempt."""
        client = SequencedHttpClient([
            response(503),
            response(200, {"data": [1]}),
        ])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        result = ingest._fetch_with_retry("https://test.example.com/api", max_retries=3, base_delay=0.01)
        assert result == b'{"data":[1]}'
        assert client.calls == 2

    def test_retry_on_429(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """429 rate limit triggers retry with exponential backoff."""
        client = SequencedHttpClient([
            response(429),
            response(200, {"data": [1]}),
        ])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        result = ingest._fetch_with_retry("https://test.example.com/api", max_retries=3, base_delay=0.01)
        assert client.calls == 2

    def test_exhausted_retries_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """All retries exhausted raises RuntimeError."""
        client = SequencedHttpClient([response(503), response(503), response(503)])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        with pytest.raises(RuntimeError, match="Failed to fetch"):
            ingest._fetch_with_retry("https://test.example.com/api", max_retries=3, base_delay=0.01)
        assert client.calls == 3

    def test_transport_error_triggers_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Network error triggers retry; success on second attempt."""
        import httpx

        client = SequencedHttpClient([
            httpx.RequestError("connection refused", request=MagicMock()),
            response(200, {"data": [1]}),
        ])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        result = ingest._fetch_with_retry("https://test.example.com/api", max_retries=3, base_delay=0.01)
        assert client.calls == 2

    def test_http_status_error_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Terminal HTTP error (404) raises after exhausting retries."""
        import httpx
        client = SequencedHttpClient([
            httpx.HTTPStatusError(
                "Not Found",
                request=MagicMock(),
                response=httpx.Response(404),
            ),
            httpx.HTTPStatusError(
                "Not Found",
                request=MagicMock(),
                response=httpx.Response(404),
            ),
            httpx.HTTPStatusError(
                "Not Found",
                request=MagicMock(),
                response=httpx.Response(404),
            ),
        ])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        with pytest.raises(RuntimeError, match="Failed to fetch"):
            ingest._fetch_with_retry("https://test.example.com/api", max_retries=3, base_delay=0.01)


# ---------------------------------------------------------------------------
# Capture orchestration tests
# ---------------------------------------------------------------------------


class TestIngestUrl:
    """Verify _ingest_url captures payloads and persists to raw_capture."""

    @patch("data_ingestion_toolbox.census_pep.ingest.persist_response_capture")
    @patch("data_ingestion_toolbox.census_pep.ingest._fetch_with_retry")
    def test_successful_capture(self, mock_fetch, mock_persist, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful fetch returns a CaptureReceipt with checksum."""
        mock_fetch.return_value = b'[{"name": "United States", "value": 331893745}]'
        mock_persist.return_value = MagicMock(payload_checksum="abc123")

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=None)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=None)

        # Mock CaptureControl to capture the control object
        captured_control = []
        original_start_request = MagicMock(return_value=MagicMock(request_id="req-001"))
        original_finish_request = MagicMock()

        def mock_start_run(*a, **k):
            pass

        def mock_start_request_patch(*a, **k):
            return MagicMock(request_id="req-001")

        def mock_finish_request_patch(*a, **k):
            pass

        monkeypatch.setattr(ingest.CaptureControl, "__init__", lambda self, *a, **k: None)
        monkeypatch.setattr(ingest.CaptureControl, "start_request", mock_start_request_patch)
        monkeypatch.setattr(ingest.CaptureControl, "finish_request", mock_finish_request_patch)
        monkeypatch.setattr(ingest.CaptureControl, "start_run", mock_start_run)
        monkeypatch.setattr(ingest.CaptureControl, "finish_run", mock_start_run)

        receipt = ingest._ingest_url(
            mock_hook,
            "https://api.census.gov/data/2023/pep/ansfile.json",
            uuid.uuid4(),
        )
        assert receipt.payload_checksum == "abc123"
        mock_fetch.assert_called_once()

    @patch("data_ingestion_toolbox.census_pep.ingest._fetch_with_retry")
    def test_failed_fetch_raises(self, mock_fetch, monkeypatch: pytest.MonkeyPatch) -> None:
        """Failed fetch raises RuntimeError."""
        mock_fetch.side_effect = RuntimeError("Network error")

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=None)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=None)

        monkeypatch.setattr(ingest.CaptureControl, "__init__", lambda self, *a, **k: None)
        monkeypatch.setattr(ingest.CaptureControl, "start_request", lambda *a, **k: MagicMock(request_id="req-001"))
        monkeypatch.setattr(ingest.CaptureControl, "finish_request", lambda *a, **k: None)
        monkeypatch.setattr(ingest.CaptureControl, "start_run", lambda *a, **k: None)
        monkeypatch.setattr(ingest.CaptureControl, "finish_run", lambda *a, **k: None)

        with pytest.raises(RuntimeError, match="Capture failed"):
            ingest._ingest_url(
                mock_hook,
                "https://api.census.gov/data/2023/pep/ansfile.json",
                uuid.uuid4(),
            )


# ---------------------------------------------------------------------------
# Public entry point tests
# ---------------------------------------------------------------------------


class TestIngestCensusPep:
    """Verify ingest_census_pep orchestrates the full capture chain."""

    @patch("data_ingestion_toolbox.census_pep.ingest._build_urls")
    @patch("data_ingestion_toolbox.census_pep.ingest._ingest_url")
    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_all_urls_captured(self, mock_ctrl, mock_get_hook, mock_ingest_url, mock_build_urls) -> None:
        """All generated URLs are fetched and counted."""
        mock_build_urls.return_value = [
            "https://api.census.gov/data/2023/pep/ansfile.json",
            "https://api.census.gov/data/2023/pep/intlfile.json",
        ]
        mock_ingest_url.return_value = MagicMock(payload_checksum="abc123")
        mock_hook = MagicMock()
        mock_get_hook.return_value = mock_hook
        mock_ctrl_instance = MagicMock()
        mock_ctrl.return_value = mock_ctrl_instance

        result = ingest.ingest_census_pep(years=range(2023, 2024), file_types=("ansfile", "intlfile"))
        assert result == 2
        assert mock_ingest_url.call_count == 2
        mock_ctrl_instance.start_run.assert_called_once()
        mock_ctrl_instance.finish_run.assert_called_once()

    @patch("data_ingestion_toolbox.census_pep.ingest._build_urls")
    @patch("data_ingestion_toolbox.census_pep.ingest._ingest_url")
    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_partial_failure_counts_only_success(self, mock_ctrl, mock_get_hook, mock_ingest_url, mock_build_urls) -> None:
        """Failed URLs are logged but don't stop ingestion; only successful counts."""
        mock_build_urls.return_value = [
            "https://api.census.gov/data/2023/pep/ansfile.json",
            "https://api.census.gov/data/2023/pep/intlfile.json",
        ]
        mock_ingest_url.side_effect = [
            MagicMock(payload_checksum="abc123"),
            RuntimeError("Network error"),
        ]
        mock_hook = MagicMock()
        mock_get_hook.return_value = mock_hook
        mock_ctrl_instance = MagicMock()
        mock_ctrl.return_value = mock_ctrl_instance

        result = ingest.ingest_census_pep(years=range(2023, 2024), file_types=("ansfile", "intlfile"))
        assert result == 1
        mock_ctrl_instance.finish_run.assert_called_once()


# ---------------------------------------------------------------------------
# get_pep_api_columns test
# ---------------------------------------------------------------------------


class TestGetPepApiColumns:
    """Verify get_pep_api_columns returns column metadata."""

    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    def test_returns_polars_dataframe(self, mock_get_hook) -> None:
        """Returns a Polars DataFrame with expected columns."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("total", "Total population", "demographics", "Total", "integer", True, False),
            ("under5", "Under 5 years", "demographics", "Total", "integer", True, False),
        ]
        mock_cursor.description = [
            ("variable_code",), ("variable_label",), ("concept",), ("universe",),
            ("data_type",), ("is_numeric",), ("is_geometry",),
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

        mock_hook = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=None)
        mock_get_hook.return_value = mock_hook

        import polars as pl
        df = ingest.get_pep_api_columns()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert "variable_code" in df.columns
        assert "is_numeric" in df.columns
