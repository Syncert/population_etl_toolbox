"""Unit contracts for Census PEP bulk-release capture."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import httpx
import pytest

from data_ingestion_toolbox.capture import ControlRequest
from data_ingestion_toolbox.census_pep import ingest
from data_ingestion_toolbox.census_pep.config import CONFIG
from tests.support.http import SequencedHttpClient, response

pytestmark = pytest.mark.unit


class TestSelectReleases:
    def test_default_scope_is_each_current_published_product(self) -> None:
        """Covers: ETL-030 — Default PEP scope selects published releases."""
        releases = ingest._select_releases()

        assert [release.dataset_code for release in releases] == [
            "pep_county_alldata",
            "pep_nst_alldata",
            "pep_subcounty",
        ]
        assert {release.vintage_year for release in releases} == {2025}
        assert {release.status for release in releases} == {"published"}

    def test_explicit_vintage_selects_archived_release(self) -> None:
        """Covers: ETL-030 — Explicit PEP vintage supports replay/backfill."""
        releases = ingest._select_releases(
            dataset_codes=("pep_nst_alldata",),
            vintage_years=(2024,),
        )

        assert len(releases) == 1
        assert releases[0].product_code == "NST-EST2024-ALLDATA"
        assert releases[0].status == "archived"

    def test_unknown_dataset_is_rejected_before_database_work(self) -> None:
        """Covers: ETL-030 — Unknown PEP products fail configuration validation."""
        with pytest.raises(ValueError, match="unknown PEP dataset"):
            ingest._select_releases(dataset_codes=("not_a_product",))

    def test_unregistered_vintage_is_rejected(self) -> None:
        """Covers: ETL-030 — Unregistered PEP vintages cannot be requested."""
        with pytest.raises(ValueError, match="no registered PEP releases"):
            ingest._select_releases(vintage_years=(2023,))


class TestFetchWithRetry:
    def test_success_preserves_response_envelope(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ARC-002 — Successful fetch retains payload and HTTP metadata."""
        source_response = httpx.Response(
            200,
            request=httpx.Request("GET", "https://source.example.test/data.csv"),
            content=b"SUMLEV,POPESTIMATE2025\n040,100\n",
            headers={"content-type": "text/csv", "etag": '"revision-1"'},
        )
        client = SequencedHttpClient([source_response])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)

        result = ingest._fetch_with_retry("https://source.example.test/data.csv")

        assert result.payload == b"SUMLEV,POPESTIMATE2025\n040,100\n"
        assert result.status_code == 200
        assert result.response_headers["content-type"] == "text/csv"
        assert result.response_headers["etag"] == '"revision-1"'
        assert client.calls == 1

    @pytest.mark.parametrize("status_code", [429, 500, 502, 503])
    def test_retryable_status_then_success(
        self,
        status_code: int,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ETL-020 — Retryable Census status receives another attempt."""
        client = SequencedHttpClient(
            [response(status_code), response(200, {"data": [1]})]
        )
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        result = ingest._fetch_with_retry(
            "https://source.example.test/data.csv",
            base_delay=0.01,
        )

        assert result.status_code == 200
        assert client.calls == 2

    def test_transport_error_then_success(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ETL-020 — Census transport failures are retryable."""
        request = httpx.Request("GET", "https://source.example.test/data.csv")
        client = SequencedHttpClient(
            [httpx.ConnectError("connection refused", request=request), response(200)]
        )
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        ingest._fetch_with_retry("https://source.example.test/data.csv")

        assert client.calls == 2

    def test_nonretryable_404_stops_immediately(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ETL-020 — Permanent Census 4xx responses are not retried."""
        client = SequencedHttpClient([response(404)])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)

        with pytest.raises(RuntimeError, match="non-retryable HTTP 404"):
            ingest._fetch_with_retry("https://source.example.test/missing.csv")

        assert client.calls == 1

    def test_retry_budget_exposes_final_cause(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ETL-021 — Census retries stop at the configured budget."""
        client = SequencedHttpClient([response(503), response(503), response(503)])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)

        with pytest.raises(RuntimeError, match="after 3 attempts") as error:
            ingest._fetch_with_retry("https://source.example.test/data.csv")

        assert isinstance(error.value.__cause__, httpx.HTTPStatusError)
        assert client.calls == 3

    def test_retry_callback_receives_each_retryable_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Covers: ETL-021 — PEP exposes retries to durable control state."""
        client = SequencedHttpClient([response(503), response(200)])
        monkeypatch.setattr(ingest.httpx, "Client", lambda *a, **k: client)
        monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)
        failures: list[Exception] = []

        ingest._fetch_with_retry(
            "https://source.example.test/data.csv",
            on_retry=failures.append,
        )

        assert len(failures) == 1
        assert isinstance(failures[0], httpx.HTTPStatusError)


class TestIngestRelease:
    @patch("data_ingestion_toolbox.census_pep.ingest.persist_response_capture")
    @patch("data_ingestion_toolbox.census_pep.ingest._fetch_with_retry")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_capture_preserves_release_identity_and_envelope(
        self,
        mock_control_class: MagicMock,
        mock_fetch: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """Covers: ARC-002 — PEP raw capture retains release provenance."""
        release = next(
            item
            for item in CONFIG.releases
            if item.dataset_code == "pep_nst_alldata" and item.vintage_year == 2025
        )
        request_id = uuid.uuid4()
        run_id = uuid.uuid4()
        control = mock_control_class.return_value
        control.start_request.return_value = ControlRequest(request_id, "fingerprint")
        mock_fetch.return_value = ingest.PEPHTTPResponse(
            payload=b"SUMLEV,POPESTIMATE2025\n040,100\n",
            status_code=200,
            response_headers={"content-type": "text/csv", "etag": '"revision-1"'},
        )
        mock_persist.return_value = MagicMock(payload_checksum="abc123")
        hook = MagicMock()

        receipt = ingest._ingest_release(hook, release, run_id)

        assert receipt.payload_checksum == "abc123"
        control.start_request.assert_called_once_with(
            run_id=run_id,
            endpoint=release.data_url,
            parameters={
                "dataset_code": "pep_nst_alldata",
                "vintage_year": 2025,
                "product_code": "NST-EST2025-ALLDATA",
            },
            max_attempts=3,
        )
        capture = mock_persist.call_args.args[1]
        assert capture.run_id == run_id
        assert capture.request_id == request_id
        assert capture.payload == mock_fetch.return_value.payload
        assert capture.media_type == "text/csv"
        assert capture.payload_schema_version == "nst-est2025-alldata"
        assert capture.source_revision == "NST-EST2025-ALLDATA"
        assert capture.response_headers["etag"] == '"revision-1"'
        retry_callback = mock_fetch.call_args.kwargs["on_retry"]
        retry_error = RuntimeError("transient failure")
        retry_callback(retry_error)
        control.record_request_retry.assert_called_once_with(
            request_id,
            error=retry_error,
        )
        control.finish_request.assert_called_once_with(request_id, status="success")

    @patch("data_ingestion_toolbox.census_pep.ingest._fetch_with_retry")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_failed_capture_is_never_marked_successful(
        self,
        mock_control_class: MagicMock,
        mock_fetch: MagicMock,
    ) -> None:
        """Covers: ARC-002 — Failed PEP request has one terminal error state."""
        release = CONFIG.releases[1]
        request_id = uuid.uuid4()
        control = mock_control_class.return_value
        control.start_request.return_value = ControlRequest(request_id, "fingerprint")
        mock_fetch.side_effect = RuntimeError("network unavailable")

        with pytest.raises(RuntimeError, match="Capture failed"):
            ingest._ingest_release(MagicMock(), release, uuid.uuid4())

        control.finish_request.assert_called_once()
        assert control.finish_request.call_args.args == (request_id,)
        assert control.finish_request.call_args.kwargs["status"] == "error"
        assert isinstance(
            control.finish_request.call_args.kwargs["error"], RuntimeError
        )


class TestIngestCensusPep:
    @patch("data_ingestion_toolbox.census_pep.ingest._ingest_release")
    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_control_run_id_drives_every_capture(
        self,
        mock_control_class: MagicMock,
        mock_get_hook: MagicMock,
        mock_ingest_release: MagicMock,
    ) -> None:
        """Covers: ARC-002 — PEP captures reference the committed control run."""
        run_id = uuid.uuid4()
        control = mock_control_class.return_value
        control.start_run.return_value = run_id

        captured = ingest.ingest_census_pep(
            dataset_codes=("pep_nst_alldata",),
            vintage_years=(2025,),
        )

        assert captured == 1
        release = mock_ingest_release.call_args.args[1]
        assert release.product_code == "NST-EST2025-ALLDATA"
        assert mock_ingest_release.call_args.args[2] == run_id
        control.finish_run.assert_called_once_with(run_id, status="success")

    @patch("data_ingestion_toolbox.census_pep.ingest._ingest_release")
    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    @patch("data_ingestion_toolbox.census_pep.ingest.CaptureControl")
    def test_partial_failure_marks_run_error_and_raises(
        self,
        mock_control_class: MagicMock,
        mock_get_hook: MagicMock,
        mock_ingest_release: MagicMock,
    ) -> None:
        """Covers: ARC-002 — Partial PEP capture cannot report run success."""
        run_id = uuid.uuid4()
        control = mock_control_class.return_value
        control.start_run.return_value = run_id
        mock_ingest_release.side_effect = [
            MagicMock(payload_checksum="abc123"),
            RuntimeError("network unavailable"),
        ]

        with pytest.raises(RuntimeError, match="1 of 2 PEP releases failed"):
            ingest.ingest_census_pep(
                dataset_codes=("pep_nst_alldata", "pep_subcounty"),
                vintage_years=(2025,),
            )

        control.finish_run.assert_called_once()
        assert control.finish_run.call_args.args == (run_id,)
        assert control.finish_run.call_args.kwargs["status"] == "error"


class TestGetPepApiColumns:
    @patch("data_ingestion_toolbox.census_pep.ingest._get_hook")
    def test_returns_polars_dataframe(self, mock_get_hook: MagicMock) -> None:
        """Covers: ETL-030 — PEP metadata query returns its declared columns."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "total",
                "Total population",
                "demographics",
                "Total",
                "integer",
                True,
                False,
            )
        ]
        mock_cursor.description = [
            ("variable_code",),
            ("variable_label",),
            ("concept",),
            ("universe",),
            ("data_type",),
            ("is_numeric",),
            ("is_geometry",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_hook = MagicMock()
        mock_hook.get_conn.return_value.__enter__.return_value = mock_conn
        mock_get_hook.return_value = mock_hook

        frame = ingest.get_pep_api_columns()

        assert frame.height == 1
        assert frame.columns == [
            "variable_code",
            "variable_label",
            "concept",
            "universe",
            "data_type",
            "is_numeric",
            "is_geometry",
        ]
