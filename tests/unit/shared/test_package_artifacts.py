"""Python package artifact-content contracts."""

from __future__ import annotations

import pytest

from tests.support.package_artifacts import _manifest, validate_artifact_names

pytestmark = pytest.mark.unit


def test_package_artifact_contract_accepts_only_runtime_assets() -> None:
    """Covers: ENV-005 — wheel contract requires API, ETL, and runtime SQL."""
    names = [
        "apps/api/main.py",
        "data_ingestion_toolbox/utility/retry.py",
        *list(_manifest()["runtime_sql"]),
    ]
    validate_artifact_names(names, wheel=True)


@pytest.mark.parametrize(
    "leaked", ["apps/web/page.py", "tests/test_app.py", "x/node_modules/y.py"]
)
def test_package_artifact_contract_rejects_non_runtime_files(leaked: str) -> None:
    """Covers: ENV-005 — frontend dependencies and tests cannot leak into artifacts."""
    with pytest.raises(ValueError, match="non-runtime files leaked"):
        validate_artifact_names([leaked], wheel=False)


def test_package_artifact_contract_rejects_manifest_drift() -> None:
    """Covers: ENV-005 — undeclared runtime SQL cannot enter a distribution."""
    names = [
        "apps/api/main.py",
        "data_ingestion_toolbox/utility/retry.py",
        *list(_manifest()["runtime_sql"]),
        "data_ingestion_toolbox/unknown/DDL/unreviewed.sql",
    ]
    with pytest.raises(ValueError, match="runtime SQL manifest mismatch"):
        validate_artifact_names(names, wheel=True)
