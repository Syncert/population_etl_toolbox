"""Python package artifact-content contracts."""

from __future__ import annotations

import pytest

from tests.support.package_artifacts import validate_artifact_names

pytestmark = pytest.mark.unit


def test_package_artifact_contract_accepts_only_runtime_assets() -> None:
    """Covers: ENV-005 — wheel contract requires API, ETL, and runtime SQL."""
    names = [
        "apps/api/main.py",
        "data_ingestion_toolbox/utility/retry.py",
        *[
            f"data_ingestion_toolbox/source_{index}/DDL/runtime.sql"
            for index in range(10)
        ],
    ]
    validate_artifact_names(names, wheel=True)


@pytest.mark.parametrize(
    "leaked", ["apps/web/page.py", "tests/test_app.py", "x/node_modules/y.py"]
)
def test_package_artifact_contract_rejects_non_runtime_files(leaked: str) -> None:
    """Covers: ENV-005 — frontend dependencies and tests cannot leak into artifacts."""
    with pytest.raises(ValueError, match="non-runtime files leaked"):
        validate_artifact_names([leaked], wheel=False)
