"""Validate that built Python distributions contain only runtime-owned assets."""

from __future__ import annotations

import argparse
import json
import tarfile
import zipfile
from pathlib import Path

MANIFEST_PATH = Path(__file__).with_name("package_manifest.json")


def _manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _runtime_sql(names: list[str]) -> set[str]:
    normalized = [name.replace("\\", "/") for name in names]
    return {
        name[name.index("data_ingestion_toolbox/") :]
        for name in normalized
        if "data_ingestion_toolbox/" in name and name.endswith(".sql")
    }


def validate_artifact_names(names: list[str], *, wheel: bool) -> None:
    """Reject leaked test/frontend files and require packaged runtime assets."""
    normalized = [name.replace("\\", "/") for name in names]
    forbidden = [
        name
        for name in normalized
        if "/node_modules/" in f"/{name}/"
        or "/tests/" in f"/{name}/"
        or "/apps/web/" in f"/{name}/"
    ]
    if forbidden:
        raise ValueError(f"non-runtime files leaked into distribution: {forbidden}")

    if not wheel:
        return
    manifest = _manifest()
    missing = [
        suffix
        for suffix in manifest["required_wheel_modules"]
        if not any(name.endswith(suffix) for name in normalized)
    ]
    if missing:
        raise ValueError(f"wheel is missing runtime modules: {missing}")
    expected_sql = set(manifest["runtime_sql"])
    actual_sql = _runtime_sql(normalized)
    missing_sql = sorted(expected_sql - actual_sql)
    unexpected_sql = sorted(actual_sql - expected_sql)
    if missing_sql or unexpected_sql:
        raise ValueError(
            "runtime SQL manifest mismatch: "
            f"missing={missing_sql}, unexpected={unexpected_sql}"
        )


def validate_distributions(wheel_path: Path, sdist_path: Path) -> None:
    """Inspect a wheel and source distribution without extracting either one."""
    with zipfile.ZipFile(wheel_path) as wheel_archive:
        wheel_names = wheel_archive.namelist()
        validate_artifact_names(wheel_names, wheel=True)
    with tarfile.open(sdist_path) as sdist_archive:
        sdist_names = sdist_archive.getnames()
        validate_artifact_names(sdist_names, wheel=False)

    if _runtime_sql(wheel_names) != _runtime_sql(sdist_names):
        raise ValueError("wheel and sdist runtime SQL assets differ")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--sdist", type=Path, required=True)
    arguments = parser.parse_args()
    validate_distributions(arguments.wheel, arguments.sdist)


if __name__ == "__main__":
    main()
