#!/usr/bin/env python3
"""Validate README.md alignment with current repository conventions."""

from __future__ import annotations

import pathlib


REQUIRED_FILES = [
    "dags/acs_ingest_dag.py",
    "dags/bls_ingest_dag.py",
    "dags/fred_ingest_dag.py",
    "dags/silver_ref_dag.py",
]

DEPRECATED_STRINGS = [
    "acs_raw_ingest_dag",
    "bls_raw_ingest_dag",
    "fred_raw_ingest_dag",
    "CONFIG.geographies",
    "CONFIG.curated_variables",
]

REQUIRED_STRINGS = [
    "acs_ingest_dag.py",
    "bls_ingest_dag.py",
    "fred_ingest_dag.py",
    "documentation/CONFIGURATION.md",
]


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    readme_path = root / "README.md"

    failures: list[str] = []
    passes: list[str] = []

    if not readme_path.exists():
        print("FAIL: README.md not found")
        return 1

    readme_text = readme_path.read_text(encoding="utf-8")

    for rel in REQUIRED_FILES:
        target = root / rel
        if target.exists():
            passes.append(f"Required file exists: {rel}")
        else:
            failures.append(f"Missing required file: {rel}")

    for token in DEPRECATED_STRINGS:
        if token in readme_text:
            failures.append(f"Deprecated string found in README.md: {token}")
        else:
            passes.append(f"Deprecated string absent: {token}")

    for token in REQUIRED_STRINGS:
        if token in readme_text:
            passes.append(f"Required string found in README.md: {token}")
        else:
            failures.append(f"Missing required string in README.md: {token}")

    last_updated_lines = [
        line.strip() for line in readme_text.splitlines() if "Last Updated:" in line
    ]

    if not last_updated_lines:
        failures.append("README.md missing 'Last Updated:' line")
    else:
        if any("2026" in line for line in last_updated_lines):
            passes.append("README.md Last Updated line includes year 2026")
        else:
            failures.append("README.md Last Updated line does not include year 2026")

    print("README alignment check")
    print("-" * 80)
    for item in passes:
        print(f"PASS: {item}")

    if failures:
        print("-" * 80)
        for item in failures:
            print(f"FAIL: {item}")
        print("-" * 80)
        print(f"Summary: {len(passes)} passed, {len(failures)} failed")
        return 1

    print("-" * 80)
    print(f"Summary: {len(passes)} passed, 0 failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
