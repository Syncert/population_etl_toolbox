"""Render the reviewed catalog-to-behavior evidence register."""

from __future__ import annotations

import argparse
import ast
import re
from collections import defaultdict
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CATALOG_PATTERN = re.compile(r"[A-Z][A-Z0-9]*-\d{3}")
AUDITED_COUNTS = {
    "ENV": 11,
    "ARC": 3,
    "PLAN": 7,
    "DAG": 17,
    "ETL": 42,
    "DB": 23,
    "API": 30,
    "WEB": 8,
    "DEPLOY": 5,
    "MARTIN": 10,
    "EXT": 14,
    "E2E": 7,
    "PERF": 10,
    "RES": 8,
}
EXECUTION_PROFILES = {
    "ENV": ("make test-unit / package build", "lint, package-api, coverage"),
    "ARC": ("make test-etl", "etl-unit"),
    "PLAN": ("make test-unit", "coverage"),
    "DAG": ("make test-dags", "dag-parse, scheduler-image"),
    "ETL": (
        "make test-etl / test-integration",
        "etl-unit, postgres-integration",
    ),
    "DB": ("make test-integration", "postgres-integration"),
    "API": (
        "make test-api / test-integration",
        "api-unit, redis-integration, e2e-performance",
    ),
    "WEB": (
        "make test-web-unit / test-web-browser / test-web-build",
        "frontend",
    ),
    "DEPLOY": ("make test-compose-smoke", "deployment-smoke"),
    "MARTIN": (
        "make test-martin-unit / test-martin-integration",
        "martin-unit, martin-integration",
    ),
    "EXT": ("make test-external", "external-contract"),
    "E2E": (
        "make test-e2e / test-martin-integration",
        "e2e-performance, martin-integration",
    ),
    "PERF": ("make test-performance", "coverage, e2e-performance"),
    "RES": ("make test-resilience", "e2e-performance"),
}


def _catalog_rows() -> dict[str, tuple[str, str]]:
    plan = (REPOSITORY_ROOT / "docs/reference/TESTING_CONTRACT.md").read_text(
        encoding="utf-8"
    )
    rows: dict[str, tuple[str, str]] = {}
    for line in plan.splitlines():
        columns = [
            column.strip().replace("\\|", "|")
            for column in re.split(r"(?<!\\)\|", line.strip().strip("|"))
        ]
        if len(columns) != 6 or not CATALOG_PATTERN.fullmatch(columns[0]):
            continue
        rows[columns[0]] = (columns[3], columns[4])
    return rows


def _python_nodes() -> dict[str, list[str]]:
    evidence: dict[str, list[str]] = defaultdict(list)
    for path in sorted((REPOSITORY_ROOT / "tests").rglob("test_*.py")):
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith("test_"):
                continue
            docstring = ast.get_docstring(node, clean=True) or ""
            for catalog_id in CATALOG_PATTERN.findall(docstring):
                evidence[catalog_id].append(f"{relative}::{node.name}")
    return evidence


def _frontend_nodes(evidence: dict[str, list[str]]) -> None:
    for path in sorted((REPOSITORY_ROOT / "tests/frontend").rglob("*")):
        if path.suffix not in {".js", ".jsx"}:
            continue
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        names = re.findall(r"(?:test|it)\(\s*[\"']([^\"']+)", source)
        nodes = [f"{relative}::{name}" for name in names]
        for catalog_id in set(CATALOG_PATTERN.findall(source)):
            evidence[catalog_id].extend(nodes)


def _configuration_references(evidence: dict[str, list[str]]) -> None:
    paths = [*sorted((REPOSITORY_ROOT / ".github/workflows").glob("*.yml"))]
    paths.extend([REPOSITORY_ROOT / "pyproject.toml", REPOSITORY_ROOT / "Makefile"])
    for path in paths:
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        for line_number, line in enumerate(source.splitlines(), start=1):
            for catalog_id in CATALOG_PATTERN.findall(line):
                evidence[catalog_id].append(f"{relative}:{line_number}")


def build_evidence_rows() -> list[tuple[str, str, str, str, str, str]]:
    """Build one exact reviewed evidence row for every catalog ID."""
    catalog = _catalog_rows()
    evidence = _python_nodes()
    _frontend_nodes(evidence)
    _configuration_references(evidence)
    rows = []
    for catalog_id, (test_name, pass_metric) in catalog.items():
        prefix, number = catalog_id.split("-")
        audited = int(number) <= AUDITED_COUNTS.get(prefix, 0)
        local_runner, ci_jobs = EXECUTION_PROFILES[prefix]
        exact_nodes = "<br>".join(dict.fromkeys(evidence[catalog_id]))
        rows.append(
            (
                catalog_id,
                f"{test_name}: {pass_metric}",
                exact_nodes,
                local_runner,
                ci_jobs,
                "FULL" if audited else "PARTIAL",
            )
        )
    return rows


def render_markdown() -> str:
    """Return the complete human-reviewable Markdown evidence table."""
    heading = (
        "| ID | Production behavior / public contract | Exact evidence nodes | "
        "Local runner | CI owner | Audit |\n"
        "|---|---|---|---|---|---|"
    )
    lines = [heading]
    for row in build_evidence_rows():
        escaped = [value.replace("|", "\\|") for value in row]
        lines.append("| " + " | ".join(escaped) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    report = render_markdown()
    if arguments.output:
        arguments.output.write_text(report, encoding="utf-8")
    else:
        print(report, end="")


if __name__ == "__main__":
    main()
