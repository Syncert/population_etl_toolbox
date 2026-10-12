"""Every configured BLS program reaches silver.

Covers: ETL-078

The DAG ingests every program in ``CONFIG.programs`` but expands the silver
transform over its own list. When average prices (``ap``) joined the config,
that list kept the five older programs, so the price rows reached
``silver_bls.observation_revision`` and stopped there: no silver fact, no gold
series, no metric in the catalog. The wiring is read statically, from the DAG
source, because the DAG tier needs Airflow installed.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from data_ingestion_toolbox.bls.config import CONFIG

pytestmark = pytest.mark.unit

DAG_FILE = Path(__file__).resolve().parents[3] / "dags" / "bls_ingest_dag.py"


def _transform_expansion() -> ast.expr:
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "expand"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "transform_to_silver_by_program"
        ):
            for keyword in node.keywords:
                if keyword.arg == "program":
                    return keyword.value
    raise AssertionError(
        "bls_ingest_dag.py no longer expands the silver transform by program"
    )


def _names_config_programs(node: ast.expr) -> bool:
    return any(
        isinstance(child, ast.Attribute)
        and child.attr == "programs"
        and isinstance(child.value, ast.Name)
        and child.value.id == "CONFIG"
        for child in ast.walk(node)
    )


def test_silver_transform_expands_over_every_configured_program() -> None:
    """Covers: ETL-078 — the transform list is the configured programs, `ap` included."""
    expansion = _transform_expansion()
    if isinstance(expansion, ast.List):
        listed = {ast.literal_eval(item) for item in expansion.elts}
        assert listed == set(CONFIG.programs), (
            "the silver transform's own program list drifted from CONFIG.programs: "
            f"missing {sorted(set(CONFIG.programs) - listed)}"
        )
    else:
        assert _names_config_programs(expansion), ast.unparse(expansion)


def test_average_prices_are_a_configured_program() -> None:
    """Covers: ETL-078 — average prices are configured, so the expansion carries them."""
    assert "ap" in CONFIG.programs
