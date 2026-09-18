"""Every source applies its own DDL before it writes.

Covers: DAG-021 -- the three capture-first sources added after the original
        four had no `ensure_*` task. Their relations lived only in
        `sql/migrations/010`, `011` and `012`, which a bootstrap applies once
        and never again, so a DAG revision that assumed a later step -- the
        FBI `derived` confidence class, say -- failed at insert time against a
        warehouse that had not received it, while the four older sources
        repaired themselves.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from data_ingestion_toolbox.cdc import schema as cdc_schema
from data_ingestion_toolbox.fbi_ucr import schema as fbi_schema
from data_ingestion_toolbox.usda_nass import schema as nass_schema
from data_ingestion_toolbox.utility import gold_schema

pytestmark = pytest.mark.dag

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: (dag id, task id, capture task prefix, module, source code).
SOURCES = [
    ("cdc_ingest", "ensure_cdc_schema", "ingest_batch_", cdc_schema, "CDC"),
    ("fbi_ucr_ingest", "ensure_fbi_schema", "ingest_batch_", fbi_schema, "FBI_UCR"),
    (
        "usda_nass_crop_ingest",
        "ensure_nass_schema",
        "ingest_batch_",
        nass_schema,
        "USDA_NASS",
    ),
]


@pytest.mark.parametrize(
    ("dag_id", "task_id", "capture_prefix", "module", "source_code"),
    SOURCES,
    ids=[dag_id for dag_id, *_ in SOURCES],
)
def test_the_schema_task_runs_before_every_capture(
    dagbag, dag_id: str, task_id: str, capture_prefix: str, module, source_code: str
) -> None:
    """Covers: DAG-021 — the DDL is applied upstream of the first write.

    Upstream of *every* capture rather than merely present: a task that exists
    but hangs off the side of the graph applies nothing before the insert that
    needs it.
    """
    dag = dagbag.dags[dag_id]
    assert task_id in dag.task_ids, (
        f"{dag_id} has no {task_id} task, so nothing re-applies the relations "
        f"the DAG writes to"
    )

    captures = [t for t in dag.tasks if t.task_id.startswith(capture_prefix)]
    assert captures, f"{dag_id} declares no capture task; this proved nothing"
    for capture in captures:
        upstream = {task.task_id for task in capture.get_flat_relatives(upstream=True)}
        assert task_id in upstream, (
            f"{dag_id}.{capture.task_id} can run before {task_id}, so it can "
            f"write to a relation the code has moved past"
        )


@pytest.mark.parametrize(
    ("dag_id", "task_id", "capture_prefix", "module", "source_code"),
    SOURCES,
    ids=[dag_id for dag_id, *_ in SOURCES],
)
def test_the_schema_task_applies_the_source_owned_ddl(
    dagbag, dag_id: str, task_id: str, capture_prefix: str, module, source_code: str
) -> None:
    """Covers: DAG-021, DB-054 — it applies files the source package owns.

    The point of the task is that the DDL is a file under `src/` rather than a
    migration, so this checks where the files are, not only that some exist.
    """
    package = Path(inspect.getfile(module)).resolve().parent
    assert module.DDL_FILES, f"{dag_id} names no DDL file"
    for ddl_file in module.DDL_FILES:
        assert ddl_file.is_file(), f"{ddl_file} does not exist"
        assert ddl_file.parent.name == "DDL", (
            f"{ddl_file} is not in a DDL directory, so it is not where "
            f"`ADDING_A_DATA_SOURCE.md` says a source's relations live"
        )
        assert package in ddl_file.parents, (
            f"{ddl_file} is outside {package}, so this source does not own it"
        )

    assert (
        module.SOURCE_SCHEMA_COMPONENTS[source_code]
        in gold_schema.SOURCE_SCHEMA_COMPONENTS.values()
    )


@pytest.mark.parametrize(
    ("dag_id", "task_id", "capture_prefix", "module", "source_code"),
    SOURCES,
    ids=[dag_id for dag_id, *_ in SOURCES],
)
def test_the_silver_ddl_is_applied_before_the_views_that_read_it(
    dagbag, dag_id: str, task_id: str, capture_prefix: str, module, source_code: str
) -> None:
    """Covers: DAG-021 — the applied order is silver, then gold, then publisher.

    `ensure_gold_schema_from_files` applies `sorted(ddl_files)`, not the list
    it is handed, and the order matters: `CREATE VIEW` resolves its body at
    definition time, so a gold view applied before its silver table fails
    outright on a fresh warehouse. Today the sort happens to agree with the
    declared order -- `DDL/` sorts before `gold_*/` on both a case-sensitive
    and a case-insensitive filesystem -- which is a coincidence worth pinning
    rather than relying on.
    """
    ordered = [path.name for path in sorted(module.DDL_FILES)]
    assert ordered == [path.name for path in module.DDL_FILES], (
        f"{dag_id} declares its DDL in an order the applier does not use: "
        f"declared {[p.name for p in module.DDL_FILES]}, applied {ordered}"
    )
    assert ordered[0].startswith("silver_"), (
        f"{dag_id} applies {ordered[0]} first; the silver tables the views "
        f"read have to exist before the views are defined"
    )
    assert ordered[-1] == "publisher.sql", (
        f"{dag_id} applies {ordered[-1]} last rather than the publisher "
        f"contract, which reads the gold views"
    )


def test_every_source_with_a_gold_package_records_a_schema_component() -> None:
    """Covers: DB-054 — seven sources, one declaration of what each records.

    The four original sources declared a component here and the three added
    later did not, which is the same gap one layer up from the DDL itself:
    nothing recorded what those sources had applied, so nothing could tell a
    stale warehouse from a current one.
    """
    toolbox = REPOSITORY_ROOT / "src/data_ingestion_toolbox"
    packages = {gold.parent.name for gold in toolbox.glob("*/gold_*") if gold.is_dir()}
    assert len(gold_schema.SOURCE_SCHEMA_COMPONENTS) == len(packages), (
        f"{len(packages)} source packages publish gold, but "
        f"{len(gold_schema.SOURCE_SCHEMA_COMPONENTS)} declare a schema "
        f"component: {sorted(packages)}"
    )
    components = gold_schema.SOURCE_SCHEMA_COMPONENTS.values()
    assert len(set(components)) == len(list(components)), (
        "two sources record their DDL under one component name, so each would "
        "see the other's hash and re-apply its own DDL every run"
    )
