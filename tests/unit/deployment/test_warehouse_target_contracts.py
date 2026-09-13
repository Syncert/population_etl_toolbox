"""Ingestion writes a warehouse, and never Airflow's own metadata database.

Every DAG resolves `PostgresHook(postgres_conn_id="public_data")`. That is a
connection *id*, not a database name, and `docker-compose.airflow.yml` read it
as both: it created the connection with `--conn-schema airflow`, so on the
stack the README presents as "DAG orchestration + metadata DB", ingestion
wrote `raw_capture`, `control`, `silver_*`, and `gold_*` into the database
Airflow needs to schedule -- and the documented reset,
`BETA_RESET_REINGESTION.md` section 2, names a database that did not exist
there.

Nothing read where `public_data` pointed. `test_container_contracts.py` parses
the same files for image digests and port bindings and asserts nothing about
the target, so the two databases could be the same one and every tier stayed
green.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest
import yaml

from tests.support.compose_expressions import resolve

pytestmark = [pytest.mark.unit, pytest.mark.deployment]

ROOT = Path(__file__).resolve().parents[3]
COMPOSE_DIRECTORY = ROOT / "infra/docker"
DAG_DIRECTORY = ROOT / "dags"

WAREHOUSE_CONNECTION_ID = "public_data"
METADATA_URL_VARIABLE = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
WAREHOUSE_DATABASE_VARIABLE = "PUBLIC_DATA_DB_NAME"


def _compose_files() -> list[Path]:
    return sorted(COMPOSE_DIRECTORY.glob("docker-compose*.yml"))


def _connection_flags(source: str) -> dict[str, str] | None:
    """The `airflow connections add public_data` flags, as written."""
    marker = f"airflow connections add {WAREHOUSE_CONNECTION_ID} "
    if marker not in source:
        return None
    command = source[source.index(marker) + len(marker) :].split(";", 1)[0]
    tokens = command.split()
    flags = {}
    for position, token in enumerate(tokens):
        if token.startswith("--") and position + 1 < len(tokens):
            flags[token[2:]] = tokens[position + 1]
    return flags


def _metadata_target(compose: dict) -> tuple[str, str]:
    """The (host, database) expressions of Airflow's own metadata database."""
    url = str(
        compose["services"]["airflow-common"]["environment"][METADATA_URL_VARIABLE]
    )
    host, _, database = url.rsplit("@", 1)[-1].partition("/")
    return host.split(":", 1)[0], database


def _stacks_that_create_the_connection() -> dict[str, tuple[dict, dict[str, str]]]:
    found = {}
    for path in _compose_files():
        source = path.read_text(encoding="utf-8")
        flags = _connection_flags(source)
        if flags is not None:
            found[path.name] = (yaml.safe_load(source), flags)
    return found


def test_the_warehouse_connection_never_resolves_to_the_metadata_database() -> None:
    """Covers: DEPLOY-007 — the `public_data` connection is not Airflow's own database.

    Two stacks can collide two ways, and a resolver sees only one of them.
    The internal and Airflow-only stacks default every reference, so resolving
    them says which database each really points at. The external stack
    defaults nothing -- the operator supplies both -- so both sides resolve to
    the empty string, and comparing the results would call every external
    stack a collision. What can be compared there is the *expressions*:
    pointing the warehouse at the same variables as the metadata database is a
    collision whatever the operator sets them to.
    """
    stacks = _stacks_that_create_the_connection()
    assert stacks, (
        "no compose file creates the public_data connection; the rule read nothing"
    )

    collisions = []
    for name, (compose, flags) in stacks.items():
        metadata = _metadata_target(compose)
        warehouse = (flags["conn-host"], flags["conn-schema"])
        resolved_metadata = tuple(resolve(part) for part in metadata)
        resolved_warehouse = tuple(resolve(part) for part in warehouse)
        if all(resolved_metadata) and resolved_metadata == resolved_warehouse:
            collisions.append(
                f"{name} -> {resolved_metadata[0]}/{resolved_metadata[1]}"
            )
        elif metadata == warehouse:
            collisions.append(f"{name} -> both read {metadata[0]}/{metadata[1]}")
    assert not collisions, (
        "these stacks point ingestion at Airflow's own metadata database, so "
        "the warehouse lands inside the database Airflow schedules from and "
        "the documented reset would drop Airflow with it: "
        f"{sorted(collisions)}"
    )


def test_a_stack_names_its_warehouse_once() -> None:
    """Covers: DEPLOY-007 — the connection and the modules name one database.

    The Airflow connection decides where a `PostgresHook` writes, and
    `PUBLIC_DATA_DB_NAME` decides where the modules' own psycopg connections
    write (`utility.db_connection.warehouse_database`). A stack that sets them
    differently runs one DAG against two databases -- which
    `docker-compose.external.yml` did, telling the modules `public_data` while
    the connection resolved to the operator's analytics database.
    """
    disagreements = []
    for name, (compose, flags) in _stacks_that_create_the_connection().items():
        declared = compose["services"]["airflow-common"]["environment"].get(
            WAREHOUSE_DATABASE_VARIABLE
        )
        assert declared is not None, (
            f"{name} creates the {WAREHOUSE_CONNECTION_ID} connection and "
            f"never passes {WAREHOUSE_DATABASE_VARIABLE}, so its modules fall "
            f"back to a default the stack never chose"
        )
        if resolve(str(declared)) != resolve(flags["conn-schema"]):
            disagreements.append(
                f"{name}: {WAREHOUSE_DATABASE_VARIABLE}={resolve(str(declared))!r} "
                f"but the connection's schema is "
                f"{resolve(flags['conn-schema'])!r}"
            )
    assert not disagreements, (
        "these stacks name two warehouses, so a hook write and a psycopg "
        f"write in the same DAG land in different databases: {disagreements}"
    )


def test_the_warehouse_database_is_read_from_the_environment_in_one_place() -> None:
    """Covers: DEPLOY-007 — one definition of which warehouse this process uses.

    Ten modules each carried their own
    `os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")`, read at import
    time, which is ten chances for a deployment to be half retargeted and no
    error when it is -- just a connection to a database that does not exist.
    """
    readers = sorted(
        str(path.relative_to(ROOT))
        for path in (ROOT / "src").rglob("*.py")
        if f'"{WAREHOUSE_DATABASE_VARIABLE}"' in path.read_text(encoding="utf-8")
    )
    assert readers == ["src/data_ingestion_toolbox/utility/db_connection.py"], (
        f"{WAREHOUSE_DATABASE_VARIABLE} is named in more than one module, so "
        f"a retargeted deployment can be half retargeted: {readers}"
    )


def _pools_a_dag_names(path: Path) -> set[str]:
    """The Airflow pools one DAG file asks for, resolved to their names."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants = {
        target.id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name) and isinstance(node.value.value, str)
    }
    # `pool=CONFIG.airflow_pool` reads a source's own configuration, so the
    # module that CONFIG comes from is imported and asked.
    config_modules = {
        alias.asname or alias.name: node.module
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module
        for alias in node.names
    }

    pools = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "pool":
                continue
            value = keyword.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                pools.add(value.value)
            elif isinstance(value, ast.Name) and value.id in constants:
                pools.add(constants[value.id])
            elif isinstance(value, ast.Attribute) and isinstance(value.value, ast.Name):
                module_name = config_modules.get(value.value.id)
                assert module_name, (
                    f"{path.name} names a pool through {value.value.id}, which "
                    f"this rule cannot resolve to a module"
                )
                config = getattr(importlib.import_module(module_name), value.value.id)
                pools.add(getattr(config, value.attr))
            else:
                raise AssertionError(
                    f"{path.name} names a pool in a shape this rule cannot "
                    f"resolve; teach it rather than leaving the pool unchecked"
                )
    return pools


def test_every_pool_a_dag_asks_for_is_created_by_every_stack() -> None:
    """Covers: DEPLOY-007 — a DAG's pool exists on every stack that runs it.

    Airflow refuses to schedule a task whose pool does not exist.
    `docker-compose.external.yml` created five pools and the DAGs name six, so
    the USDA NASS DAG could not run there at all -- and the failure is a
    scheduler message about a missing pool, not anything the ingestion code
    could report.
    """
    required = set()
    for path in sorted(DAG_DIRECTORY.glob("*_dag.py")):
        required |= _pools_a_dag_names(path)
    assert required, "no DAG names a pool; the rule read nothing"

    missing = {}
    for path in _compose_files():
        source = path.read_text(encoding="utf-8")
        marker = "airflow pools set "
        if marker not in source:
            continue
        created = {
            fragment.split()[0]
            for fragment in source.split(marker)[1:]
            if fragment.split()
        }
        absent = sorted(required - created)
        if absent:
            missing[path.name] = absent
    assert not missing, (
        "these stacks initialize Airflow without the pools their DAGs ask "
        f"for, so those tasks never schedule: {missing}"
    )
