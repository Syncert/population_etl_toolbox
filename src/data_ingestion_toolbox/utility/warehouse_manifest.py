"""The reviewed bootstrap order, and the ledger of what a warehouse received.

`sql/bootstrap/warehouse_manifest.json` is the order in which forty-odd assets
build a warehouse. Until this module there was no answer to "which of them does
*this* warehouse carry?": `control.schema_migration_state` held one row per
source's gold DDL, written by `utility.gold_schema`, and nothing recorded a
manifest asset at all. `DQ-SHARED-004` is a BLOCK rule about exactly that
comparison and could not run, because one side of it did not exist (DB-049).

Two things live here rather than in two places:

* **The reader.** `tests/support/postgres.py` already parsed the manifest for
  the disposable test warehouse. A second parser in a script would be a second
  opinion about the bootstrap order, which is the one thing the manifest exists
  to prevent, so that module now imports this one.
* **The applier.** Every path that builds a warehouse goes through
  `apply_manifest`, so the ledger a deployment carries is written by the same
  code that writes the one a test carries.

**One asset, one transaction, and the ledger row inside it.** No file under
`sql/` opens a transaction of its own, so a failure part-way through a file
used to leave a half-applied step and nothing that said so. Here the DDL and
the row that claims it are committed together: an asset that fails leaves no
row, the assets before it keep theirs, and the exit names the one that broke.
That is what makes a re-run safe to point at the same warehouse -- the ledger
says where it got to, and every asset is itself written to be re-runnable.

Nothing here is a migration framework. ADR-0001 keeps rebuild as the rollback
strategy, and this records what was applied rather than deciding what to apply.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

#: The repository this package was installed from. The manifest names paths
#: relative to it, and they are repository assets rather than packaged data --
#: an applier is an operator tool run from a checkout or the image built from
#: one. `WAREHOUSE_ROOT` overrides it for anything that relocates them.
REPOSITORY_ROOT = Path(
    os.environ.get("WAREHOUSE_ROOT") or Path(__file__).resolve().parents[3]
)
MANIFEST_PATH = REPOSITORY_ROOT / "sql/bootstrap/warehouse_manifest.json"

_LEDGER_UPSERT = """
INSERT INTO control.schema_migration_state (component_name, ddl_hash, applied_at)
VALUES (%s, %s, NOW())
ON CONFLICT (component_name) DO UPDATE
   SET ddl_hash = EXCLUDED.ddl_hash,
       applied_at = EXCLUDED.applied_at
"""


class ManifestError(RuntimeError):
    """The manifest itself cannot be read or does not describe the repository."""


class ManifestApplicationError(RuntimeError):
    """One asset failed. Its id is the first thing an operator needs."""

    def __init__(self, asset_id: str, path: str, cause: BaseException) -> None:
        super().__init__(f"{asset_id} ({path}) failed to apply: {cause}")
        self.asset_id = asset_id
        self.path = path
        self.cause = cause


@dataclass(frozen=True)
class ManifestAsset:
    """One reviewed step: what it is called, when it runs, and what it says.

    `root` is carried rather than read from the module so a caller can point a
    manifest at a directory that is not this checkout. That is what lets the
    failure path be tested honestly: a test builds a small manifest whose last
    asset does not parse and applies it for real, instead of mocking a cursor
    into raising and proving only that the `except` branch is reachable.
    """

    id: str
    phase: str
    relative_path: str
    root: Path = REPOSITORY_ROOT
    #: Applied on every deploy, not only when the ledger lacks it. For a step
    #: whose effect depends on what the warehouse holds rather than on its own
    #: text: the read-only grants name every gold schema that exists when they
    #: run, so a schema created after they last ran is unreadable by the API
    #: until they run again, while their hash says nothing changed.
    reapply: bool = False

    @property
    def path(self) -> Path:
        return self.root / self.relative_path

    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")

    def content_hash(self) -> str:
        """The sha256 of the file's bytes, not of its decoded text.

        The bytes are what was applied. Hashing the decoded string would make
        the same file hash differently under a different newline convention,
        and a Windows checkout would then read as drift against a Linux
        deployment that applied the identical step.
        """
        return hashlib.sha256(self.path.read_bytes()).hexdigest()


def load_manifest(manifest_path: Path | None = None) -> dict[str, Any]:
    path = manifest_path or MANIFEST_PATH
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"no warehouse manifest at {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{path} is not readable JSON: {exc}") from exc


def manifest_assets(
    manifest_path: Path | None = None, root: Path | None = None
) -> tuple[ManifestAsset, ...]:
    """Every asset, in the manifest's order, which is the bootstrap order."""
    path = manifest_path or MANIFEST_PATH
    manifest = load_manifest(path)
    assets = manifest.get("assets")
    if not assets:
        raise ManifestError(f"{path} declares no assets")
    base = root if root is not None else REPOSITORY_ROOT
    return tuple(
        ManifestAsset(
            id=entry["id"],
            phase=entry["phase"],
            relative_path=entry["path"],
            root=base,
            reapply=bool(entry.get("reapply", False)),
        )
        for entry in assets
    )


def apply_manifest(
    database_connection: Any,
    assets: Sequence[ManifestAsset] | None = None,
    *,
    log: logging.Logger | None = None,
) -> tuple[str, ...]:
    """Apply each asset and record it, one transaction per asset.

    Returns the ids applied, in order. Raises `ManifestApplicationError` naming
    the first asset that failed, having rolled that asset back; every asset
    before it is committed and recorded.
    """
    logger = log or logging.getLogger(__name__)
    applied: list[str] = []
    for asset in assets if assets is not None else manifest_assets():
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(asset.sql())
                # In the same transaction as the DDL it describes: a row that
                # could outlive a rolled-back step would be a warehouse
                # claiming a step it does not carry, which is worse than no
                # ledger at all.
                cursor.execute(_LEDGER_UPSERT, (asset.id, asset.content_hash()))
            database_connection.commit()
        except Exception as exc:
            database_connection.rollback()
            raise ManifestApplicationError(asset.id, asset.relative_path, exc) from exc
        applied.append(asset.id)
        logger.info("applied %s (%s)", asset.id, asset.relative_path)
    return tuple(applied)


def read_recorded_assets(cursor: Any) -> dict[str, str]:
    """Every manifest asset this warehouse has recorded, as id -> hash.

    Only manifest ids: `gold_schema`'s per-source components share the table
    and answer a different question, and a reader that conflated them would
    report drift for a relation the manifest never named. The two sets cannot
    collide -- `utility.gold_schema.SOURCE_SCHEMA_COMPONENTS` is the one place
    those names are spelled, and `test_a_manifest_asset_is_not_a_gold_component`
    holds the two apart -- so filtering by manifest id is exact rather than a
    prefix guess.

    Takes a cursor because a quality rule executor is handed one, and the rule
    that reads this must not open a connection of its own inside someone
    else's transaction.
    """
    ids = {asset.id for asset in manifest_assets()}
    cursor.execute(
        "SELECT component_name, ddl_hash FROM control.schema_migration_state"
    )
    return {name: digest for name, digest in cursor.fetchall() if name in ids}


#: Records an asset only when the ledger holds it at another hash, so a task
#: that re-confirms an unchanged step rewrites no row (refresh-churn rule).
_LEDGER_UPSERT_IF_CHANGED = (
    _LEDGER_UPSERT
    + """
 WHERE control.schema_migration_state.ddl_hash IS DISTINCT FROM EXCLUDED.ddl_hash
"""
)


def assets_for_files(files: Iterable[Path]) -> dict[Path, ManifestAsset]:
    """The manifest asset each file is, for the files that are one.

    A source's `ensure_*` task applies some of the same files the manifest
    names (its silver, gold and publisher DDL). Matching is by resolved path,
    so a task running from a mounted checkout and the manifest read from the
    same checkout agree. A file the manifest does not name is left out, and
    an unreadable manifest yields nothing rather than failing the task that
    asked: the ledger is evidence about the DDL, not a precondition for it.
    """
    try:
        by_path = {asset.path.resolve(): asset for asset in manifest_assets()}
    except (ManifestError, OSError, KeyError) as exc:
        logging.getLogger(__name__).warning(
            "warehouse manifest unreadable, task-applied DDL not recorded: %s", exc
        )
        return {}
    found: dict[Path, ManifestAsset] = {}
    for file in files:
        asset = by_path.get(Path(file).resolve())
        if asset is not None:
            found[Path(file)] = asset
    return found


def record_task_applied_assets(
    cursor: Any, assets: Iterable[ManifestAsset]
) -> tuple[str, ...]:
    """Record manifest assets a source task applied, at the bytes it applied.

    The second way a manifest step reaches a warehouse. Before this, a task
    that applied `gold_elections.sql` recorded only its own component row, so
    the ledger kept the hash of the last manifest apply and `DQ-SHARED-004`
    reported the step drifted although the warehouse carried exactly the
    checkout's file (2026-11-08). Called inside the task's transaction, after
    the DDL, so the row commits or rolls back with what it claims. Returns the
    ids whose row changed.
    """
    changed: list[str] = []
    for asset in assets:
        cursor.execute(_LEDGER_UPSERT_IF_CHANGED, (asset.id, asset.content_hash()))
        if cursor.rowcount:
            changed.append(asset.id)
    return tuple(changed)


def assets_recorded_current(cursor: Any, assets: Sequence[ManifestAsset]) -> bool:
    """Whether the ledger holds every one of `assets` at its file's hash.

    What lets a source task skip DDL the manifest applier already applied at
    a deploy: the same bytes, recorded in the same transaction as the DDL.
    """
    if not assets:
        return False
    cursor.execute(
        "SELECT component_name, ddl_hash FROM control.schema_migration_state"
        " WHERE component_name = ANY(%s)",
        ([asset.id for asset in assets],),
    )
    recorded = dict(cursor.fetchall())
    return all(recorded.get(asset.id) == asset.content_hash() for asset in assets)


def recorded_assets(database_connection: Any) -> dict[str, str]:
    """`read_recorded_assets` for a caller that holds a connection."""
    with database_connection.cursor() as cursor:
        return read_recorded_assets(cursor)


def compare_recorded(
    recorded: dict[str, str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """What a ledger is missing, and what it holds at another hash.

    Missing and drifted are returned apart because they are different faults
    with different answers. A missing asset is a step that never ran here, and
    the answer is to apply it. A drifted one ran, and the file has changed
    since -- the warehouse is at a revision the checkout no longer describes,
    and re-applying it is a decision rather than a formality.
    """
    missing: list[str] = []
    drifted: list[str] = []
    for asset in manifest_assets():
        digest = recorded.get(asset.id)
        if digest is None:
            missing.append(asset.id)
        elif digest != asset.content_hash():
            drifted.append(asset.id)
    return tuple(missing), tuple(drifted)


def compare_to_manifest(
    database_connection: Any,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """`compare_recorded` for a caller that holds a connection."""
    return compare_recorded(recorded_assets(database_connection))


def assets_to_bring_current(
    assets: Sequence[ManifestAsset],
    missing: Iterable[str],
    drifted: Iterable[str],
) -> list[ManifestAsset]:
    """What a deploy applies to a populated warehouse, in manifest order.

    The assets the ledger lacks or holds at another hash, and every asset
    marked `reapply` whatever the ledger says.
    """
    flagged = set(missing) | set(drifted)
    return [asset for asset in assets if asset.id in flagged or asset.reapply]


#: The gold schemas the public serving role cannot use. Empty on a warehouse
#: whose grants are current; a row is a schema the API answers 503 for.
_UNGRANTED_SERVING_SCHEMAS = """
SELECT namespace.nspname
FROM pg_catalog.pg_namespace AS namespace
WHERE namespace.nspname LIKE 'gold%%'
  AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s)
  AND NOT pg_catalog.has_schema_privilege(%s, namespace.oid, 'USAGE')
ORDER BY namespace.nspname
"""


def ungranted_serving_schemas(cursor: Any, role: str = "api_reader") -> tuple[str, ...]:
    """Gold schemas `role` has no USAGE on. Empty when the role does not exist.

    A deploy that creates a gold schema and does not re-run the grants leaves
    every route over it failing with a permission error the API reports as
    "Database service is temporarily unavailable" -- which is what happened
    to `gold_elections`, `gold_census_govfin` and `gold_irs_soi` on 2026-11-06.
    """
    cursor.execute(_UNGRANTED_SERVING_SCHEMAS, (role, role))
    return tuple(row[0] for row in cursor.fetchall())


def asset_paths(assets: Iterable[ManifestAsset] | None = None) -> tuple[Path, ...]:
    """The files, in order -- what a caller that only applies SQL needs."""
    return tuple(
        asset.path for asset in (assets if assets is not None else manifest_assets())
    )
