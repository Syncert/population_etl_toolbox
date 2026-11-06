"""The read-only grants are re-applied on every deploy and name every gold schema.

Covers: DB-071 -- the serving role's grants follow the gold schemas a deploy
        creates, and a gold schema the policy does not name fails the build.

On 2026-11-06 `/public-money` and `/governor-terms` answered 503 on OFFICE_LAB:
`gold_elections`, `gold_census_govfin` and `gold_irs_soi` were created by
manifest assets after `sql/bootstrap/001_api_readonly.sql` last ran (at initdb,
before any gold schema existed), and nothing ran it again.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from data_ingestion_toolbox.utility.warehouse_manifest import (
    ManifestAsset,
    assets_to_bring_current,
    manifest_assets,
)

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GRANTS_SQL = REPOSITORY_ROOT / "sql/bootstrap/001_api_readonly.sql"
_CREATES_GOLD_SCHEMA = re.compile(
    r"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?(gold[a-z0-9_]*)", re.IGNORECASE
)


def _granted_schemas() -> set[str]:
    text = GRANTS_SQL.read_text(encoding="utf-8")
    listed = re.search(r"FOREACH _schema IN ARRAY ARRAY\[(.*?)\]", text, re.DOTALL)
    assert listed, "the grant loop's schema list was not found"
    return set(re.findall(r"'([a-z0-9_]+)'", listed.group(1)))


def _created_gold_schemas() -> dict[str, Path]:
    created: dict[str, Path] = {}
    for root in ("sql", "src"):
        for path in sorted((REPOSITORY_ROOT / root).rglob("*.sql")):
            for name in _CREATES_GOLD_SCHEMA.findall(path.read_text(encoding="utf-8")):
                created.setdefault(name.lower(), path.relative_to(REPOSITORY_ROOT))
    return created


def test_every_gold_schema_the_repository_creates_is_granted_to_the_serving_role() -> (
    None
):
    """Covers: DB-071 — a new gold schema without a grant fails here, not in production."""
    created = _created_gold_schemas()
    assert "gold_elections" in created, "the schema scan found nothing it should"
    ungranted = {
        name: str(path)
        for name, path in created.items()
        if name not in _granted_schemas()
    }
    assert not ungranted, (
        "these gold schemas are created but not in the grant list of "
        f"sql/bootstrap/001_api_readonly.sql, so the API cannot read them: {ungranted}"
    )


def test_the_grants_are_the_last_manifest_asset_and_are_reapplied() -> None:
    """Covers: DB-071 — the grants run after every schema exists, on every deploy."""
    assets = manifest_assets()
    last = assets[-1]
    assert last.id == "api-readonly-grants"
    assert last.relative_path == "sql/bootstrap/001_api_readonly.sql"
    assert last.reapply
    assert [asset.id for asset in assets if asset.reapply] == ["api-readonly-grants"]


def test_a_deploy_applies_what_the_ledger_lacks_and_every_reapplied_asset_in_order() -> (
    None
):
    """Covers: DB-071 — `--only-flagged` keeps the grants even when nothing is flagged."""
    assets = (
        ManifestAsset("first", "foundation", "a.sql"),
        ManifestAsset("second", "gold", "b.sql"),
        ManifestAsset("grants", "grants", "c.sql", reapply=True),
    )
    assert [a.id for a in assets_to_bring_current(assets, (), ())] == ["grants"]
    assert [a.id for a in assets_to_bring_current(assets, ("second",), ())] == [
        "second",
        "grants",
    ]
    assert [a.id for a in assets_to_bring_current(assets, (), ("first",))] == [
        "first",
        "grants",
    ]
    real = manifest_assets()
    assert [a.id for a in assets_to_bring_current(real, (), ())] == [
        "api-readonly-grants"
    ]
