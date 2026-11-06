"""A deploy re-applies the serving role's grants to schemas created since.

Covers: DB-071 -- the serving role's grants follow the gold schemas a deploy
        creates, and a gold schema the policy does not name fails the check.

Runs against the manifest-built warehouse with no separate application of
`sql/bootstrap/001_api_readonly.sql`: the manifest's own last asset is what
must grant, because that is all a deploy runs.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.utility.warehouse_manifest import (
    apply_manifest,
    assets_to_bring_current,
    manifest_assets,
    ungranted_serving_schemas,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: What `/public-money` and `/governor-terms` read; all three schemas answered
#: 503 on OFFICE_LAB before the grants were re-applied.
PUBLIC_MONEY_RELATIONS = (
    "gold_census_govfin.tax_burden",
    "gold_census_govfin.tax_mix",
    "gold_elections.governor_term_fiscal",
    "gold_elections.governor_term_tax_mix",
    "gold_elections.governor_fiscal_measure_definition",
    "gold_irs_soi.observation_latest",
    "gold_bea.observation_latest",
)


def _redeploy(database: connection) -> tuple[str, ...]:
    """What `apply_warehouse_manifest --only-flagged` runs on a current ledger."""
    return apply_manifest(database, assets_to_bring_current(manifest_assets(), (), ()))


def test_the_manifest_alone_grants_every_gold_schema(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-071 — no gold schema is ungranted and the public-money reads are allowed."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            assert ungranted_serving_schemas(cursor) == ()
            for relation in PUBLIC_MONEY_RELATIONS:
                cursor.execute(
                    "SELECT has_table_privilege('api_reader', %s, 'SELECT')",
                    (relation,),
                )
                assert cursor.fetchone()[0], f"api_reader cannot read {relation}"
    finally:
        database.rollback()
        database.close()


def test_a_redeploy_grants_a_schema_created_after_the_grants_last_ran(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-071 — the OFFICE_LAB 503: USAGE missing, then restored by the deploy alone."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            # A schema created after the grants ran carries none; revoking
            # reproduces that state on a schema the API reads.
            cursor.execute("REVOKE USAGE ON SCHEMA gold_elections FROM api_reader")
        database.commit()
        with database.cursor() as cursor:
            assert ungranted_serving_schemas(cursor) == ("gold_elections",)
        database.rollback()

        assert _redeploy(database) == ("api-readonly-grants",)

        with database.cursor() as cursor:
            assert ungranted_serving_schemas(cursor) == ()
            cursor.execute(
                "SELECT has_table_privilege('api_reader', 'gold_elections.governor_term_fiscal', 'SELECT')"
            )
            assert cursor.fetchone()[0]
        database.rollback()
    finally:
        database.rollback()
        _redeploy(database)
        database.close()


def test_a_gold_schema_the_policy_does_not_name_is_reported(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-071 — the check names a gold schema the grants do not cover."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("CREATE SCHEMA gold_db071_probe")
            cursor.execute("CREATE VIEW gold_db071_probe.answer AS SELECT 1 AS one")
            assert ungranted_serving_schemas(cursor) == ("gold_db071_probe",)
            assert ungranted_serving_schemas(cursor, role="no_such_role_db071") == ()
    finally:
        database.rollback()
        database.close()
