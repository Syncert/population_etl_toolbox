"""Replay one captured BEA table into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the line dimension, the parsed revisions and
quarantine, the geography resolution ledger and the conformed facts land
together or not at all, and the run is only marked ready when the counts
reconcile. A county table's run also adds each metro area summed from its
counties (``metro.py``), marked as derived, in the same transaction. A run
the capture found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import PARSER_CONTRACT_VERSION
from ..config import SOURCE_CODE
from ..registry import BeaTable, get_table
from .metro import derive_metro_observations
from .parse import parse_table

OBSERVATION_BASIS = (
    "BEA regional economic accounts: the county as an economy (income and "
    "production by place of residence or of work, as the line states), not a "
    "survey of residents"
)
METHODOLOGY_URL = (
    "https://www.bea.gov/resources/methodologies/local-area-personal-income"
)


#: What a regional price parity is (grocery-and-gasoline-prices): a price
#: level, not a price change, and not a grocery price.
PRICE_OBSERVATION_BASIS = (
    "Price level relative to the United States all-items level (= 100) in one "
    "year: comparable across areas within a year, not a measure of inflation. "
    "There is no grocery line; food is inside goods."
)
PRICE_METHODOLOGY_URL = "https://www.bea.gov/data/prices-inflation/regional-price-parities-state-and-metro-area"

#: What an implicit regional price deflator is: a price index over time, not
#: a price level across areas.
DEFLATOR_OBSERVATION_BASIS = (
    "Implicit regional price deflator (2017 = 100): the area's regional price "
    "parity times the national PCE price index. Current-dollar income divided "
    "by it is real income in 2017 dollars."
)

_BASES = {
    "price_level_us_100": (PRICE_OBSERVATION_BASIS, PRICE_METHODOLOGY_URL),
    "price_index": (DEFLATOR_OBSERVATION_BASIS, PRICE_METHODOLOGY_URL),
}

#: The newest CBSA delineation's member counties, by five-digit FIPS.
_METRO_MEMBERSHIP_SQL = """
    WITH delineation AS (
        SELECT relationship.geography_vintage, parent.geo_id AS metro_geo_id,
               member.state_fips || member.county_fips AS county_code
        FROM silver_ref.bridge_geo_relationship_version AS relationship
        JOIN silver_ref.dim_geo_entity AS parent ON parent.geo_sk = relationship.parent_geo_sk
        JOIN silver_ref.dim_geo_entity AS member ON member.geo_sk = relationship.related_geo_sk
        WHERE relationship.relationship_type = 'contains'
          AND relationship.evidence_source = 'census_cbsa_delineation'
          AND parent.geo_type = 'metro'
          AND member.geo_type = 'county'
    )
    SELECT geography_vintage, metro_geo_id, county_code
    FROM delineation
    WHERE geography_vintage = (SELECT MAX(geography_vintage) FROM delineation)
"""


def _metro_memberships(cursor: Any) -> tuple[int | None, dict[str, list[str]]]:
    cursor.execute(_METRO_MEMBERSHIP_SQL)
    vintage: int | None = None
    memberships: dict[str, list[str]] = {}
    for row_vintage, metro, county in cursor.fetchall():
        vintage = int(row_vintage)
        memberships.setdefault(metro, []).append(county)
    return vintage, memberships


class BeaReconciliationError(RuntimeError):
    """A replayed run does not account for every in-scope captured row."""


class BeaPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


#: The table's newest published capture, and whether a registered line has
#: any fact at all (`bea_fact_lookup_idx` answers it from its first key).
_PUBLISHED_CAPTURE_SQL = """
    SELECT capture.capture_id::TEXT
    FROM control.bea_table_capture AS capture
    WHERE capture.table_code = %s AND capture.status = 'published'
    ORDER BY capture.published_at DESC
    LIMIT 1
"""
_LINE_LOADED_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM silver_bea.fact_observation
        WHERE table_code = %s AND line_code = %s
    )
"""


def _unloaded_lines(cursor: Any, table: BeaTable) -> tuple[frozenset[str], str | None]:
    """Registered lines with no fact yet, and the capture to read them from.

    Lines added to the registry after the file was last read. A fact, not a
    `dim_line` row, is the evidence: a line row can outlive its facts.
    """
    missing = set()
    for line in sorted(table.lines):
        cursor.execute(_LINE_LOADED_SQL, (table.code, line))
        if not cursor.fetchone()[0]:
            missing.add(line)
    if not missing:
        return frozenset(), None
    cursor.execute(_PUBLISHED_CAPTURE_SQL, (table.code,))
    row = cursor.fetchone()
    return frozenset(missing), (row[0] if row else None)


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured table; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_code, capture_id::TEXT, status
                FROM control.bea_table_capture WHERE run_id = %s
                """,
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise BeaReconciliationError("run has no captured BEA table")
            table_code, capture_id, status = row
            table = get_table(table_code)
            backfill: frozenset[str] = frozenset()
            if status == "unchanged":
                backfill, published_capture = _unloaded_lines(cursor, table)
                if not backfill or published_capture is None:
                    connection.commit()
                    return 0
                # The bytes repeat the published capture, but the registry
                # names lines that capture was never read for (CAINC4 line 46
                # was registered after its file was first loaded). Read them
                # from that same capture, so nothing already loaded is stored
                # a second time and the new lines carry its release.
                capture_id = published_capture
            parsed = parse_table(
                load_captured_payload(connection_factory, UUID(capture_id)), table=table
            )
            if backfill:
                parsed = replace(
                    parsed,
                    observations=tuple(
                        obs for obs in parsed.observations if obs.line_code in backfill
                    ),
                    quarantined=(),
                    combined=tuple(
                        obs for obs in parsed.combined if obs.line_code in backfill
                    ),
                )
            lines = {}
            for observation in parsed.observations:
                lines.setdefault(
                    observation.line_code, (observation.description, observation.unit)
                )
            if lines:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.dim_line (
                        table_code, line_code, table_title, description, unit,
                        dollar_basis, observation_basis, methodology_url,
                        parser_contract_version
                    ) VALUES %s
                    ON CONFLICT (table_code, line_code) DO UPDATE SET
                        table_title = EXCLUDED.table_title,
                        description = EXCLUDED.description,
                        unit = EXCLUDED.unit,
                        dollar_basis = EXCLUDED.dollar_basis,
                        observation_basis = EXCLUDED.observation_basis,
                        methodology_url = EXCLUDED.methodology_url,
                        parser_contract_version = EXCLUDED.parser_contract_version,
                        updated_at = NOW()
                -- Only a row whose content changed is rewritten
                -- (refresh-churn audit, 2026-11-06).
                WHERE (silver_bea.dim_line.table_title, silver_bea.dim_line.description, silver_bea.dim_line.unit, silver_bea.dim_line.dollar_basis, silver_bea.dim_line.observation_basis, silver_bea.dim_line.methodology_url, silver_bea.dim_line.parser_contract_version)
                      IS DISTINCT FROM
                      (EXCLUDED.table_title, EXCLUDED.description, EXCLUDED.unit, EXCLUDED.dollar_basis, EXCLUDED.observation_basis, EXCLUDED.methodology_url, EXCLUDED.parser_contract_version)
                    """,
                    [
                        (
                            table.code,
                            line,
                            table.title,
                            description,
                            unit,
                            table.lines[line],
                            _BASES.get(
                                table.lines[line], (OBSERVATION_BASIS, METHODOLOGY_URL)
                            )[0],
                            _BASES.get(
                                table.lines[line], (OBSERVATION_BASIS, METHODOLOGY_URL)
                            )[1],
                            PARSER_CONTRACT_VERSION,
                        )
                        for line, (description, unit) in sorted(lines.items())
                    ],
                )
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.observation_revision (
                        capture_id, source_row_index, year, run_id, table_code,
                        line_code, geo_type, geo_source_code, geo_source_label,
                        geo_id, value_source, value, value_status, source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, year) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            obs.source_row_index,
                            obs.year,
                            str(run_id),
                            table.code,
                            obs.line_code,
                            obs.geo_type,
                            obs.geo_source_code,
                            obs.geo_source_label,
                            obs.geo_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.source_record_id,
                        )
                        for obs in parsed.observations
                    ],
                    page_size=10000,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.observation_quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            capture_id,
                            q.source_row_index,
                            q.error_code,
                            q.error_summary,
                        )
                        for q in parsed.quarantined
                    ],
                )
            payload_rejected = any(q.source_row_index == 0 for q in parsed.quarantined)
            # A backfill leaves this run `unchanged`: its own bytes added nothing.
            cursor.execute(
                """
                UPDATE control.bea_table_capture
                   SET member_name = %s, release_date = %s, captured_row_count = %s,
                       in_scope_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status NOT IN ('published', 'unchanged')
                """,
                (
                    parsed.member,
                    parsed.release_date,
                    parsed.row_count,
                    parsed.in_scope_row_count,
                    "quarantined" if payload_rejected else "silver_ready",
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.geo_source_code)
                       %s, revision.table_code, revision.geo_type, revision.geo_source_code,
                       revision.geo_source_label, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_bea.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, revision.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                -- Only a row whose content changed is rewritten
                -- (refresh-churn audit, 2026-11-06).
                WHERE (silver_ref.geography_resolution.source_label, silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.source_label, EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (
                    SOURCE_CODE,
                    parsed.release_date.year if parsed.release_date else None,
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_bea.fact_observation (
                    table_code, line_code, geo_id, year, capture_id, run_id,
                    release_date, retrieved_at, geo_sk, geo_type, geography_status,
                    value_source, value, value_status, source_record_id
                )
                SELECT revision.table_code, revision.line_code, revision.geo_id, revision.year,
                       revision.capture_id, revision.run_id, %s, capture.retrieved_at,
                       entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.source_record_id
                FROM silver_bea.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (table_code, line_code, geo_id, year, capture_id) DO NOTHING
                """,
                (parsed.release_date, str(run_id)),
            )
            derived = []
            if table.geography == "county" and parsed.release_date is not None:
                vintage, memberships = _metro_memberships(cursor)
                derived = derive_metro_observations(
                    table,
                    parsed.observations,
                    parsed.combined,
                    memberships,
                    delineation_vintage=vintage,
                )
            if derived:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.fact_observation (
                        table_code, line_code, geo_id, year, capture_id, run_id,
                        release_date, retrieved_at, geo_sk, geo_type, geography_status,
                        value_source, value, value_status, source_record_id, derivation
                    )
                    SELECT derived.table_code, derived.line_code, derived.geo_id,
                           derived.year, capture.capture_id, derived.run_id,
                           derived.release_date, capture.retrieved_at,
                           entity.geo_sk, 'metro',
                           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                           derived.value_source, derived.value, derived.value_status,
                           derived.source_record_id, derived.derivation
                    FROM (VALUES %s) AS derived (
                        table_code, line_code, geo_id, year, capture_id, run_id,
                        release_date, value, value_status, value_source,
                        source_record_id, derivation
                    )
                    JOIN raw_capture.response_capture AS capture
                      ON capture.capture_id = derived.capture_id
                    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = derived.geo_id
                    ON CONFLICT (table_code, line_code, geo_id, year, capture_id) DO NOTHING
                    """,
                    [
                        (
                            table.code,
                            item.line_code,
                            item.geo_id,
                            item.year,
                            capture_id,
                            str(run_id),
                            parsed.release_date,
                            item.value,
                            item.value_status,
                            item.value_source,
                            item.source_record_id,
                            item.derivation,
                        )
                        for item in derived
                    ],
                    template=(
                        "(%s, %s, %s, %s::INTEGER, %s::UUID, %s::UUID, %s::DATE, "
                        "%s::NUMERIC, %s, %s, %s, %s)"
                    ),
                    page_size=10000,
                )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bea.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bea.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != len(parsed.observations) or facts != revisions + len(
                derived
            ):
                raise BeaReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions and {facts} facts "
                    f"against {len(parsed.observations)} parsed and "
                    f"{len(derived)} derived"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    _analyze(connection_factory)
    return facts


#: Refreshed after every replay, so a table's first load (millions of rows for
#: CAINC35 or CAINC5N) is planned from real counts, not empty-table estimates.
ANALYZED_RELATIONS = ("silver_bea.fact_observation", "silver_bea.observation_revision")


def _analyze(connection_factory: Callable[[], Any]) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            for relation in ANALYZED_RELATIONS:
                cursor.execute(f"ANALYZE {relation}")
        connection.commit()
    finally:
        connection.close()


def _run_wrote_facts(cursor: Any, run_id: UUID) -> bool:
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM silver_bea.fact_observation WHERE run_id = %s)",
        (str(run_id),),
    )
    return bool(cursor.fetchone()[0])


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled table through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.bea_table_capture
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.bea_table_capture WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise BeaPublicationError(f"run {run_id} was never replayed")
            # An unchanged run that read newly registered lines from the
            # published capture: those lines are served now, so the glossary
            # must name them.
            backfilled = row[0] == "unchanged" and _run_wrote_facts(cursor, run_id)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published or backfilled:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_bea")
    return published
