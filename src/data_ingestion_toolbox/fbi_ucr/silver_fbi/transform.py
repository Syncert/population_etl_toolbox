"""Transactional FBI UCR silver conformance and geography resolution.

Geography resolution here is deliberately conservative:

* a national or state subject resolves through its exact provider code;
* an agency resolves to its own ORI identity and never to an area;
* a county association resolves only when the provider county label matches
  exactly one authoritative Census county name inside the agency's own state,
  and stays ambiguous or unresolved otherwise; and
* a place association exists only where a reviewed, effective-dated crosswalk
  entry covers the whole registered period.

No relationship changes an observation's grain. County and place relationships
support discovery and filtering, never an area total.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from ..reference import CROSSWALK_VERSION, REVIEWED_PLACE_MAPPINGS
from ..registry import STATE_CODE_CONTRACT, UNSUPPORTED_STATE_CODES, FbiUcrProduct
from .participation import period_bounds


class FbiReconciliationError(RuntimeError):
    """A captured release cannot be marked ready for publication."""


#: Legal-status suffixes the Census publishes on county-equivalent names. The
#: FBI publishes the bare name, so the suffix is removed before an exact match.
#: This is an exact, uniqueness-checked rule, not a fuzzy or approximate match.
COUNTY_SUFFIX_PATTERN = (
    r"\s+(CITY AND BOROUGH|CENSUS AREA|MUNICIPALITY|MUNICIPIO|BOROUGH|PARISH"
    r"|COUNTY|CITY)$"
)

_AGENCY_STATUS_CTE = """
    agency_status AS (
        SELECT ori,
               CASE
                   WHEN BOOL_OR(relationship_type = 'place'
                                AND resolution_status = 'resolved')
                        THEN 'agency_place_bridged'
                   WHEN BOOL_OR(relationship_type = 'county'
                                AND resolution_status = 'resolved')
                        THEN 'agency_county_bridged'
                   WHEN BOOL_OR(relationship_type = 'county'
                                AND resolution_status = 'ambiguous')
                        THEN 'ambiguous'
                   ELSE 'agency_only'
               END AS geography_status
        FROM silver_fbi.agency_geography_relationship
        WHERE product_id = %(product_id)s AND release_key = %(release_key)s
        GROUP BY ori
    )
"""

_GEOGRAPHY_STATUS_SQL = """
    CASE revision.subject_type
        WHEN 'national' THEN 'provider_geo_exact'
        WHEN 'state' THEN CASE WHEN state.state_fips IS NOT NULL
                               THEN 'provider_geo_exact' ELSE 'unsupported' END
        ELSE COALESCE(agency_status.geography_status, 'agency_only')
    END
"""

_GEO_ID_SQL = """
    CASE revision.subject_type
        WHEN 'national' THEN 'us:1'
        WHEN 'state' THEN CASE WHEN state.state_fips IS NOT NULL
                               THEN 'state:' || state.state_fips END
        ELSE 'agency:' || revision.subject_code
    END
"""


def seed_reference_contracts(cursor: Any) -> None:
    """Load the frozen state contract and reviewed place crosswalk.

    Both are owned by checked-in Python so review happens in code; they are
    materialized here only so silver conformance can join against them.
    """
    states = [
        (code, label, fips, True)
        for code, (label, fips) in sorted(STATE_CODE_CONTRACT.items())
    ] + [(code, code, None, False) for code in sorted(UNSUPPORTED_STATE_CODES)]
    execute_values(
        cursor,
        """
        INSERT INTO silver_fbi.dim_state_code (
            state_code, state_label, state_fips, is_supported
        ) VALUES %s
        ON CONFLICT (state_code) DO UPDATE SET
            state_label = EXCLUDED.state_label,
            state_fips = EXCLUDED.state_fips,
            is_supported = EXCLUDED.is_supported,
            updated_at = NOW()
        """,
        states,
    )
    mappings = [
        (
            mapping.ori,
            mapping.place_geo_id,
            mapping.place_name,
            mapping.state_fips,
            mapping.place_fips,
            mapping.geography_vintage,
            mapping.effective_start,
            mapping.effective_end,
            CROSSWALK_VERSION,
            mapping.evidence_url,
            mapping.review_note,
        )
        for mapping in REVIEWED_PLACE_MAPPINGS
    ]
    if mappings:
        execute_values(
            cursor,
            """
            INSERT INTO silver_fbi.reviewed_place_crosswalk (
                ori, place_geo_id, place_name, state_fips, place_fips,
                geography_vintage, effective_start, effective_end,
                crosswalk_version, evidence_url, review_note
            ) VALUES %s
            ON CONFLICT (ori, place_geo_id, effective_start) DO UPDATE SET
                place_name = EXCLUDED.place_name,
                effective_end = EXCLUDED.effective_end,
                crosswalk_version = EXCLUDED.crosswalk_version,
                evidence_url = EXCLUDED.evidence_url,
                review_note = EXCLUDED.review_note,
                updated_at = NOW()
            """,
            mappings,
        )


def transform_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: FbiUcrProduct,
    release_key: str,
) -> int:
    """Conform one replayed release atomically and enforce reconciliation."""
    period_start, _ = period_bounds(product.period_start)
    _, period_end = period_bounds(product.period_end)
    scope = {
        "run_id": str(run_id),
        "product_id": product.product_id,
        "release_key": release_key,
        "effective_start": period_start,
        "effective_end": period_end,
        "vintage": period_start.year,
        "transformation_version": product.parser_contract_version,
    }
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            seed_reference_contracts(cursor)
            _load_release(cursor, product=product, scope=scope)
            _load_measures(cursor, scope=scope)
            _load_agencies(cursor, scope=scope)
            _load_agency_entities(cursor, scope=scope)
            _load_state_relationships(cursor, scope=scope)
            _load_county_relationships(cursor, scope=scope)
            _load_place_relationships(cursor, scope=scope)
            _supersede_contained_relationships(cursor, scope=scope)
            _load_shared_bridges(cursor, scope=scope)
            _load_geography_resolution(cursor, scope=scope)
            _load_participation_facts(cursor, scope=scope)
            _load_observation_facts(cursor, scope=scope)
            facts = _reconcile(cursor, scope=scope)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return facts


def _load_release(cursor: Any, *, product: FbiUcrProduct, scope: dict) -> None:
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_ucr_dataset_release (
            product_id, release_key, refresh_date, max_data_month, ucr_program,
            offense_code, offense_label, period_start, period_end,
            documentation_url, methodology_url, parser_contract_version,
            reported_status, counted_entity_note, release_capture_id,
            source_run_id, source_record_count, quarantine_count, status
        )
        SELECT release.product_id, release.refresh_date::TEXT,
               release.refresh_date, release.max_data_month,
               release.ucr_program, release.offense_code, %(offense_label)s,
               release.period_start, release.period_end,
               %(documentation_url)s, %(methodology_url)s,
               release.parser_contract_version, %(reported_status)s,
               %(counted_entity_note)s, release.release_capture_id,
               release.run_id,
               (SELECT COUNT(*) FROM silver_fbi.observation_revision
                 WHERE run_id = release.run_id)
             + (SELECT COUNT(*) FROM silver_fbi.participation_revision
                 WHERE run_id = release.run_id)
             + (SELECT COUNT(*) FROM silver_fbi.agency_revision
                 WHERE run_id = release.run_id),
               (SELECT COUNT(*) FROM silver_fbi.slice_quarantine
                 WHERE run_id = release.run_id),
               'replaying'
        FROM control.fbi_ucr_release AS release
        WHERE release.run_id = %(run_id)s
          AND release.product_id = %(product_id)s
          AND release.decision = 'ingest'
          AND release.complete
        ON CONFLICT (product_id, release_key) DO UPDATE SET
            source_run_id = EXCLUDED.source_run_id,
            source_record_count = EXCLUDED.source_record_count,
            quarantine_count = EXCLUDED.quarantine_count,
            status = CASE
                WHEN silver_fbi.dim_ucr_dataset_release.status = 'published'
                THEN 'published' ELSE 'replaying' END,
            updated_at = NOW()
        """,
        {
            **scope,
            "offense_label": product.offense_label,
            "documentation_url": product.documentation_url,
            "methodology_url": product.methodology_url,
            "reported_status": product.reported_status,
            "counted_entity_note": product.counted_entity_note,
        },
    )
    if cursor.rowcount != 1:
        raise FbiReconciliationError(
            "FBI release is absent, quarantined, or incomplete"
        )


def _load_measures(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_offense_measure (
            product_id, measure_id, ucr_program, offense_code, offense_label,
            measure_form, counted_entity_basis, unit, reported_status
        )
        SELECT DISTINCT product_id, measure_id, ucr_program, offense_code,
               offense_label, measure_form, counted_entity_basis, unit,
               reported_status
        FROM silver_fbi.observation_revision
        WHERE run_id = %(run_id)s
        ON CONFLICT (product_id, measure_id) DO UPDATE SET
            offense_label = EXCLUDED.offense_label,
            unit = EXCLUDED.unit,
            reported_status = EXCLUDED.reported_status,
            updated_at = NOW()
        """,
        scope,
    )


def _load_agencies(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_agency (
            ori, state_code, first_seen_release, last_seen_release
        )
        SELECT DISTINCT ON (ori) ori, state_code, release_key, release_key
        FROM silver_fbi.agency_revision
        WHERE run_id = %(run_id)s
        ORDER BY ori, capture_id, source_row_index
        ON CONFLICT (ori) DO UPDATE SET
            last_seen_release = GREATEST(
                silver_fbi.dim_agency.last_seen_release,
                EXCLUDED.last_seen_release
            ),
            first_seen_release = LEAST(
                silver_fbi.dim_agency.first_seen_release,
                EXCLUDED.first_seen_release
            ),
            updated_at = NOW()
        """,
        scope,
    )
    cursor.execute(
        """
        INSERT INTO silver_fbi.dim_agency_version (
            ori, release_key, agency_name, agency_type, state_name,
            county_labels, is_nibrs, nibrs_start_date, latitude, longitude,
            attribute_checksum, evidence_capture_id
        )
        SELECT DISTINCT ON (ori, release_key)
               ori, release_key, agency_name, agency_type, state_name,
               county_labels, is_nibrs, nibrs_start_date, latitude, longitude,
               ENCODE(SHA256(CONVERT_TO(
                   agency_name || '|' || agency_type || '|' ||
                   COALESCE(state_name, '') || '|' ||
                   ARRAY_TO_STRING(county_labels, ',') || '|' ||
                   COALESCE(is_nibrs::TEXT, '') || '|' ||
                   COALESCE(nibrs_start_date, ''), 'UTF8')), 'hex'),
               capture_id
        FROM silver_fbi.agency_revision
        WHERE run_id = %(run_id)s
        ORDER BY ori, release_key, capture_id, source_row_index
        ON CONFLICT (ori, release_key) DO NOTHING
        """,
        scope,
    )


def _load_agency_entities(cursor: Any, *, scope: dict) -> None:
    """Register each ORI as a source-native geography entity."""
    cursor.execute(
        """
        INSERT INTO silver_ref.dim_geo_entity (
            geo_id, geo_type, provider_agency_code, state_fips,
            first_seen_version, last_seen_version
        )
        SELECT DISTINCT ON (revision.ori)
               'agency:' || revision.ori, 'agency', revision.ori,
               state.state_fips, %(vintage)s, %(vintage)s
        FROM silver_fbi.agency_revision AS revision
        LEFT JOIN silver_fbi.dim_state_code AS state
               ON state.state_code = revision.state_code
        WHERE revision.run_id = %(run_id)s
        ORDER BY revision.ori, revision.capture_id, revision.source_row_index
        ON CONFLICT (geo_id) DO UPDATE SET
            last_seen_version = GREATEST(
                silver_ref.dim_geo_entity.last_seen_version,
                EXCLUDED.last_seen_version
            ),
            updated_at = NOW()
        """,
        scope,
    )
    cursor.execute(
        """
        INSERT INTO silver_ref.dim_geo_entity_version (
            geo_sk, geography_vintage, source_snapshot_id, name, usps,
            latitude, longitude, is_active, attribute_checksum
        )
        SELECT DISTINCT ON (entity.geo_sk)
               entity.geo_sk, %(vintage)s, revision.capture_id,
               revision.agency_name, revision.state_code, revision.latitude,
               revision.longitude, TRUE,
               ENCODE(SHA256(CONVERT_TO(
                   revision.agency_name || '|' || revision.agency_type, 'UTF8'
               )), 'hex')
        FROM silver_fbi.agency_revision AS revision
        JOIN silver_ref.dim_geo_entity AS entity
          ON entity.geo_id = 'agency:' || revision.ori
        WHERE revision.run_id = %(run_id)s
        ORDER BY entity.geo_sk, revision.capture_id, revision.source_row_index
        ON CONFLICT (geo_sk, geography_vintage, attribute_checksum) DO NOTHING
        """,
        scope,
    )
    cursor.execute(
        """
        UPDATE silver_fbi.dim_agency AS agency
           SET geo_sk = entity.geo_sk, updated_at = NOW()
          FROM silver_ref.dim_geo_entity AS entity
         WHERE entity.geo_id = 'agency:' || agency.ori
           AND agency.geo_sk IS DISTINCT FROM entity.geo_sk
        """
    )


_RELATIONSHIP_CONFLICT = """
    ON CONFLICT (
        ori, relationship_type, source_label, geography_vintage, effective_start
    ) DO UPDATE SET
        geo_id = EXCLUDED.geo_id,
        geo_sk = EXCLUDED.geo_sk,
        resolution_method = EXCLUDED.resolution_method,
        resolution_status = EXCLUDED.resolution_status,
        confidence_class = EXCLUDED.confidence_class,
        reason_code = EXCLUDED.reason_code,
        evidence_capture_id = EXCLUDED.evidence_capture_id,
        release_key = EXCLUDED.release_key,
        updated_at = NOW()
"""

_RELATIONSHIP_COLUMNS = """
    INSERT INTO silver_fbi.agency_geography_relationship (
        ori, relationship_type, source_label, geo_id, geo_sk,
        resolution_method, resolution_status, confidence_class, reason_code,
        effective_start, effective_end, geography_vintage, evidence_source,
        evidence_capture_id, product_id, release_key
    )
"""


def _supersede_contained_relationships(cursor: Any, *, scope: dict) -> None:
    """Remove relationship rows a wider effective window fully supersedes.

    A reviewed period-window widening re-derives each agency relationship with
    broader effectivity (and a new vintage year), so the conflict key keeps the
    narrower row from the earlier window. That row carries no distinct evidence
    and would double every observation falling inside both ranges in
    ``gold_fbi.agency_observation_area_filter``. Deletion is restricted to rows
    whose identity and resolved geography match the wider row exactly.
    """
    cursor.execute(
        """
        DELETE FROM silver_fbi.agency_geography_relationship AS narrower
        USING silver_fbi.agency_geography_relationship AS wider
        WHERE narrower.product_id = %(product_id)s
          AND wider.product_id = narrower.product_id
          AND wider.ori = narrower.ori
          AND wider.relationship_type = narrower.relationship_type
          AND wider.source_label = narrower.source_label
          AND wider.geo_id IS NOT DISTINCT FROM narrower.geo_id
          AND wider.effective_start <= narrower.effective_start
          AND wider.effective_end >= narrower.effective_end
          AND (wider.effective_start, wider.effective_end)
              <> (narrower.effective_start, narrower.effective_end)
        """,
        scope,
    )


def _load_state_relationships(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        _RELATIONSHIP_COLUMNS
        + """
        SELECT DISTINCT ON (revision.ori)
               revision.ori, 'state', revision.state_code,
               CASE WHEN state.state_fips IS NOT NULL
                    THEN 'state:' || state.state_fips END,
               entity.geo_sk,
               CASE WHEN state.state_fips IS NOT NULL
                    THEN 'exact_state_code' END,
               CASE WHEN state.state_fips IS NULL THEN 'unsupported'
                    WHEN entity.geo_sk IS NULL THEN 'unresolved'
                    ELSE 'resolved' END,
               CASE WHEN state.state_fips IS NULL OR entity.geo_sk IS NULL
                    THEN 'unresolved' ELSE 'exact' END,
               CASE WHEN state.state_fips IS NULL THEN 'undocumented_state_code'
                    WHEN entity.geo_sk IS NULL
                    THEN 'canonical_geography_absent' END,
               %(effective_start)s, %(effective_end)s, %(vintage)s,
               'fbi_cde_agency_state_code', revision.capture_id,
               %(product_id)s, %(release_key)s
        FROM silver_fbi.agency_revision AS revision
        LEFT JOIN silver_fbi.dim_state_code AS state
               ON state.state_code = revision.state_code
        LEFT JOIN silver_ref.dim_geo_entity AS entity
               ON entity.geo_id = 'state:' || state.state_fips
        WHERE revision.run_id = %(run_id)s
        ORDER BY revision.ori, revision.capture_id, revision.source_row_index
        """
        + _RELATIONSHIP_CONFLICT,
        scope,
    )


def _load_county_relationships(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        """
        WITH labels AS (
            SELECT DISTINCT revision.ori, revision.state_code,
                   revision.capture_id, label
            FROM silver_fbi.agency_revision AS revision,
                 LATERAL UNNEST(revision.county_labels) AS label
            WHERE revision.run_id = %(run_id)s
        ), matched AS (
            SELECT labels.ori, labels.capture_id, labels.label,
                   COUNT(county.geo_sk) AS match_count,
                   MIN(county.geo_id) AS geo_id,
                   MIN(county.geo_sk) AS geo_sk
            FROM labels
            LEFT JOIN silver_fbi.dim_state_code AS state
                   ON state.state_code = labels.state_code
            LEFT JOIN silver_ref.dim_geo_current AS county
                   ON county.geo_type = 'county'
                  AND county.is_active
                  AND county.state_fips = state.state_fips
                  AND BTRIM(REGEXP_REPLACE(
                          UPPER(county.county_name), %(suffix)s, '')
                      ) = labels.label
            GROUP BY labels.ori, labels.capture_id, labels.label
        )
        """
        + _RELATIONSHIP_COLUMNS
        + """
        SELECT matched.ori, 'county', matched.label,
               CASE WHEN matched.match_count = 1 THEN matched.geo_id END,
               CASE WHEN matched.match_count = 1 THEN matched.geo_sk END,
               CASE WHEN matched.match_count = 1
                    THEN 'reviewed_county_name_crosswalk' END,
               CASE WHEN matched.match_count = 1 THEN 'resolved'
                    WHEN matched.match_count > 1 THEN 'ambiguous'
                    ELSE 'unresolved' END,
               CASE WHEN matched.match_count = 1 THEN 'reviewed'
                    ELSE 'unresolved' END,
               CASE WHEN matched.match_count > 1 THEN 'ambiguous_county_name'
                    WHEN matched.match_count = 0
                    THEN 'canonical_county_absent' END,
               %(effective_start)s, %(effective_end)s, %(vintage)s,
               'fbi_cde_agency_county_label', matched.capture_id,
               %(product_id)s, %(release_key)s
        FROM matched
        """
        + _RELATIONSHIP_CONFLICT,
        {**scope, "suffix": COUNTY_SUFFIX_PATTERN},
    )


def _load_place_relationships(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        _RELATIONSHIP_COLUMNS
        + """
        SELECT DISTINCT ON (revision.ori, crosswalk.place_geo_id)
               revision.ori, 'place', crosswalk.place_name,
               crosswalk.place_geo_id, entity.geo_sk,
               'reviewed_place_crosswalk',
               CASE WHEN entity.geo_sk IS NULL THEN 'unresolved'
                    ELSE 'resolved' END,
               CASE WHEN entity.geo_sk IS NULL THEN 'unresolved'
                    ELSE 'reviewed' END,
               CASE WHEN entity.geo_sk IS NULL
                    THEN 'canonical_geography_absent' END,
               %(effective_start)s, %(effective_end)s, %(vintage)s,
               'reviewed_agency_place_crosswalk:' || crosswalk.crosswalk_version,
               revision.capture_id, %(product_id)s, %(release_key)s
        FROM silver_fbi.agency_revision AS revision
        JOIN silver_fbi.reviewed_place_crosswalk AS crosswalk
          ON crosswalk.ori = revision.ori
         AND crosswalk.effective_start <= %(effective_start)s
         AND (crosswalk.effective_end IS NULL
              OR crosswalk.effective_end >= %(effective_end)s)
        LEFT JOIN silver_ref.dim_geo_entity AS entity
               ON entity.geo_id = crosswalk.place_geo_id
        WHERE revision.run_id = %(run_id)s
        ORDER BY revision.ori, crosswalk.place_geo_id, revision.capture_id
        """
        + _RELATIONSHIP_CONFLICT,
        scope,
    )


def _load_shared_bridges(cursor: Any, *, scope: dict) -> None:
    """Publish resolved relationships into the shared effective-dated bridge."""
    cursor.execute(
        """
        INSERT INTO silver_ref.bridge_geo_relationship_version (
            parent_geo_sk, related_geo_sk, relationship_type,
            geography_vintage, evidence_source, source_snapshot_id
        )
        SELECT DISTINCT ON (agency.geo_sk, relationship.geo_sk, bridge.bridge_type)
               agency.geo_sk, relationship.geo_sk, bridge.bridge_type,
               relationship.geography_vintage, relationship.evidence_source,
               relationship.evidence_capture_id
        FROM silver_fbi.agency_geography_relationship AS relationship
        JOIN silver_fbi.dim_agency AS agency USING (ori)
        CROSS JOIN LATERAL (
            SELECT CASE WHEN relationship.relationship_type = 'place'
                        THEN 'provider_crosswalk' ELSE 'serves' END AS bridge_type
        ) AS bridge
        WHERE relationship.product_id = %(product_id)s
          AND relationship.release_key = %(release_key)s
          AND relationship.resolution_status = 'resolved'
          AND relationship.geo_sk IS NOT NULL
          AND agency.geo_sk IS NOT NULL
        ORDER BY agency.geo_sk, relationship.geo_sk, bridge.bridge_type,
                 relationship.relationship_sk
        ON CONFLICT (
            parent_geo_sk, related_geo_sk, relationship_type, geography_vintage
        ) DO NOTHING
        """,
        scope,
    )


def _load_geography_resolution(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        """
        INSERT INTO silver_ref.geography_resolution (
            provider_source, provider_dataset, source_geo_type, source_code,
            source_label, source_vintage, geo_sk, resolution_method,
            evidence_capture_id, status, reason_code
        )
        SELECT DISTINCT ON (revision.subject_type, revision.subject_code)
               'FBI_UCR', revision.product_id, revision.subject_type,
               revision.subject_code, revision.subject_label, %(vintage)s,
               entity.geo_sk,
               CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
               revision.capture_id,
               CASE WHEN revision.subject_type = 'state'
                         AND state.state_fips IS NULL THEN 'unsupported'
                    WHEN entity.geo_sk IS NULL THEN 'unmapped'
                    ELSE 'resolved' END,
               CASE WHEN revision.subject_type = 'state'
                         AND state.state_fips IS NULL
                    THEN 'undocumented_state_code'
                    WHEN entity.geo_sk IS NULL
                    THEN 'canonical_geography_absent' END
        FROM silver_fbi.observation_revision AS revision
        LEFT JOIN silver_fbi.dim_state_code AS state
               ON revision.subject_type = 'state'
              AND state.state_code = revision.subject_code
        LEFT JOIN silver_ref.dim_geo_entity AS entity
               ON entity.geo_id = CASE revision.subject_type
                      WHEN 'national' THEN 'us:1'
                      WHEN 'state' THEN 'state:' || state.state_fips
                      ELSE 'agency:' || revision.subject_code END
        WHERE revision.run_id = %(run_id)s
        ORDER BY revision.subject_type, revision.subject_code,
                 revision.capture_id, revision.source_row_index
        ON CONFLICT (
            provider_source, provider_dataset, source_geo_type, source_code,
            source_vintage
        ) DO UPDATE SET
            source_label = EXCLUDED.source_label,
            geo_sk = EXCLUDED.geo_sk,
            resolution_method = EXCLUDED.resolution_method,
            evidence_capture_id = EXCLUDED.evidence_capture_id,
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            resolved_at = NOW()
        """,
        scope,
    )


def _load_participation_facts(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        "WITH "
        + _AGENCY_STATUS_CTE
        + """
        INSERT INTO silver_fbi.fact_reporting_participation (
            product_id, release_key, ucr_program, subject_type, subject_code,
            subject_label, source_geo_level, period, period_start, period_end,
            geo_id, geo_sk, geography_status, population,
            participated_population, coverage_percent, coverage_basis,
            participation_status, source_run_id, capture_id, source_row_index,
            transformation_version
        )
        SELECT revision.product_id, revision.release_key, revision.ucr_program,
               revision.subject_type, revision.subject_code,
               revision.subject_label, revision.source_geo_level,
               revision.period, revision.period_start, revision.period_end,
        """
        + _GEO_ID_SQL
        + """, entity.geo_sk, """
        + _GEOGRAPHY_STATUS_SQL
        + """,
               revision.population, revision.participated_population,
               revision.coverage_percent, revision.coverage_basis,
               revision.participation_status, revision.run_id,
               revision.capture_id, revision.source_row_index,
               %(transformation_version)s
        FROM silver_fbi.participation_revision AS revision
        LEFT JOIN silver_fbi.dim_state_code AS state
               ON revision.subject_type = 'state'
              AND state.state_code = revision.subject_code
        LEFT JOIN agency_status
               ON revision.subject_type = 'agency'
              AND agency_status.ori = revision.subject_code
        LEFT JOIN silver_ref.dim_geo_entity AS entity
               ON entity.geo_id = CASE revision.subject_type
                      WHEN 'national' THEN 'us:1'
                      WHEN 'state' THEN 'state:' || state.state_fips
                      ELSE 'agency:' || revision.subject_code END
        WHERE revision.run_id = %(run_id)s
        ON CONFLICT (product_id, release_key, subject_type, subject_code, period)
        DO NOTHING
        """,
        scope,
    )


def _load_observation_facts(cursor: Any, *, scope: dict) -> None:
    cursor.execute(
        "WITH "
        + _AGENCY_STATUS_CTE
        + """
        INSERT INTO silver_fbi.fact_crime_observation (
            product_id, release_key, source_record_id, measure_id,
            subject_type, subject_code, subject_label, source_geo_level,
            period, period_start, period_end, geo_id, geo_sk,
            geography_status, value_source, value, value_status,
            population_denominator, source_run_id, capture_id,
            source_row_index, transformation_version
        )
        SELECT revision.product_id, revision.release_key,
               revision.source_record_id, revision.measure_id,
               revision.subject_type, revision.subject_code,
               revision.subject_label, revision.source_geo_level,
               revision.period, revision.period_start, revision.period_end,
        """
        + _GEO_ID_SQL
        + """, entity.geo_sk, """
        + _GEOGRAPHY_STATUS_SQL
        + """,
               revision.value_source, revision.value, revision.value_status,
               revision.population_denominator, revision.run_id,
               revision.capture_id, revision.source_row_index,
               %(transformation_version)s
        FROM silver_fbi.observation_revision AS revision
        JOIN silver_fbi.fact_reporting_participation AS coverage
          ON coverage.product_id = revision.product_id
         AND coverage.release_key = revision.release_key
         AND coverage.subject_type = revision.subject_type
         AND coverage.subject_code = revision.subject_code
         AND coverage.period = revision.period
        LEFT JOIN silver_fbi.dim_state_code AS state
               ON revision.subject_type = 'state'
              AND state.state_code = revision.subject_code
        LEFT JOIN agency_status
               ON revision.subject_type = 'agency'
              AND agency_status.ori = revision.subject_code
        LEFT JOIN silver_ref.dim_geo_entity AS entity
               ON entity.geo_id = CASE revision.subject_type
                      WHEN 'national' THEN 'us:1'
                      WHEN 'state' THEN 'state:' || state.state_fips
                      ELSE 'agency:' || revision.subject_code END
        WHERE revision.run_id = %(run_id)s
        ON CONFLICT (product_id, release_key, source_record_id) DO NOTHING
        """,
        scope,
    )
    # An observation whose participation companion was quarantined has no
    # coverage interpretation, so it is recorded as quarantined rather than
    # published without one.
    cursor.execute(
        """
        INSERT INTO silver_fbi.slice_quarantine (
            run_id, product_id, release_key, slice_key, source_row_index,
            error_code, error_summary
        )
        SELECT revision.run_id, revision.product_id, revision.release_key,
               revision.subject_type || ':' || revision.subject_code,
               revision.source_row_index, 'coverage_interpretation_missing',
               'no reporting-participation row exists for this subject period'
        FROM silver_fbi.observation_revision AS revision
        LEFT JOIN silver_fbi.fact_crime_observation AS fact
               ON fact.product_id = revision.product_id
              AND fact.release_key = revision.release_key
              AND fact.source_record_id = revision.source_record_id
        WHERE revision.run_id = %(run_id)s AND fact.observation_sk IS NULL
        ON CONFLICT (
            run_id, product_id, release_key, slice_key, source_row_index,
            error_code
        ) DO NOTHING
        """,
        scope,
    )


def _reconcile(cursor: Any, *, scope: dict) -> int:
    cursor.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM silver_fbi.observation_revision
            WHERE run_id = %(run_id)s),
          (SELECT COUNT(*) FROM silver_fbi.participation_revision
            WHERE run_id = %(run_id)s),
          (SELECT COUNT(*) FROM silver_fbi.fact_crime_observation
            WHERE product_id = %(product_id)s AND release_key = %(release_key)s),
          (SELECT COUNT(*) FROM silver_fbi.fact_reporting_participation
            WHERE product_id = %(product_id)s AND release_key = %(release_key)s),
          (SELECT COUNT(*) FROM silver_fbi.slice_quarantine
            WHERE run_id = %(run_id)s
              AND error_code = 'coverage_interpretation_missing')
        """,
        scope,
    )
    observations, coverage, facts, coverage_facts, orphaned = cursor.fetchone()
    if facts + orphaned != observations or coverage_facts != coverage:
        raise FbiReconciliationError("FBI release row reconciliation failed")
    cursor.execute(
        """
        UPDATE silver_fbi.dim_ucr_dataset_release
           SET quarantine_count = (
                   SELECT COUNT(*) FROM silver_fbi.slice_quarantine
                    WHERE run_id = %(run_id)s
               ),
               status = CASE WHEN status = 'published'
                             THEN status ELSE 'silver_ready' END,
               reconciled_at = NOW(), updated_at = NOW()
         WHERE product_id = %(product_id)s AND release_key = %(release_key)s
        """,
        scope,
    )
    cursor.execute(
        """
        UPDATE control.fbi_ucr_release
           SET status = 'silver_ready', updated_at = NOW()
         WHERE run_id = %(run_id)s
        """,
        scope,
    )
    return int(facts)
