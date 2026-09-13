"""LAUS publishes per measure, every other BLS program per series.

Covers: ETL-048 — a LAUS series id codes a program, an area, and a measure, so
series-level publication gave 13,261 single-place metrics and no BLS metric
spanning geographies. The explorer's map, distribution bins, and comparison
routes are capability-driven and therefore had nothing to draw. These tests
pin the identity mapping and the DDL branches that apply it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLD_DDL = REPO_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql"
PUBLISHER_DDL = REPO_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/publisher.sql"
GOLD_TRANSFORM = REPO_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/transform.py"

#: measure_code -> (metric_key, display name, units, value_type)
EXPECTED_MEASURES = {
    "03": ("LAU:UNEMP_RATE", "Unemployment rate", "Percent", "RATE"),
    "04": ("LAU:UNEMP_LEVEL", "Unemployment level", "Persons", "LEVEL"),
    "05": ("LAU:EMP_LEVEL", "Employment level", "Persons", "LEVEL"),
    "06": ("LAU:LABOR_FORCE", "Labor force level", "Persons", "LEVEL"),
    "07": ("LAU:EMP_POP_RATIO", "Employment-population ratio", "Percent", "RATIO"),
    "08": ("LAU:LFPR", "Labor force participation rate", "Percent", "RATE"),
    "09": ("LAU:CNIP", "Civilian noninstitutional population", "Persons", "LEVEL"),
}

_SEED_ROW = re.compile(
    r"\('LA', '(?P<measure_code>\d{2})', '(?P<metric_key>[A-Z:_]+)', "
    r"'(?P<display_name>[^']+)', '(?P<units>[^']+)', '(?P<value_type>[A-Z]+)'\)"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_transform_seeds_exactly_the_seven_laus_measures() -> None:
    """Covers: ETL-048 — the published measure identities are the LAUS seven."""
    seeded = {
        match.group("measure_code"): (
            match.group("metric_key"),
            match.group("display_name"),
            match.group("units"),
            match.group("value_type"),
        )
        for match in _SEED_ROW.finditer(_read(GOLD_TRANSFORM))
    }
    assert seeded == EXPECTED_MEASURES


def test_measure_dimension_holds_no_non_laus_program() -> None:
    """Covers: ETL-048 — CES, CPI, JOLTS, and CPS keep their series identity.

    The serving refresh and the publisher both branch on membership of
    ``dim_bls_measure``, so seeding another program here would silently retire
    that program's series codes.
    """
    seed = _read(GOLD_TRANSFORM).split("INSERT INTO gold_bls.dim_bls_measure", 1)[1]
    seed = seed.split("ON CONFLICT", 1)[0]
    programs = set(re.findall(r"\('([A-Z]{2})',", seed))
    assert programs == {"LA"}


def test_serving_refresh_prefers_the_measure_identity_for_mapped_programs() -> None:
    """Covers: ETL-048 — mapped rows publish the measure code, others the series."""
    sql = _read(GOLD_DDL)
    assert "COALESCE('BLS:' || bm.metric_key, 'BLS:' || bs.series_id)" in sql
    assert (
        "COALESCE(bm.metric_display_name, bs.gold_metric_name, bs.series_title)" in sql
    )
    assert "LEFT JOIN gold_bls.dim_bls_measure bm" in sql
    assert "AND bm.measure_code = b.measure_code" in sql


def test_serving_refresh_keeps_the_series_id_on_every_row() -> None:
    """Covers: ETL-048 — lineage back to the BLS series survives the change."""
    sql = _read(GOLD_DDL)
    assert "series_id                  TEXT NOT NULL," in sql
    assert "        bs.series_id,\n" in sql


def test_latest_relation_keys_on_geography_series_and_metric() -> None:
    """Covers: ETL-048 — one latest row per geography per measure.

    LAUS is unadjusted only at both grains, so a geography has exactly one
    series per measure and this key yields exactly one latest row for it.
    """
    sql = _read(GOLD_DDL)
    procedure = sql.split(
        "CREATE OR REPLACE PROCEDURE gold_bls.refresh_mv_bls_latest(", 1
    )[1]
    assert "SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)" in procedure


def test_publisher_reads_laus_grains_from_the_served_rows() -> None:
    """Covers: ETL-048, DB-036 — grains are aggregated from what is served.

    Aggregated rather than declared as a constant (ETL-048), and aggregated
    from `mv_bls_latest` rather than the fact view over silver (DB-036): a
    grain published here is one the API can answer, and the fact view's rows
    advance at silver ingest, before the serving refresh.
    """
    sql = _read(PUBLISHER_DDL)
    export = sql.split("CREATE OR REPLACE VIEW gold_bls.measure_export AS", 1)[1]
    export = export.split("CREATE OR REPLACE VIEW gold_bls.metric_publisher AS", 1)[0]

    assert "ARRAY_AGG(DISTINCT UPPER(latest.geo_level)" in export
    assert "FROM gold_bls.mv_bls_latest AS latest" in export
    assert "ARRAY['STATE']" not in export
    assert "ARRAY['COUNTY']" not in export


def test_publisher_emits_measure_rows_and_excludes_their_series() -> None:
    """Covers: ETL-048, DB-036 — a measure-identified row publishes once.

    Per `(program_code, measure_code)`, which is how the serving refresh
    assigns identity: `COALESCE('BLS:' || measure.metric_key, 'BLS:' ||
    series.series_id)`. Excluding whole *programs* instead meant an LA
    measure code `dim_bls_measure` does not hold served rows under a series
    identity the publisher never published (DB-036).
    """
    sql = _read(PUBLISHER_DDL)
    assert "'measure'::TEXT," in sql
    assert "JOIN gold_bls.dim_bls_measure AS measure" in sql
    assert "AND measure.measure_code = fact.measure_code" in sql
    # A series with no rows at all is the one case the program still decides:
    # an empty-answer LAUS series must not become a single-place metric.
    assert (
        "OR series.program_code NOT IN (\n"
        "            SELECT DISTINCT program_code FROM gold_bls.dim_bls_measure\n"
        "        )" in sql
    )
    assert "UNION ALL" in sql


def test_publisher_keys_measure_lineage_on_the_measure() -> None:
    """Covers: ETL-048 — physical lineage points at the measure, not a series."""
    sql = _read(PUBLISHER_DDL)
    assert (
        "JSONB_BUILD_OBJECT('schema', 'gold_bls', 'relation', "
        "'fact_bls_observation', 'key', export.source_object_key)" in sql
    )
