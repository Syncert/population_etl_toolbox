"""Bounded live BLS ingestion contracts using the disposable database."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import geography, ingest, metadata

pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.external,
    pytest.mark.slow,
]


def _seed_published_laus_state_series(
    postgres_connection_factory: Callable[[], connection],
) -> str:
    """Seed one currently published Wisconsin LAUS series as test metadata."""
    frame = metadata.read_bls_tsv(f"{metadata.BASE_URL}la/la.series")
    candidates = [
        record
        for record in metadata.process_series_data(frame, "la")
        if str(record.get("area_code", "")).strip().startswith("ST55")
        and str(record.get("seasonal", "")).strip() == "U"
        and str(record.get("measure_code", "")).strip()
        in ingest.CONFIG.curated_by_program["la"]
    ]
    assert candidates, "live BLS LAUS metadata has no curated Wisconsin state series"
    candidate = candidates[0]
    series_id = str(candidate["series_id"]).strip()

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure,
                    area_code, area_text, raw_metadata
                ) VALUES ('la', %s, %s, 'U', %s, %s, %s, '{}'::JSONB)
                ON CONFLICT (program, series_id) DO NOTHING
                """,
                (
                    series_id,
                    str(candidate.get("series_title", "")).strip(),
                    str(candidate.get("measure_code", "")).strip(),
                    str(candidate.get("area_code", "")).strip(),
                    str(candidate.get("area_text", "")).strip(),
                ),
            )
        writer.commit()
    finally:
        writer.close()
    return series_id


@pytest.mark.parametrize(
    ("program", "geo_level", "state_fips"),
    [
        ("la", "state", "55"),
        ("ln", None, None),
        ("ce", None, None),
        ("cu", None, None),
        ("jt", None, None),
    ],
)
def test_bounded_live_bls_program_loads_source_appropriate_rows(
    program: str,
    geo_level: str | None,
    state_fips: str | None,
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: EXT-007 — each promised BLS program reaches production raw storage."""
    monkeypatch.setattr(ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(geography, "_get_pg_connection", postgres_connection_factory)
    laus_series_id = (
        _seed_published_laus_state_series(postgres_connection_factory)
        if program == "la"
        else None
    )
    try:
        loaded = ingest.ingest_slice(
            program=program,
            start_year=2023,
            end_year=2023,
            geo_level=geo_level,
            state_fips=state_fips,
        )
        assert loaded > 0
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT series_id),
                           BOOL_AND(year = 2023), BOOL_AND(value IS NOT NULL)
                    FROM raw_bls.bls_long WHERE program = %s AND year = 2023
                    """,
                    (program,),
                )
                count, series_count, year_ok, values_ok = cursor.fetchone()
                assert count == loaded
                assert series_count > 0 and year_ok and values_ok
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM raw_bls.bls_long WHERE program = %s AND year = 2023",
                    (program,),
                )
                if laus_series_id:
                    cursor.execute(
                        "DELETE FROM raw_bls.bls_series "
                        "WHERE program = 'la' AND series_id = %s",
                        (laus_series_id,),
                    )
            cleanup.commit()
        finally:
            cleanup.close()
