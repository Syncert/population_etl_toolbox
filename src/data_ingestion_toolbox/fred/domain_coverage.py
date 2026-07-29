from __future__ import annotations

from typing import Any

from data_ingestion_toolbox.fred.config import CONFIG


def assert_configured_domain_coverage(
    hook: Any,
    series_by_domain: dict[str, list[str]] | None = None,
) -> None:
    """Fail when any configured series is absent from silver or served gold."""
    if series_by_domain is None:
        series_by_domain = CONFIG.configured_series_by_domain()

    missing_by_layer: dict[str, dict[str, list[str]]] = {}

    with hook.get_conn() as conn, conn.cursor() as cur:
        for domain, expected_series in series_by_domain.items():
            cur.execute(
                """
                SELECT DISTINCT series_id
                FROM silver_fred.fact_economic_indicators
                WHERE domain = %s
                  AND series_id = ANY(%s);
                """,
                (domain, expected_series),
            )
            silver_series = {row[0] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT DISTINCT series_id
                FROM gold_fred.rpt_fred_observations
                WHERE series_id = ANY(%s);
                """,
                (expected_series,),
            )
            gold_series = {row[0] for row in cur.fetchall()}

            expected = set(expected_series)
            silver_missing = sorted(expected - silver_series)
            gold_missing = sorted(expected - gold_series)
            if silver_missing or gold_missing:
                missing_by_layer[domain] = {
                    "silver": silver_missing,
                    "gold": gold_missing,
                }

    if missing_by_layer:
        raise ValueError(
            "Configured FRED domain coverage is incomplete: "
            f"{missing_by_layer}"
        )
