"""The fiscal views read BEA by its indexed columns, never by `metric_key`.

Covers: ETL-119 -- the governor fiscal views and the tax burden they read.

`gold_bea.observation_latest.metric_key` is `table_code || ':' || line_code`,
which no index serves: a view joining on it ranks every BEA row of the line in
the country before keeping one place's. On OFFICE_LAB on 2026-11-06 that made
the governor reads 6.7 s and the tax burden read 3.6 s.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
VIEWS = (
    "src/data_ingestion_toolbox/elections/gold_elections/DDL/governor_fiscal.sql",
    "src/data_ingestion_toolbox/census_govfin/gold_census_govfin/DDL/gold_tax_burden.sql",
)


@pytest.mark.parametrize("path", VIEWS)
def test_a_fiscal_view_never_filters_bea_on_the_concatenated_key(path: str) -> None:
    """Covers: ETL-119 — BEA is read through table_code and line_code."""
    sql = re.sub(r"--[^\n]*", "", (REPOSITORY_ROOT / path).read_text(encoding="utf-8"))
    assert "gold_bea.observation_latest" not in sql
    assert not re.search(r"metric_key\s*=\s*'(?:CAINC|SAINC)", sql)
    assert "gold_bea.observation_revision" in sql


def test_the_governor_tax_mix_reads_census_by_place_and_year() -> None:
    """Covers: ETL-119 — the governor tax mix never re-ranks every asslgf row.

    `gold_census_govfin.tax_mix` and `observation_latest` rank by a
    concatenated `metric_key`, so a per-tenure filter on measure never reaches
    `census_govfin_fact_metric_geo_idx`: 512 s for one state on OFFICE_LAB on
    2026-11-06.
    """
    sql = re.sub(
        r"--[^\n]*", "", (REPOSITORY_ROOT / VIEWS[0]).read_text(encoding="utf-8")
    )
    mix = sql[sql.index("VIEW gold_elections.governor_term_tax_mix") :]
    mix = mix[: mix.index("COMMENT ON VIEW")]
    assert "gold_census_govfin.tax_mix" not in mix
    assert "gold_census_govfin.observation_latest" not in mix
    assert "gold_census_govfin.observation_revision" in mix
    assert re.search(r"revision\.geo_id\s*=\s*tenure\.geo_id", mix)
    assert re.search(r"revision\.fiscal_year\s*=\s*tenure\.window_end_year", mix)
