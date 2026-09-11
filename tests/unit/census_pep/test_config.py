"""Census PEP adapter configuration contracts."""

from __future__ import annotations

import os

import pytest

from pathlib import Path

from data_ingestion_toolbox.census_pep import config
from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    parse_captured_pep_values,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


#: The products serving the current decade. They keep their codes, their
#: files, and their parser as the closed decades are registered alongside.
CURRENT_DECADE_DATASETS = {
    "pep_nst_alldata",
    "pep_county_alldata",
    "pep_subcounty",
}

#: One product per closed decade, each with a single immutable release.
HISTORICAL_DATASETS = {
    "pep_county_alldata_2010s",
    "pep_nst_alldata_2010s",
    "pep_county_alldata_2000s",
    "pep_county_intercensal_2000s",
    "pep_county_totals_1990s",
    "pep_county_totals_1980s",
    "pep_county_totals_1970s",
}


def test_config_has_curated_datasets() -> None:
    """Covers: ETL-030 — default config includes curated datasets."""
    assert set(config.CONFIG.datasets) == (
        CURRENT_DECADE_DATASETS | HISTORICAL_DATASETS
    )


def test_config_pepdataset_immutable() -> None:
    """Covers: ETL-030 — PEPDataset is immutable after construction."""
    ds = config.PEPDataset(
        code="test_ds",
        title="Test Dataset",
        release_status="active",
    )
    with pytest.raises(AttributeError, match="immutable"):
        ds.code = "changed"  # type: ignore[attr-defined]


def test_config_pepdataset_hash_equality() -> None:
    """Covers: ETL-030 — PEPDataset hash is based on code."""
    ds1 = config.PEPDataset(code="dup", title="Dup")
    ds2 = config.PEPDataset(code="dup", title="Dup different title")
    assert hash(ds1) == hash(ds2)
    assert ds1 == ds2


def test_config_with_api_key_returns_new_instance() -> None:
    """Covers: ETL-030 — with_api_key returns a new config with key set."""
    new_config = config.CONFIG.with_api_key("test-key-123")
    assert new_config.has_api_key
    assert new_config.get_api_key() == "test-key-123"


def test_config_get_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: ETL-030 — get_api_key falls back to environment variable."""
    monkeypatch.setenv(config.CENSUS_API_KEY_ENV, "env-key-456")
    env_config = config.PEPConfig()
    assert env_config.has_api_key
    assert env_config.get_api_key() == "env-key-456"


def test_config_get_api_key_raises_without_key() -> None:
    """Covers: ETL-030 — get_api_key raises when no key available."""
    # Ensure the env var is not set
    env_key = config.CENSUS_API_KEY_ENV
    if env_key in os.environ:
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.delenv(env_key, raising=False)

    no_key_config = config.PEPConfig(_api_key=None)
    assert not no_key_config.has_api_key
    with pytest.raises(ValueError, match=env_key):
        no_key_config.get_api_key()


def test_config_frozen_dataclass() -> None:
    """Covers: ETL-030 — PEPConfig is a frozen dataclass."""
    cfg = config.PEPConfig(request_timeout=60.0, max_concurrency=8)
    with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
        cfg.request_timeout = 10.0  # type: ignore[misc]


def test_config_default_values() -> None:
    """Covers: ETL-030 — PEPConfig has sensible defaults."""
    cfg = config.PEPConfig()
    assert cfg.request_timeout == 60.0
    assert cfg.max_concurrency == 2
    assert cfg.airflow_pool == "census_api"


def test_config_curated_dataset_geography_levels() -> None:
    """Covers: ETL-030 — curated products declare official geographies."""
    nst = config.CONFIG.datasets["pep_nst_alldata"]
    assert nst.geography_levels == frozenset(
        {"national", "region", "division", "state"}
    )
    assert nst.summary_levels == frozenset({"010", "020", "030", "040"})

    county = config.CONFIG.datasets["pep_county_alldata"]
    assert county.geography_levels == frozenset({"state", "county"})
    assert county.summary_levels == frozenset({"040", "050"})

    subcounty = config.CONFIG.datasets["pep_subcounty"]
    assert "place" in subcounty.geography_levels
    assert subcounty.summary_levels == frozenset(
        {"040", "050", "061", "071", "157", "162", "170", "172"}
    )


def test_config_curated_dataset_variables() -> None:
    """Covers: ETL-030 — curated datasets use official variable families."""
    nst = config.CONFIG.datasets["pep_nst_alldata"]
    expected_vars = {
        "ESTIMATESBASE",
        "POPESTIMATE",
        "BIRTHS",
        "DEATHS",
        "NATURALCHG",
        "DOMESTICMIG",
        "INTERNATIONALMIG",
        "NPOPCHG",
        "RNETMIG",
    }
    assert expected_vars.issubset(nst.variables)


def test_config_freezes_official_current_bulk_products() -> None:
    """Covers: ETL-030 — PEP scope uses official Vintage 2025 bulk products."""
    for code in CURRENT_DECADE_DATASETS:
        dataset = config.CONFIG.datasets[code]
        assert dataset.transport == "bulk_csv"
        assert dataset.parser_version == "census-pep-bulk-csv-v1"
        assert dataset.decennial_base == 2020
        assert dataset.era == "2020s"
    for dataset in config.CONFIG.datasets.values():
        assert dataset.transport in {"bulk_csv", "bulk_text", "bulk_zip"}
        assert dataset.release_page_url.startswith("https://www.census.gov/")
        assert dataset.native_grain in dataset.summary_levels


def test_config_registers_one_product_per_closed_decade() -> None:
    """Covers: ETL-043 — each closed decade is its own registered product."""
    eras = {code: config.CONFIG.datasets[code].era for code in HISTORICAL_DATASETS}
    assert eras == {
        "pep_county_alldata_2010s": "2010s",
        "pep_nst_alldata_2010s": "2010s",
        "pep_county_alldata_2000s": "2000s",
        "pep_county_intercensal_2000s": "2000s",
        "pep_county_totals_1990s": "1990s",
        "pep_county_totals_1980s": "1980s",
        "pep_county_totals_1970s": "1970s",
    }
    bases = {config.CONFIG.datasets[c].decennial_base for c in HISTORICAL_DATASETS}
    assert bases == {2010, 2000, 1990, 1980, 1970}
    # A file the Bureau published after the following census closes an
    # earlier decade, so its observations end before the vintage on it.
    intercensal = {
        release.dataset_code
        for release in config.CONFIG.releases
        if release.series_kind == "intercensal"
    }
    assert intercensal == {
        "pep_county_intercensal_2000s",
        "pep_county_totals_1980s",
        "pep_county_totals_1970s",
    }
    for release in config.CONFIG.releases:
        if release.dataset_code in intercensal:
            assert release.observation_end_year < release.vintage_year
    # A derived total says so, so it is never read as one the Bureau printed.
    assert config.CONFIG.datasets["pep_county_totals_1990s"].derivation
    # A closed decade is published once, so its product carries one release.
    for code in HISTORICAL_DATASETS:
        releases = [r for r in config.CONFIG.releases if r.dataset_code == code]
        assert len(releases) == 1
        assert releases[0].status == "published"


def test_config_has_operational_nonsecret_defaults() -> None:
    """Covers: ETL-030 — PEP runtime defaults are validated without credentials."""
    assert config.CONFIG.postgres_conn_id == "public_data"
    assert config.CONFIG.airflow_pool == "census_api"
    assert config.CONFIG.request_timeout == 60.0
    assert config.CONFIG.max_concurrency == 2


def test_config_separates_current_and_prior_release_vintages() -> None:
    """Covers: ETL-030 — each PEP product retains current and prior releases."""
    release_keys = {
        (release.dataset_code, release.vintage_year)
        for release in config.CONFIG.releases
    }
    assert {key for key in release_keys if key[0] in CURRENT_DECADE_DATASETS} == {
        ("pep_nst_alldata", 2024),
        ("pep_nst_alldata", 2025),
        ("pep_county_alldata", 2024),
        ("pep_county_alldata", 2025),
        ("pep_subcounty", 2024),
        ("pep_subcounty", 2025),
    }
    # The current decade still begins at its own 2020 estimates base.
    assert all(
        release.observation_start_year == 2020
        for release in config.CONFIG.releases
        if release.dataset_code in CURRENT_DECADE_DATASETS
    )
    # Every postcensal release, of any decade, ends at its own vintage.
    assert all(
        release.observation_end_year == release.vintage_year
        for release in config.CONFIG.releases
        if release.series_kind == "postcensal"
    )
    # The observation range is read from the release rather than assumed:
    # each closed decade starts where its own estimates base does.
    starts = {
        release.dataset_code: release.observation_start_year
        for release in config.CONFIG.releases
    }
    assert starts["pep_county_alldata_2010s"] == 2010
    assert starts["pep_county_alldata_2000s"] == 2000


# ---------------------------------------------------------------------------
# ETL-043 — the release contract across decades
# ---------------------------------------------------------------------------


def _release_kwargs(**overrides: object) -> dict[str, object]:
    """A valid postcensal release, so each test varies exactly one thing."""
    base: dict[str, object] = {
        "dataset_code": "pep_county_alldata",
        "vintage_year": 2025,
        "product_code": "TEST-PRODUCT",
        "data_url": "https://www2.census.gov/programs-surveys/popest/x.csv",
        "layout_url": "https://www2.census.gov/programs-surveys/popest/x.pdf",
        "release_date": "2026-01-01",
        "observation_start_year": 2020,
        "observation_end_year": 2025,
        "geography_basis_date": "2025-01-01",
        "schema_version": "test-product",
        "status": "published",
    }
    base.update(overrides)
    return base


def test_release_accepts_an_intercensal_range_that_ends_before_its_vintage() -> None:
    """Covers: ETL-043 — an intercensal release closes an earlier decade.

    It is published once the following census can close the decade, so its
    last observation year is years behind the vintage carrying it. The old
    contract rejected exactly this shape.
    """
    release = config.PEPRelease(
        **_release_kwargs(
            series_kind="intercensal",
            vintage_year=2012,
            observation_start_year=2000,
            observation_end_year=2010,
        )
    )
    assert release.observation_end_year == 2010
    assert release.vintage_year == 2012


def test_release_still_requires_a_postcensal_series_to_end_at_its_vintage() -> None:
    """Covers: ETL-043 — the vintage equality holds where it is true."""
    with pytest.raises(ValueError, match="postcensal"):
        config.PEPRelease(**_release_kwargs(observation_end_year=2023))


def test_release_rejects_observations_after_its_own_vintage() -> None:
    """Covers: ETL-043 — no release may observe beyond its publication."""
    with pytest.raises(ValueError, match="ends after its own vintage"):
        config.PEPRelease(
            **_release_kwargs(
                series_kind="intercensal",
                vintage_year=2010,
                observation_end_year=2011,
            )
        )


def test_release_rejects_a_reversed_range_and_an_unofficial_host() -> None:
    """Covers: ETL-030 — the original release guards still hold."""
    with pytest.raises(ValueError, match="reversed"):
        config.PEPRelease(
            **_release_kwargs(
                series_kind="intercensal",
                observation_start_year=2026,
                observation_end_year=2025,
            )
        )
    with pytest.raises(ValueError, match="official Census host"):
        config.PEPRelease(**_release_kwargs(data_url="https://example.com/x.csv"))


def test_single_file_release_is_captured_from_one_url() -> None:
    """Covers: ETL-043 — an unpartitioned product yields one source file."""
    release = config.PEPRelease(**_release_kwargs())
    assert release.source_files() == (
        (None, "https://www2.census.gov/programs-surveys/popest/x.csv"),
    )


def test_partitioned_release_yields_one_source_file_per_partition() -> None:
    """Covers: ETL-043 — a product split across files declares its parts.

    The Bureau splits some products one file per state. Each part is
    captured separately so a part that fails to answer is a missing file
    rather than a silently shorter release.
    """
    release = config.PEPRelease(
        **_release_kwargs(
            data_url=(
                "https://www2.census.gov/programs-surveys/popest/"
                "co-est00int-alldata-{partition}.csv"
            ),
            partitions=("01", "02"),
        )
    )
    assert release.source_files() == (
        (
            "01",
            "https://www2.census.gov/programs-surveys/popest/"
            "co-est00int-alldata-01.csv",
        ),
        (
            "02",
            "https://www2.census.gov/programs-surveys/popest/"
            "co-est00int-alldata-02.csv",
        ),
    )


def test_partitioned_release_requires_a_templated_url_and_unique_parts() -> None:
    """Covers: ETL-043 — a partition set that cannot address its files fails."""
    with pytest.raises(ValueError, match="template its data URL"):
        config.PEPRelease(**_release_kwargs(partitions=("01", "02")))
    with pytest.raises(ValueError, match="duplicate partition"):
        config.PEPRelease(
            **_release_kwargs(
                data_url=(
                    "https://www2.census.gov/programs-surveys/popest/x-{partition}.csv"
                ),
                partitions=("01", "01"),
            )
        )


def test_current_decade_releases_are_unchanged_by_the_generalised_helper() -> None:
    """Covers: ETL-043 — widening the contract moved no published value.

    Every 2020s release is re-expressed through the helper that now reads
    the observation range from its arguments. The resulting contracts must
    be identical to the ones the pipeline already serves.
    """
    expected = {
        ("pep_nst_alldata", 2024): (
            "NST-EST2024-ALLDATA",
            2020,
            2024,
            "2024-01-01",
            "archived",
        ),
        ("pep_nst_alldata", 2025): (
            "NST-EST2025-ALLDATA",
            2020,
            2025,
            "2025-01-01",
            "published",
        ),
        ("pep_county_alldata", 2024): (
            "CO-EST2024-ALLDATA",
            2020,
            2024,
            "2024-01-01",
            "archived",
        ),
        ("pep_county_alldata", 2025): (
            "CO-EST2025-ALLDATA",
            2020,
            2025,
            "2025-01-01",
            "published",
        ),
        ("pep_subcounty", 2024): (
            "SUB-EST2024",
            2020,
            2024,
            "2024-01-01",
            "archived",
        ),
        ("pep_subcounty", 2025): (
            "SUB-EST2025",
            2020,
            2025,
            "2025-01-01",
            "published",
        ),
    }
    actual = {
        (release.dataset_code, release.vintage_year): (
            release.product_code,
            release.observation_start_year,
            release.observation_end_year,
            release.geography_basis_date,
            release.status,
        )
        for release in config.CONFIG.releases
        if release.dataset_code in CURRENT_DECADE_DATASETS
    }
    assert actual == expected
    # The vintage placeholder is still the only one filled for these.
    for release in config.CONFIG.releases:
        assert "{vintage}" not in release.data_url
        assert "{partition}" not in release.data_url


def test_intercensal_product_restores_codes_its_file_omits() -> None:
    """Covers: ETL-045 — a FIPS code of known width is padded, not guessed.

    The intercensal county file prints Alabama as state 1 at summary level
    40 where every other product writes 01 and 040. Read as printed, its
    rows would resolve against no geography at all.
    """
    release = next(
        item
        for item in config.CONFIG.releases
        if item.dataset_code == "pep_county_intercensal_2000s"
    )
    rows = parse_captured_pep_values(
        (FIXTURES / "co_intercensal_2000s.csv").read_bytes(), release=release
    )
    alabama = [row for row in rows if row["summary_level"] == "040"]
    autauga = [row for row in rows if row["summary_level"] == "050"]

    assert alabama and autauga
    assert {row["state_fips_source"] for row in alabama} == {"01"}
    assert {row["county_fips_source"] for row in alabama} == {"000"}
    assert {row["county_fips_source"] for row in autauga} == {"001"}
    # The padding is declared per product, not applied to every file.
    assert config.CONFIG.datasets["pep_county_intercensal_2000s"].pads_geography_codes
    assert not config.CONFIG.datasets["pep_county_alldata_2000s"].pads_geography_codes


def test_intercensal_publication_revises_the_postcensal_estimates() -> None:
    """Covers: ETL-044 — the two 2000s products disagree, as they should.

    The intercensal series closes the decade against both enumerations, so
    it supersedes the postcensal estimates rather than repeating them.
    Registering both is what makes the revision visible.
    """

    def alabama_2005(dataset_code: str, fixture: str) -> str:
        release = next(
            item for item in config.CONFIG.releases if item.dataset_code == dataset_code
        )
        rows = parse_captured_pep_values(
            (FIXTURES / fixture).read_bytes(), release=release
        )
        return next(
            row["value_source"]
            for row in rows
            if row["metric_code"] == "POPESTIMATE"
            and row["observation_year"] == 2005
            and row["summary_level"] == "040"
        )

    postcensal = alabama_2005("pep_county_alldata_2000s", "co_2000s.csv")
    intercensal = alabama_2005(
        "pep_county_intercensal_2000s", "co_intercensal_2000s.csv"
    )
    assert postcensal == "4545049"
    assert intercensal == "4569805"
