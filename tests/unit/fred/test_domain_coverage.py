"""ETL unit tests: FRED domain ownership.

Covers ETL-015 (every curated series belongs to exactly one configured
domain; configured order is stable; no duplicate or unclassified series).
"""

import pytest

from data_ingestion_toolbox.fred.config import CONFIG, FredConfig


@pytest.mark.unit
class TestFredDomainOwnership:
    """ETL-015: FRED domain classification is complete and non-overlapping."""

    def test_configured_series_by_domain_succeeds(self) -> None:
        result = CONFIG.configured_series_by_domain()
        assert isinstance(result, dict)

    def test_every_curated_series_belongs_to_exactly_one_domain(self) -> None:
        series_by_domain = CONFIG.configured_series_by_domain()
        all_classified = [s for series in series_by_domain.values() for s in series]
        # No duplicates across domains
        assert len(all_classified) == len(set(all_classified))

    def test_classified_series_matches_curated_list(self) -> None:
        series_by_domain = CONFIG.configured_series_by_domain()
        classified = {s for series in series_by_domain.values() for s in series}
        curated = set(CONFIG.curated_series_ids)
        assert classified == curated

    def test_domain_order_is_stable(self) -> None:
        result1 = CONFIG.configured_series_by_domain()
        result2 = CONFIG.configured_series_by_domain()
        assert list(result1.keys()) == list(result2.keys())
        for domain in result1:
            assert result1[domain] == result2[domain]

    def test_no_empty_domains(self) -> None:
        series_by_domain = CONFIG.configured_series_by_domain()
        for domain, series in series_by_domain.items():
            assert len(series) > 0, f"Domain '{domain}' has no series"

    def test_duplicate_curated_series_raises(self) -> None:
        bad_config = FredConfig(
            curated_series_ids=CONFIG.curated_series_ids + ["UNRATE"],
            curated_by_domain={**CONFIG.curated_by_domain},
            domains=list(CONFIG.domains),
        )
        with pytest.raises(ValueError, match="duplicate_curated"):
            bad_config.configured_series_by_domain()

    def test_series_in_two_domains_raises(self) -> None:
        bad_by_domain = {k: list(v) for k, v in CONFIG.curated_by_domain.items()}
        # Put UNRATE into a second domain
        first_non_labor = [d for d in CONFIG.domains if d != "labor_cycle"][0]
        bad_by_domain[first_non_labor] = bad_by_domain[first_non_labor] + ["UNRATE"]
        bad_config = FredConfig(
            curated_series_ids=CONFIG.curated_series_ids + ["UNRATE"],
            curated_by_domain=bad_by_domain,
            domains=list(CONFIG.domains),
        )
        with pytest.raises(ValueError, match="multiply classified|conflicts"):
            bad_config.configured_series_by_domain()

    def test_duplicate_domain_name_raises(self) -> None:
        bad_config = FredConfig(
            domains=list(CONFIG.domains) + ["labor_cycle"],
            curated_series_ids=CONFIG.curated_series_ids,
            curated_by_domain=CONFIG.curated_by_domain,
        )
        with pytest.raises(ValueError, match="unique"):
            bad_config.configured_series_by_domain()
