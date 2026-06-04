"""Gold refresh orchestration wrapper."""

from census_acs.gold_census.transform import refresh_acs_elements
from bls.gold_bls.transform import refresh_bls_elements
from fred.gold_fred.transform import refresh_fred_elements


def refresh_gold_views() -> None:
    """Refresh first-pass gold serving views/tables across sources."""
    refresh_acs_elements()
    refresh_bls_elements()
    refresh_fred_elements()
