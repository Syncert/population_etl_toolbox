from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)

ACS_EST_DOC = "https://www.census.gov/programs-surveys/acs/guidance/estimates.html"


def compute_acs_duration(dataset: str, estimate_year: int) -> tuple[date, date]:
    """
    Compute ACS estimate window based on dataset type.
    """
    dataset = (dataset or "").lower()
    if dataset == "acs1":
        return date(estimate_year, 1, 1), date(estimate_year, 12, 31)
    if dataset == "acs5":
        return date(estimate_year - 4, 1, 1), date(estimate_year, 12, 31)

    logger.warning(
        "Unknown ACS dataset '%s'. Defaulting to 1-year window. See %s",
        dataset,
        ACS_EST_DOC,
    )
    return date(estimate_year, 1, 1), date(estimate_year, 12, 31)
