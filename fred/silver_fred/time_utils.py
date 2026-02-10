from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

FRED_FREQ_DOC = "https://fred.stlouisfed.org/docs/api/fred/series.html"


def _normalize_frequency(frequency: str | None) -> str:
    if not frequency:
        return ""
    return frequency.strip().upper()


def _frequency_code(frequency: str | None) -> str:
    freq = _normalize_frequency(frequency)
    if freq in {"D", "W", "BW", "M", "Q", "SA", "A"}:
        return freq
    if "DAILY" in freq:
        return "D"
    if "BIWEEK" in freq or "BI-WEEK" in freq or "BI WEEK" in freq:
        return "BW"
    if "WEEK" in freq:
        return "W"
    if "MONTH" in freq:
        return "M"
    if "QUART" in freq:
        return "Q"
    if "SEMIANNUAL" in freq or "SEMI-ANNUAL" in freq:
        return "SA"
    if "ANNUAL" in freq or "YEAR" in freq:
        return "A"
    return ""


def compute_fred_duration(observation_date: date, frequency: str | None) -> tuple[date, date]:
    """
    Compute duration window for a FRED observation date based on frequency.
    """
    code = _frequency_code(frequency)
    if not code:
        logger.warning(
            "Unknown FRED frequency '%s'. Defaulting to daily. See %s",
            frequency,
            FRED_FREQ_DOC,
        )
        return observation_date, observation_date

    if code == "D":
        return observation_date, observation_date

    if code == "W":
        return observation_date - timedelta(days=6), observation_date

    if code == "BW":
        return observation_date - timedelta(days=13), observation_date

    if code == "M":
        month_end = calendar.monthrange(observation_date.year, observation_date.month)[1]
        start = date(observation_date.year, observation_date.month, 1)
        end = date(observation_date.year, observation_date.month, month_end)
        return start, end

    if code == "Q":
        quarter = (observation_date.month - 1) // 3 + 1
        start_month = 1 + (quarter - 1) * 3
        end_month = start_month + 2
        end_day = calendar.monthrange(observation_date.year, end_month)[1]
        start = date(observation_date.year, start_month, 1)
        end = date(observation_date.year, end_month, end_day)
        return start, end

    if code == "SA":
        if observation_date.month <= 6:
            return date(observation_date.year, 1, 1), date(observation_date.year, 6, 30)
        return date(observation_date.year, 7, 1), date(observation_date.year, 12, 31)

    if code == "A":
        return date(observation_date.year, 1, 1), date(observation_date.year, 12, 31)

    logger.warning(
        "Unhandled FRED frequency '%s'. Defaulting to daily. See %s",
        frequency,
        FRED_FREQ_DOC,
    )
    return observation_date, observation_date
