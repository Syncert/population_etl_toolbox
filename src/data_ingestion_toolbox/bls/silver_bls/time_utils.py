from __future__ import annotations

import calendar
import logging
from datetime import date

logger = logging.getLogger(__name__)

BLS_PERIOD_DOC = "https://download.bls.gov/pub/time.series/overview.txt"


def parse_bls_period_to_date(year: int, period: str) -> tuple[date, date, date]:
    """
    Parse BLS period code into (period_date, duration_start, duration_end).
    """
    if not period or len(period) < 3:
        logger.warning(
            "Unknown BLS period '%s' for year=%s. Defaulting to annual. See %s",
            period,
            year,
            BLS_PERIOD_DOC,
        )
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        return end, start, end

    code = period.upper()

    if code.startswith("M") and code[1:3].isdigit():
        month = int(code[1:3])
        if 1 <= month <= 12:
            end_day = calendar.monthrange(year, month)[1]
            start = date(year, month, 1)
            end = date(year, month, end_day)
            return end, start, end

    if code.startswith("Q") and code[1:3].isdigit():
        quarter = int(code[1:3])
        if 1 <= quarter <= 4:
            start_month = 1 + (quarter - 1) * 3
            end_month = start_month + 2
            end_day = calendar.monthrange(year, end_month)[1]
            start = date(year, start_month, 1)
            end = date(year, end_month, end_day)
            return end, start, end

    if code == "S01":
        return date(year, 6, 30), date(year, 1, 1), date(year, 6, 30)
    if code == "S02":
        return date(year, 12, 31), date(year, 7, 1), date(year, 12, 31)

    if code == "A01":
        return date(year, 12, 31), date(year, 1, 1), date(year, 12, 31)

    logger.warning(
        "Unknown BLS period '%s' for year=%s. Defaulting to annual. See %s",
        period,
        year,
        BLS_PERIOD_DOC,
    )
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    return end, start, end
