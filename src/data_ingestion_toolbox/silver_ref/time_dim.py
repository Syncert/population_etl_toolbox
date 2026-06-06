# silver_ref/time_dim.py

from __future__ import annotations

import calendar
from datetime import date, datetime, timezone
from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.silver_ref.config import CONFIG


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _resolve_end_date(end_date: Optional[date]) -> date:
    if end_date is not None:
        return end_date
    today = date.today()
    return date(today.year, 12, 31)


def sync_time_dim(
    start_date: date = date(1970, 1, 1),
    end_date: Optional[date] = None,
) -> int:
    """
    Upsert a daily time dimension into silver_ref.dim_time.

    Returns number of rows upserted.
    """
    hook = _get_hook()
    end_date = _resolve_end_date(end_date)

    rows = []
    d = start_date
    while d <= end_date:
        iso = d.isocalendar()
        month_end_day = calendar.monthrange(d.year, d.month)[1]
        is_month_end = d.day == month_end_day
        is_month_start = d.day == 1
        quarter = (d.month - 1) // 3 + 1
        is_quarter_start = d.month in (1, 4, 7, 10) and d.day == 1
        is_quarter_end = d.month in (3, 6, 9, 12) and is_month_end
        is_year_start = d.month == 1 and d.day == 1
        is_year_end = d.month == 12 and d.day == 31

        rows.append(
            {
                "date_key": d,
                "year": d.year,
                "quarter": quarter,
                "month": d.month,
                "day": d.day,
                "day_of_week": d.isoweekday(),
                "day_name": d.strftime("%A"),
                "month_name": d.strftime("%B"),
                "week_of_year": iso.week,
                "is_weekend": d.isoweekday() in (6, 7),
                "is_month_start": is_month_start,
                "is_month_end": is_month_end,
                "is_quarter_start": is_quarter_start,
                "is_quarter_end": is_quarter_end,
                "is_year_start": is_year_start,
                "is_year_end": is_year_end,
                "ingested_at": datetime.now(timezone.utc),
            }
        )
        d = d.replace(day=d.day)  # no-op for clarity
        d = d.fromordinal(d.toordinal() + 1)

    sql = """
        INSERT INTO silver_ref.dim_time (
            date_key, year, quarter, month, day,
            day_of_week, day_name, month_name, week_of_year,
            is_weekend, is_month_start, is_month_end,
            is_quarter_start, is_quarter_end,
            is_year_start, is_year_end,
            ingested_at
        )
        VALUES (
            %(date_key)s, %(year)s, %(quarter)s, %(month)s, %(day)s,
            %(day_of_week)s, %(day_name)s, %(month_name)s, %(week_of_year)s,
            %(is_weekend)s, %(is_month_start)s, %(is_month_end)s,
            %(is_quarter_start)s, %(is_quarter_end)s,
            %(is_year_start)s, %(is_year_end)s,
            %(ingested_at)s
        )
        ON CONFLICT (date_key)
        DO UPDATE SET
            year = EXCLUDED.year,
            quarter = EXCLUDED.quarter,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            day_of_week = EXCLUDED.day_of_week,
            day_name = EXCLUDED.day_name,
            month_name = EXCLUDED.month_name,
            week_of_year = EXCLUDED.week_of_year,
            is_weekend = EXCLUDED.is_weekend,
            is_month_start = EXCLUDED.is_month_start,
            is_month_end = EXCLUDED.is_month_end,
            is_quarter_start = EXCLUDED.is_quarter_start,
            is_quarter_end = EXCLUDED.is_quarter_end,
            is_year_start = EXCLUDED.is_year_start,
            is_year_end = EXCLUDED.is_year_end,
            ingested_at = EXCLUDED.ingested_at;
    """

    with hook.get_conn() as conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, r)
        conn.commit()

    return len(rows)
