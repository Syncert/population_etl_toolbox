from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.db.connection import get_engine


def execute_query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_settings()
    if settings.use_mock_data:
        return []

    try:
        with get_engine().connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row) for row in result.mappings().all()]
    except SQLAlchemyError:
        return []
