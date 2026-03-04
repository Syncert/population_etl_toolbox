from __future__ import annotations

from pydantic import BaseModel


class GoldConfig(BaseModel):
    postgres_conn_id: str = "public_data"
    # Default lookback window in months for incremental updates
    default_lookback_months: int = 3


CONFIG = GoldConfig()
