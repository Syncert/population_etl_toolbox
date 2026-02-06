# gold/config.py

from __future__ import annotations

from pydantic import BaseModel


class GoldConfig(BaseModel):
    """Configuration for the gold analytics / ML layer."""

    postgres_conn_id: str = "public_data"


CONFIG = GoldConfig()
