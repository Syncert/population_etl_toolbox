# silver/config.py

from __future__ import annotations

from pydantic import BaseModel


class SilverConfig(BaseModel):
    """Configuration for the silver analytics layer."""

    postgres_conn_id: str = "public_data"


CONFIG = SilverConfig()
