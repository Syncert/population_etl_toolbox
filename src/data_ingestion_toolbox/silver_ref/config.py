# data_ingestion_toolbox/silver_ref/config.py

from __future__ import annotations

from pydantic import BaseModel


class SilverRefConfig(BaseModel):
    """Configuration for silver_ref utilities."""

    postgres_conn_id: str = "public_data"


CONFIG = SilverRefConfig()
