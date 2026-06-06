from functools import lru_cache
from urllib.parse import quote_plus
import os


class Settings:
    def __init__(self) -> None:
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = int(os.getenv("DB_PORT", "5432"))
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "")
        self.db_name = os.getenv("DB_NAME", "population_etl")

        self.api_title = os.getenv("API_TITLE", "Population ETL Toolbox API")
        self.api_version = os.getenv("API_VERSION", "0.1.0")
        self.api_description = os.getenv(
            "API_DESCRIPTION",
            "Catalog and observation endpoints for curated population ETL outputs.",
        )

    @property
    def sqlalchemy_url(self) -> str:
        encoded_password = quote_plus(self.db_password)
        return (
            f"postgresql+psycopg2://{self.db_user}:{encoded_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
