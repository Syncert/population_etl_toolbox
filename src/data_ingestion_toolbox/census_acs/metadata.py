# data_ingestion_toolbox/census_acs/metadata.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import logging
import psycopg2
import re

# Adjust this import path to wherever db_connection.py lives in your project
from data_ingestion_toolbox.utility.db_connection import (
    PostgresConnectionFactory,
    PostgresConnectionDetails,
    warehouse_database,
)

from .config import CENSUS_DATASETS, CONFIG, census_api_url
from .decennial import crosswalk


DATA_JSON_URL = "https://api.census.gov/data.json"

# Which database inside the Postgres instance do you want to use?
# Change this if your metadata lives somewhere else.
# Overridable so self-contained stacks can point at their own warehouse
# database; production deployments default to the shared "public_data".
_TARGET_DATABASE = warehouse_database()

# When running inside Airflow, you can let CONFIG.postgres_conn_id drive the
# connection. In local dev (no Airflow), this will be None and the factory
# will fall back to POSTGRES_* env vars.
_AIRFLOW_CONN_ID: Optional[str] = getattr(CONFIG, "postgres_conn_id", None)

logger = logging.getLogger(__name__)


def _get_pg_conn_details() -> PostgresConnectionDetails:
    """
    Get Postgres connection details from either:

    - Airflow connection (if _AIRFLOW_CONN_ID is set and Airflow is installed)
    - Environment variables POSTGRES_HOST, POSTGRES_PORT, etc. (local dev)
    """
    return PostgresConnectionFactory.auto(
        conn_id=_AIRFLOW_CONN_ID,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the shared connection factory.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


#: Census datasets the adapter reads beside the ACS, by their catalog
#: identifier: a census is one fixed release, not a yearly series, so it is
#: named rather than matched by title (decennial-2000-and-1990-history).
CENSUS_CATALOG_IDS: Dict[str, str] = {
    "DECENNIALSF12000": "dec2000sf1",
    "DECENNIALSF32000": "dec2000sf3",
}


def _catalog_id(identifier: str) -> str:
    return identifier.rstrip("/").rsplit("/", 1)[-1]


def fetch_acs_datasets_from_data_json() -> List[Dict]:
    """
    Fetch dataset metadata from data.json and filter to ACS 1- and 5- year datasets.
    Uses regex to match flexible title patterns.
    """
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(DATA_JSON_URL, params={"key": CONFIG.require_api_key()})
        resp.raise_for_status()
        data = resp.json()

    datasets = data.get("dataset", [])
    filtered: List[Dict] = []

    # Regex pattern to match ACS 1-year or 5-year estimates
    # Matches: "American Community Survey", "ACS", "Census", etc.
    # And "1-year", "5-year", "1 year", "5 year", "1-Year", "5-Year", etc.
    pattern = re.compile(
        r"(?:american community survey|acs|census).*?(?:1[-\s]year|5[-\s]year|1[-\s]years|5[-\s]years)",
        re.IGNORECASE,
    )

    for ds in datasets:
        title = ds.get("title", "")
        c = ds.get("c_vintage", None)
        ident = ds.get("identifier", "")

        # Skip if title is empty
        if not title:
            continue

        # Use regex to match ACS 1/5 year; a named census passes by identifier.
        if not pattern.search(title) and _catalog_id(ident) not in CENSUS_CATALOG_IDS:
            continue

        # Ensure c_vintage is a valid year
        if c is None:
            continue
        try:
            year = int(c)
        except ValueError:
            continue

        filtered.append(
            {
                "title": title,
                "year": year,
                "identifier": ident,
            }
        )

    return filtered


def _classify_detailed_table_dataset(dataset: Dict) -> tuple[str, int, str, str] | None:
    """Return the local dataset identity for an ACS Detailed Tables catalog row."""
    title = str(dataset["title"])
    year = int(dataset["year"])
    identifier = str(dataset["identifier"])
    census_id = _catalog_id(identifier)
    expected_ids = {
        f"ACSDT1Y{year}": "acs1",
        f"ACSDT5Y{year}": "acs5",
        **CENSUS_CATALOG_IDS,
    }
    local_name = expected_ids.get(census_id)
    if local_name is None:
        return None
    return local_name, year, census_id, title


def sync_acs_dataset_table() -> int:
    """
    Upsert filtered ACS datasets into raw_census.acs_datasets.

    Uses ID prefixes (e.g., ACSDT1Y, ACSDT5Y) to determine dataset type.
    """
    datasets = fetch_acs_datasets_from_data_json()
    classified = [
        classified_dataset
        for dataset in datasets
        if (classified_dataset := _classify_detailed_table_dataset(dataset)) is not None
    ]
    if not classified:
        raise RuntimeError(
            "Census catalog returned no ACS Detailed Tables datasets; "
            "refusing to continue with an empty ingestion plan"
        )

    conn = _get_pg_connection()
    cur = None
    try:
        conn.autocommit = False
        cur = conn.cursor()

        now = datetime.now(timezone.utc)
        for dataset, year, census_id, title in classified:
            cur.execute(
                """
                INSERT INTO raw_census.acs_datasets (
                    dataset, year, census_id, title, is_available, first_seen_at, last_checked_at
                )
                VALUES (%s, %s, %s, %s, TRUE, %s, %s)
                ON CONFLICT (dataset, year)
                DO UPDATE SET
                    census_id = EXCLUDED.census_id,
                    title = EXCLUDED.title,
                    is_available = TRUE,
                    last_checked_at = EXCLUDED.last_checked_at;
                """,
                (dataset, year, census_id, title, now, now),
            )

        conn.commit()
        logger.info("Synchronized %s ACS Detailed Tables datasets", len(classified))
        return len(classified)

    except Exception:
        conn.rollback()
        raise
    finally:
        if cur is not None:
            cur.close()
        conn.close()


def fetch_variables_json(year: int, dataset: str) -> Dict:
    """
    Fetch variables.json for a given year and dataset (`CENSUS_DATASETS`).
    """
    url = f"{census_api_url(year, dataset)}/variables.json"
    with httpx.Client(timeout=60.0) as client:
        resp = client.get(url, params={"key": CONFIG.require_api_key()})
        resp.raise_for_status()
        return resp.json()


def census_variable_rows(
    year: int, dataset: str, variables: Dict[str, Dict]
) -> List[tuple]:
    """`raw_census.acs_variables` rows for a census dataset's crosswalked variables.

    Each row keeps the census's own variable name, label and concept, filed
    under the ACS table of the estimate it measures, so a slice asking for a
    curated table's variables asks a census dataset for exactly its declared
    pairings and nothing else. A census publishes no margins, so there are no
    margin rows. A crosswalked variable missing from `variables.json` refuses:
    the Bureau renamed or withdrew it and the pairing needs review.
    """
    rows: List[tuple] = []
    for source, entry in sorted(crosswalk().get(dataset, {}).items()):
        info = variables.get(source)
        if info is None:
            raise ValueError(f"{dataset} {year} variables.json has no {source}")
        table_id = entry.acs_variable.split("_", 1)[0]
        rows.append(
            (
                dataset,
                year,
                source,
                table_id,
                info.get("label"),
                info.get("concept"),
                info.get("predicateType"),
                info.get("group") or table_id,
            )
        )
    return rows


def _sync_census_variable_metadata(year: int, dataset: str, variables: Dict) -> None:
    rows = census_variable_rows(year, dataset, variables)
    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO raw_census.acs_variables (
                        dataset, year, variable_name, table_id,
                        label, concept, predicate_type, group_name
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (dataset, year, variable_name)
                    DO UPDATE SET
                        table_id = EXCLUDED.table_id,
                        label = EXCLUDED.label,
                        concept = EXCLUDED.concept,
                        predicate_type = EXCLUDED.predicate_type,
                        group_name = EXCLUDED.group_name;
                    """,
                    row,
                )
            for table_id, concept in {row[3]: row[5] for row in rows}.items():
                cur.execute(
                    """
                    INSERT INTO raw_census.acs_tables (
                        dataset, table_id, concept, product
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (dataset, table_id)
                    DO UPDATE SET
                        concept = EXCLUDED.concept,
                        product = EXCLUDED.product;
                    """,
                    (dataset, table_id, concept, dataset),
                )
        conn.commit()
    finally:
        conn.close()


def sync_variable_metadata_for_year(year: int, dataset: str) -> None:
    """
    For the given year+dataset, load variables.json, filter to curated table IDs,
    and upsert into acs_tables and acs_variables. A census dataset (one that
    publishes no margins) files only its crosswalked variables.
    """
    meta = fetch_variables_json(year, dataset)
    variables = meta.get("variables", {})
    if not CENSUS_DATASETS[dataset].margins:
        _sync_census_variable_metadata(year, dataset, variables)
        return

    conn = _get_pg_connection()

    try:
        conn.autocommit = False
        cur = conn.cursor()

        curated = set(CONFIG.curated_tables)

        # We'll accumulate table metadata (concept) from variables.
        seen_tables: Dict[str, Dict] = {}

        for var_name, info in variables.items():
            # Keep only detailed-table estimate variables (E)
            if "_" not in var_name or not var_name.endswith("E"):
                continue

            table_id = var_name.split("_", 1)[0]
            if table_id not in curated:
                continue

            label = info.get("label")
            concept = info.get("concept")
            predicate_type = info.get("predicateType")
            group = info.get("group") or table_id

            # Insert the estimate variable (E)
            cur.execute(
                """
                INSERT INTO raw_census.acs_variables (
                    dataset, year, variable_name, table_id,
                    label, concept, predicate_type, group_name
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dataset, year, variable_name)
                DO UPDATE SET
                    table_id = EXCLUDED.table_id,
                    label = EXCLUDED.label,
                    concept = EXCLUDED.concept,
                    predicate_type = EXCLUDED.predicate_type,
                    group_name = EXCLUDED.group_name;
                """,
                (
                    dataset,
                    year,
                    var_name,
                    table_id,
                    label,
                    concept,
                    predicate_type,
                    group,
                ),
            )

            # Now pull MOE variables from attributes and insert them too
            attrs = info.get("attributes") or ""
            for a in [x.strip() for x in attrs.split(",") if x.strip()]:
                if not a.endswith("M"):
                    continue  # skip EA/MA annotation vars

                moe_name = a

                # MOE variables usually share concept/table; label can be derived
                moe_label = (
                    label.replace("Estimate", "Margin of Error")
                    if isinstance(label, str)
                    else None
                )

                cur.execute(
                    """
                    INSERT INTO raw_census.acs_variables (
                        dataset, year, variable_name, table_id,
                        label, concept, predicate_type, group_name
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (dataset, year, variable_name)
                    DO UPDATE SET
                        table_id = EXCLUDED.table_id,
                        label = EXCLUDED.label,
                        concept = EXCLUDED.concept,
                        predicate_type = EXCLUDED.predicate_type,
                        group_name = EXCLUDED.group_name;
                    """,
                    (
                        dataset,
                        year,
                        moe_name,
                        table_id,
                        moe_label,
                        concept,
                        predicate_type,
                        group,
                    ),
                )

            # track table metadata
            if table_id not in seen_tables:
                seen_tables[table_id] = {
                    "concept": concept,
                }

        # upsert into acs_tables
        for table_id, info in seen_tables.items():
            cur.execute(
                """
                INSERT INTO raw_census.acs_tables (
                    dataset, table_id, concept, product
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (dataset, table_id)
                DO UPDATE SET
                    concept = EXCLUDED.concept,
                    product = EXCLUDED.product;
                """,
                (dataset, table_id, info["concept"], dataset),
            )

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
