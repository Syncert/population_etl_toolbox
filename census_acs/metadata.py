# include/census_acs/metadata.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import psycopg2
import re

# Adjust this import path to wherever db_connection.py lives in your project
from utility.db_connection import (
    PostgresConnectionFactory,
    PostgresConnectionDetails,
)

from .config import CONFIG


DATA_JSON_URL = "https://api.census.gov/data.json"

# Which database inside the Postgres instance do you want to use?
# Change this if your metadata lives somewhere else.
_TARGET_DATABASE = "public_data"

# When running inside Airflow, you can let CONFIG.postgres_conn_id drive the
# connection. In local dev (no Airflow), this will be None and the factory
# will fall back to POSTGRES_* env vars.
_AIRFLOW_CONN_ID: Optional[str] = getattr(CONFIG, "postgres_conn_id", None)


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


def fetch_acs_datasets_from_data_json() -> List[Dict]:
    """
    Fetch dataset metadata from data.json and filter to ACS 1- and 5- year datasets.
    Uses regex to match flexible title patterns.
    """
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(DATA_JSON_URL)
        resp.raise_for_status()
        data = resp.json()

    datasets = data.get("dataset", [])
    filtered: List[Dict] = []

    # Regex pattern to match ACS 1-year or 5-year estimates
    # Matches: "American Community Survey", "ACS", "Census", etc.
    # And "1-year", "5-year", "1 year", "5 year", "1-Year", "5-Year", etc.
    pattern = re.compile(
        r"(?:american community survey|acs|census).*?(?:1[-\s]year|5[-\s]year|1[-\s]years|5[-\s]years)",
        re.IGNORECASE
    )

    for ds in datasets:
        title = ds.get("title", "")
        c = ds.get("c_vintage", None)
        ident = ds.get("identifier", "")

        # Skip if title is empty
        if not title:
            continue

        # Use regex to match ACS 1/5 year
        if not pattern.search(title):
            continue

        # Ensure c_vintage is a valid year
        if c is None:
            continue
        try:
            year = int(c)
        except ValueError:
            continue

        filtered.append({
            "title": title,
            "year": year,
            "identifier": ident,
        })

    return filtered


def sync_acs_dataset_table() -> None:
    """
    Upsert filtered ACS datasets into raw_census.acs_datasets.

    Uses ID prefixes (e.g., ACSDT1Y, ACSDT5Y) to determine dataset type.
    """
    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        datasets = fetch_acs_datasets_from_data_json()
        now = datetime.now(timezone.utc)
        print(f"Found {len(datasets)} datasets from data.json")

        inserted_count = 0
        skipped_count = 0

        for ds in datasets:
            title = ds["title"]
            year = ds["year"]
            identifier = ds["identifier"]

            # Extract the ID from the URL
            id_part = identifier.split("/")[-1]

            # Classify by ID prefix
            if id_part.startswith("ACSDT1Y") or id_part.startswith("ACSDP1Y") or id_part.startswith("ACSST1Y") or id_part.startswith("ACSSPP1Y") or id_part.startswith("ACSSE1Y"):
                dataset = "acs1"
            elif id_part.startswith("ACSDT5Y") or id_part.startswith("ACSDP5Y") or id_part.startswith("ACSST5Y") or id_part.startswith("ACSSPP5Y") or id_part.startswith("ACSSE5Y"):
                dataset = "acs5"
            else:
                skipped_count += 1
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO raw_census.acs_datasets (
                        dataset, year, title, is_available, first_seen_at, last_checked_at
                    )
                    VALUES (%s, %s, %s, TRUE, %s, %s)
                    ON CONFLICT (dataset, year)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        is_available = TRUE,
                        last_checked_at = EXCLUDED.last_checked_at;
                    """,
                    (dataset, year, title, now, now),
                )
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting {dataset}, {year}: {e}")
                continue

        conn.commit()
        print(f"Sync complete. Inserted: {inserted_count}, Skipped: {skipped_count}")

    except Exception as e:
        print(f"Transaction failed: {e}")
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def fetch_variables_json(year: int, dataset: str) -> Dict:
    """
    Fetch variables.json for a given year and dataset (acs1 or acs5).
    """
    if dataset not in ("acs1", "acs5"):
        raise ValueError(f"Unsupported dataset: {dataset}")

    url = f"https://api.census.gov/data/{year}/acs/{dataset}/variables.json"
    with httpx.Client(timeout=60.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.json()


def sync_variable_metadata_for_year(year: int, dataset: str) -> None:
    """
    For the given year+dataset, load variables.json, filter to curated table IDs,
    and upsert into acs_tables and acs_variables.
    """
    meta = fetch_variables_json(year, dataset)
    variables = meta.get("variables", {})

    conn = _get_pg_connection()

    try:
        conn.autocommit = False
        cur = conn.cursor()

        curated = set(CONFIG.curated_tables)

        # We'll accumulate table metadata (concept/universe) from variables.
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
                (dataset, year, var_name, table_id, label, concept, predicate_type, group),
            )

            # Now pull MOE variables from attributes and insert them too
            attrs = info.get("attributes") or ""
            for a in [x.strip() for x in attrs.split(",") if x.strip()]:
                if not a.endswith("M"):
                    continue  # skip EA/MA annotation vars

                moe_name = a

                # MOE variables usually share concept/table; label can be derived
                moe_label = (label.replace("Estimate", "Margin of Error") if isinstance(label, str) else None)

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
                    (dataset, year, moe_name, table_id, moe_label, concept, predicate_type, group),
                )

            # track table metadata
            if table_id not in seen_tables:
                seen_tables[table_id] = {
                    "concept": concept,
                    # variables.json doesn't always expose universe; could be from another endpoint
                    "universe": None,
                }

        # upsert into acs_tables
        for table_id, info in seen_tables.items():
            cur.execute(
                """
                INSERT INTO raw_census.acs_tables (
                    dataset, table_id, concept, universe, product
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (dataset, table_id)
                DO UPDATE SET
                    concept = EXCLUDED.concept,
                    universe = EXCLUDED.universe,
                    product = EXCLUDED.product;
                """,
                (dataset, table_id, info["concept"], info["universe"], dataset),
            )

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()