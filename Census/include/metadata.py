# include/census_acs/metadata.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List

import httpx
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .config import CONFIG


DATA_JSON_URL = "https://api.census.gov/data.json"


def _get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def fetch_acs_datasets_from_data_json() -> List[Dict]:
    """
    Fetch dataset metadata from data.json and filter to ACS 1- and 5- year datasets.
    """
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(DATA_JSON_URL)
        resp.raise_for_status()
        data = resp.json()

    datasets = data.get("dataset", [])
    filtered: List[Dict] = []
    for ds in datasets:
        title = ds.get("title", "")
        c = ds.get("c_vintage", None)
        ident = ds.get("identifier", "")
        # We care about ACS 1-year and 5-year detailed estimates
        if "American Community Survey" in title and ("1-year estimates" in title or "5-year estimates" in title):
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


def sync_acs_dataset_table() -> None:
    """
    Upsert filtered ACS datasets into raw_census.acs_datasets.

    This gives us (dataset, year) entries for acs1 and acs5.
    """
    hook = _get_postgres_hook()
    conn = hook.get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    datasets = fetch_acs_datasets_from_data_json()
    now = datetime.now(timezone.utc)

    for ds in datasets:
        title = ds["title"]
        year = ds["year"]
        identifier = ds["identifier"]

        # Heuristic: map "acs/acs1" vs "acs/acs5" from identifier
        if "acs/acs1" in identifier:
            dataset = "acs1"
        elif "acs/acs5" in identifier:
            dataset = "acs5"
        else:
            # some ACS datasets we don't care about here (e.g. subject tables)
            continue

        cur.execute(
            """
            INSERT INTO raw_census.acs_datasets (dataset, year, title, is_available, first_seen_at, last_checked_at)
            VALUES (%s, %s, %s, TRUE, %s, %s)
            ON CONFLICT (dataset, year)
            DO UPDATE SET
                title = EXCLUDED.title,
                is_available = TRUE,
                last_checked_at = EXCLUDED.last_checked_at;
            """,
            (dataset, year, title, now, now),
        )

    conn.commit()
    cur.close()
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

    hook = _get_postgres_hook()
    conn = hook.get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    curated = set(CONFIG.curated_tables)

    # We'll accumulate table metadata (concept/universe) from variables.
    seen_tables: Dict[str, Dict] = {}

    for var_name, info in variables.items():
        group = info.get("group", "")
        if not group:
            continue
        # group looks like 'B01001', 'B19013', etc.
        table_id = group

        if table_id not in curated:
            continue

        label = info.get("label")
        concept = info.get("concept")
        predicate_type = info.get("predicateType")
        # upsert variable row
        cur.execute(
            """
            INSERT INTO raw_census.acs_variables (
                dataset, year, variable_name, table_id, label, concept, predicate_type, group_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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

        # track table metadata
        if table_id not in seen_tables:
            seen_tables[table_id] = {
                "concept": concept,
                "universe": None,  # variables.json doesn't always expose universe; could be from another endpoint
            }

    # upsert into acs_tables
    for table_id, info in seen_tables.items():
        cur.execute(
            """
            INSERT INTO raw_census.acs_tables (dataset, table_id, concept, universe, product)
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
    cur.close()
    conn.close()