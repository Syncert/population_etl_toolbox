#!/usr/bin/env python3
"""
Validate the MVP API-to-map geography contract.

The check is intentionally host-side and read-only:
- verifies county geometries exist in gold.dim_geo_latest
- verifies Martin exposes a tile layer with a usable geography join key
- verifies latest API observation geo_ids join back to county geometry rows
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import psycopg2
import requests

from data_ingestion_toolbox.martin_contract import (
    choose_join_key,
    extract_vector_layer,
    field_names,
    normalize_base_url,
    sample_tile_url,
    tile_layer_url,
)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _print_result(result: CheckResult) -> None:
    status = "PASS" if result.ok else "FAIL"
    print(f"{status} {result.name}: {result.detail}")


def _load_env_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return values

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env_value(env_file_values: dict[str, str], key: str, default: str = "") -> str:
    return os.getenv(key) or env_file_values.get(key, default)


def _db_connect(env_file_values: dict[str, str], serving_role: bool = False):
    user_key = "ANALYTICS_API_DB_USER" if serving_role else "ANALYTICS_DB_USER"
    password_key = (
        "ANALYTICS_API_DB_PASSWORD" if serving_role else "ANALYTICS_DB_PASSWORD"
    )
    return psycopg2.connect(
        host=_env_value(env_file_values, "ANALYTICS_DB_HOST", "localhost"),
        port=int(_env_value(env_file_values, "ANALYTICS_DB_PORT", "5432")),
        user=_env_value(env_file_values, user_key, "" if serving_role else "postgres"),
        password=_env_value(env_file_values, password_key, ""),
        dbname=_env_value(env_file_values, "ANALYTICS_DB_NAME", "population_etl"),
        connect_timeout=10,
    )


def check_serving_role(env_file_values: dict[str, str]) -> CheckResult:
    configured_role = _env_value(env_file_values, "ANALYTICS_API_DB_USER", "")
    if not configured_role:
        return CheckResult(
            "api-readonly-role",
            False,
            "ANALYTICS_API_DB_USER is not configured; run scripts/provision_api_readonly.py.",
        )

    conn = _db_connect(env_file_values, serving_role=True)
    try:
        sql = """
            SELECT
                current_user,
                current_setting('transaction_read_only') = 'on' AS transaction_read_only,
                has_schema_privilege(current_user, 'gold', 'USAGE') AS can_use_gold,
                has_schema_privilege(current_user, 'gold', 'CREATE') AS can_create_in_gold,
                COALESCE(bool_and(has_table_privilege(current_user, schemaname || '.' || tablename, 'SELECT')), false) AS can_select_all,
                COALESCE(bool_or(has_table_privilege(current_user, schemaname || '.' || tablename, 'INSERT,UPDATE,DELETE,TRUNCATE')), false) AS can_mutate_any
            FROM pg_tables
            WHERE schemaname = 'gold'
            GROUP BY current_user;
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        if not row:
            return CheckResult(
                "api-readonly-role",
                False,
                "No gold tables were available for privilege validation.",
            )
        role, read_only, can_use, can_create, can_select_all, can_mutate_any = row
        ok = (
            role == configured_role
            and bool(read_only)
            and bool(can_use)
            and not bool(can_create)
            and bool(can_select_all)
            and not bool(can_mutate_any)
        )
        return CheckResult(
            "api-readonly-role",
            ok,
            (
                f"role={role}; transaction_read_only={read_only}; can_use_gold={can_use}; "
                f"can_create_in_gold={can_create}; can_select_all={can_select_all}; "
                f"can_mutate_any={can_mutate_any}"
            ),
        )
    finally:
        conn.close()


def _get_json(url: str, timeout: int = 30) -> Any:
    response = requests.get(
        url, timeout=timeout, headers={"Accept": "application/json"}
    )
    response.raise_for_status()
    return response.json()


def check_db_geometry(conn) -> tuple[CheckResult, dict[str, int]]:
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE UPPER(geo_level) = 'COUNTY')::int AS county_rows,
            COUNT(*) FILTER (
                WHERE UPPER(geo_level) = 'COUNTY'
                  AND geo_geom IS NOT NULL
            )::int AS county_geometry_rows,
            COUNT(*) FILTER (
                WHERE UPPER(geo_level) = 'COUNTY'
                  AND geo_id IS NOT NULL
                  AND geo_geom IS NOT NULL
            )::int AS county_joinable_rows,
            COUNT(*) FILTER (
                WHERE UPPER(geo_level) = 'COUNTY'
                  AND geo_geom IS NOT NULL
                  AND NOT ST_IsValid(geo_geom)
            )::int AS invalid_geometry_rows
        FROM gold.dim_geo_latest;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    counts = {
        "county_rows": int(row[0] or 0),
        "county_geometry_rows": int(row[1] or 0),
        "county_joinable_rows": int(row[2] or 0),
        "invalid_geometry_rows": int(row[3] or 0),
    }
    ok = (
        counts["county_rows"] > 0
        and counts["county_geometry_rows"] > 0
        and counts["county_joinable_rows"] == counts["county_geometry_rows"]
        and counts["invalid_geometry_rows"] == 0
    )
    detail = (
        f"county_rows={counts['county_rows']}; "
        f"county_geometry_rows={counts['county_geometry_rows']}; "
        f"county_joinable_rows={counts['county_joinable_rows']}; "
        f"invalid_geometry_rows={counts['invalid_geometry_rows']}"
    )
    return CheckResult("db-county-geometry", ok, detail), counts


def _metric_supports_geo(metric: dict[str, Any], geo_level: str) -> bool:
    valid_geo_grains = metric.get("valid_geo_grains")
    if not isinstance(valid_geo_grains, list):
        return False
    return any(str(grain).upper() == geo_level.upper() for grain in valid_geo_grains)


def _discover_metric_code(
    api_base_url: str, requested_metric_code: str, geo_level: str
) -> str | None:
    if requested_metric_code.lower() != "population":
        return None

    catalog_url = urljoin(
        normalize_base_url(api_base_url), "catalog/metrics?q=population&limit=250"
    )
    payload = _get_json(catalog_url)
    items = payload.get("items", []) if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return None

    candidates = [
        item
        for item in items
        if isinstance(item, dict)
        and _metric_supports_geo(item, geo_level)
        and "B01003_001" in str(item.get("metric_code", ""))
    ]

    if not candidates:
        candidates = [
            item
            for item in items
            if isinstance(item, dict)
            and _metric_supports_geo(item, geo_level)
            and "population" in str(item.get("business_definition", "")).lower()
        ]

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            0 if "acs5" in str(item.get("metric_code", "")).lower() else 1,
            str(item.get("metric_code", "")),
        )
    )
    return str(candidates[0].get("metric_code") or "") or None


def _fetch_observations(
    api_base_url: str, metric_code: str, geo_level: str, limit: int
) -> tuple[str, list[dict[str, Any]]]:
    url = urljoin(
        normalize_base_url(api_base_url),
        f"observations/latest?metric_code={metric_code}&geo_level={geo_level}&limit={limit}",
    )
    payload = _get_json(url)
    items = payload.get("items", []) if isinstance(payload, dict) else []
    return url, items if isinstance(items, list) else []


def check_api_observations(
    api_base_url: str,
    metric_code: str,
    geo_level: str,
    limit: int,
) -> tuple[CheckResult, list[dict[str, Any]], str]:
    url, items = _fetch_observations(api_base_url, metric_code, geo_level, limit)
    selected_metric_code = metric_code

    if not items:
        if metric_code.lower() == "population":
            for candidate_metric_code in ("ACS:acs5:B01003_001", "ACS:acs1:B01003_001"):
                fallback_url, fallback_items = _fetch_observations(
                    api_base_url,
                    candidate_metric_code,
                    geo_level,
                    limit,
                )
                if fallback_items:
                    selected_metric_code = candidate_metric_code
                    items = fallback_items
                    url = fallback_url
                    break

    if not items:
        discovered_metric_code = _discover_metric_code(
            api_base_url, metric_code, geo_level
        )
        if discovered_metric_code:
            fallback_url, fallback_items = _fetch_observations(
                api_base_url,
                discovered_metric_code,
                geo_level,
                limit,
            )
            if fallback_items:
                selected_metric_code = discovered_metric_code
                items = fallback_items
                url = fallback_url

    ok = len(items) > 0
    detail = f"metric_code={selected_metric_code}; requested_metric_code={metric_code}; items={len(items)}; url={url}"
    return (
        CheckResult("api-county-observations", ok, detail),
        items,
        selected_metric_code,
    )


def check_martin_tiles(
    tiles_base_url: str, layer_id: str
) -> tuple[CheckResult, str | None]:
    tilejson_url = tile_layer_url(tiles_base_url, layer_id)
    tilejson = _get_json(tilejson_url)
    vector_layer = extract_vector_layer(tilejson, layer_id)
    fields = field_names(vector_layer)
    join_key = choose_join_key(fields)

    sample_url = sample_tile_url(tilejson, tiles_base_url, layer_id)
    response = requests.get(sample_url, timeout=30)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    content_length = len(response.content or b"")

    ok = bool(join_key) and content_length > 0
    detail = (
        f"layer={vector_layer.get('id', layer_id)}; "
        f"join_key={join_key or 'missing'}; "
        f"fields={','.join(sorted(fields)) or 'none'}; "
        f"sample_status={response.status_code}; "
        f"sample_bytes={content_length}; "
        f"sample_content_type={content_type or 'unknown'}"
    )
    return CheckResult("martin-county-tiles", ok, detail), join_key


def check_observation_geometry_join(
    conn, observations: list[dict[str, Any]], minimum_join_ratio: float
) -> CheckResult:
    geo_ids = sorted(
        {str(item.get("geo_id")) for item in observations if item.get("geo_id")}
    )
    if not geo_ids:
        return CheckResult(
            "api-geometry-join", False, "No geo_id values returned by API observations."
        )

    sql = """
        SELECT COUNT(*)::int
        FROM gold.dim_geo_latest
        WHERE UPPER(geo_level) = 'COUNTY'
          AND geo_geom IS NOT NULL
          AND geo_id = ANY(%s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (geo_ids,))
        joined_count = int(cur.fetchone()[0] or 0)

    ratio = joined_count / len(geo_ids)
    ok = ratio >= minimum_join_ratio
    detail = (
        f"api_geo_ids={len(geo_ids)}; "
        f"joined_to_county_geometry={joined_count}; "
        f"join_ratio={ratio:.3f}; "
        f"minimum_join_ratio={minimum_join_ratio:.3f}"
    )
    return CheckResult("api-geometry-join", ok, detail)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate MVP Martin/API county join contract."
    )
    parser.add_argument("--env-file", default="infra/docker/stack.external.env")
    parser.add_argument("--api-base-url", default="http://localhost:3001/api/")
    parser.add_argument("--tiles-base-url", default="http://localhost:3001/tiles/")
    parser.add_argument("--metric-code", default="population")
    parser.add_argument("--geo-level", default="COUNTY")
    parser.add_argument("--layer-id", default="counties")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--minimum-join-ratio", type=float, default=1.0)
    parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON summary."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file_values = _load_env_file(args.env_file)
    results: list[CheckResult] = []

    conn = None
    try:
        conn = _db_connect(env_file_values)

        geometry_result, _counts = check_db_geometry(conn)
        results.append(geometry_result)

        results.append(check_serving_role(env_file_values))

        api_result, observations, _selected_metric_code = check_api_observations(
            api_base_url=args.api_base_url,
            metric_code=args.metric_code,
            geo_level=args.geo_level,
            limit=args.limit,
        )
        results.append(api_result)

        tile_result, _join_key = check_martin_tiles(
            tiles_base_url=args.tiles_base_url,
            layer_id=args.layer_id,
        )
        results.append(tile_result)

        join_result = check_observation_geometry_join(
            conn=conn,
            observations=observations,
            minimum_join_ratio=args.minimum_join_ratio,
        )
        results.append(join_result)
    except Exception as exc:
        results.append(CheckResult("mvp-geo-tile-join", False, str(exc)))
    finally:
        if conn is not None:
            conn.close()

    if args.json:
        print(json.dumps([result.__dict__ for result in results], indent=2))
    else:
        for result in results:
            _print_result(result)

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
