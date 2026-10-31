"""The warehouse schema atlas, read from the Postgres catalog at request time.

Every relation, column, primary key, unique constraint, foreign key, view
dependency and partition the connected database actually holds, grouped into
diagrams and laid out for the admin page. Nothing is drawn from the DDL files:
a table deployed this morning is on the page this afternoon, and a constraint
the DDL declares but the database lacks is absent, which is the truth an admin
needs.

The catalog read touches only ``pg_catalog``, which every role may read, so it
works under the API's read-only serving role without a grant. It is a few
thousand rows and runs in tens of milliseconds; the result is still cached for
a few minutes per process because nothing on the page needs it fresher.

Diagram grouping follows the warehouse's own naming: a source's ``silver_*`` and
``gold_*`` schemas plus its ``control`` ledgers (from the declared inventory),
a shared capture backbone, the reference geography, the catalog and serving
contract, and an ``Other`` diagram that catches any schema nothing else claims
-- so a new schema is visible before anyone describes it.
"""

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.services.warehouse_inventory import WAREHOUSE_SOURCES

#: Schemas that are Postgres's or an extension's, never the warehouse's.
_SYSTEM_SCHEMAS = (
    "pg_catalog",
    "information_schema",
    "public",
    "topology",
    "tiger",
    "tiger_data",
)

CATALOG_SQL = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS relation_name,
           c.relkind::TEXT AS relkind,
           c.relispartition AS is_partition,
           pg_catalog.pg_get_partkeydef(c.oid) AS partition_key,
           (SELECT pn.nspname || '.' || p.relname
              FROM pg_catalog.pg_inherits i
              JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
              JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace
             WHERE i.inhrelid = c.oid
             LIMIT 1) AS parent,
           c.reltuples::BIGINT AS estimated_rows,
           COALESCE((SELECT json_agg(json_build_array(
                        a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull)
                        ORDER BY a.attnum)
                       FROM pg_catalog.pg_attribute a
                      WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
                    '[]'::json) AS columns,
           COALESCE((SELECT json_agg(a.attname ORDER BY array_position(con.conkey, a.attnum))
                       FROM pg_catalog.pg_constraint con
                       JOIN pg_catalog.pg_attribute a
                         ON a.attrelid = con.conrelid AND a.attnum = ANY (con.conkey)
                      WHERE con.conrelid = c.oid AND con.contype = 'p'),
                    '[]'::json) AS primary_key,
           COALESCE((SELECT json_agg(pg_catalog.pg_get_constraintdef(con.oid) ORDER BY con.conname)
                       FROM pg_catalog.pg_constraint con
                      WHERE con.conrelid = c.oid AND con.contype = 'u'),
                    '[]'::json) AS uniques,
           COALESCE((SELECT json_agg(json_build_array(
                        (SELECT json_agg(a.attname ORDER BY k.ord)
                           FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
                           JOIN pg_catalog.pg_attribute a
                             ON a.attrelid = con.conrelid AND a.attnum = k.attnum),
                        fn.nspname || '.' || f.relname) ORDER BY con.conname)
                       FROM pg_catalog.pg_constraint con
                       JOIN pg_catalog.pg_class f ON f.oid = con.confrelid
                       JOIN pg_catalog.pg_namespace fn ON fn.oid = f.relnamespace
                      WHERE con.conrelid = c.oid AND con.contype = 'f'),
                    '[]'::json) AS foreign_keys,
           COALESCE((SELECT json_agg(DISTINCT dn.nspname || '.' || dc.relname)
                       FROM pg_catalog.pg_rewrite rw
                       JOIN pg_catalog.pg_depend d
                         ON d.objid = rw.oid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
                       JOIN pg_catalog.pg_class dc ON dc.oid = d.refobjid
                       JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
                      WHERE rw.ev_class = c.oid AND dc.oid <> c.oid
                        AND dc.relkind IN ('r', 'p', 'v', 'm')),
                    '[]'::json) AS reads_from,
           (SELECT count(*) FROM pg_catalog.pg_index ix WHERE ix.indrelid = c.oid) AS index_count
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind IN ('r', 'p', 'v', 'm')
       AND n.nspname <> ALL (:system_schemas)
       AND n.nspname NOT LIKE 'pg\\_%'
     ORDER BY n.nspname, c.relname
    """
)


@dataclass
class Relation:
    name: str
    kind: str  # table | partitioned table | view | materialized view
    columns: list[tuple[str, str, bool]]
    primary_key: list[str]
    uniques: list[str]
    foreign_keys: list[tuple[list[str], str]]
    reads_from: list[str]
    partition_key: str | None
    partitions: int
    estimated_rows: int | None
    index_count: int

    @property
    def schema(self) -> str:
        return self.name.split(".", 1)[0]


_KIND = {"r": "table", "p": "partitioned table", "v": "view", "m": "materialized view"}


def read_catalog(db: Session) -> dict[str, Relation]:
    """Every warehouse relation, partitions folded into their parent."""
    rows = (
        db.execute(CATALOG_SQL, {"system_schemas": list(_SYSTEM_SCHEMAS)})
        .mappings()
        .all()
    )
    partitions: dict[str, int] = defaultdict(int)
    partition_rows: dict[str, int] = defaultdict(int)
    relations: dict[str, Relation] = {}
    for row in rows:
        name = f"{row['schema_name']}.{row['relation_name']}"
        if row["is_partition"] and row["parent"]:
            partitions[row["parent"]] += 1
            partition_rows[row["parent"]] += max(int(row["estimated_rows"] or 0), 0)
            continue
        relations[name] = Relation(
            name=name,
            kind=_KIND.get(row["relkind"], row["relkind"]),
            columns=[(c[0], c[1], bool(c[2])) for c in (row["columns"] or [])],
            primary_key=list(row["primary_key"] or []),
            uniques=list(row["uniques"] or []),
            foreign_keys=[
                (list(fk[0] or []), fk[1]) for fk in (row["foreign_keys"] or [])
            ],
            reads_from=list(row["reads_from"] or []),
            partition_key=row["partition_key"],
            partitions=0,
            estimated_rows=(
                int(row["estimated_rows"])
                if row["estimated_rows"] is not None and row["estimated_rows"] >= 0
                else None
            ),
            index_count=int(row["index_count"] or 0),
        )
    for parent, count in partitions.items():
        if parent in relations:
            relations[parent].partitions = count
            relations[parent].estimated_rows = partition_rows[parent]
    # A view that reads a partition reads its parent.
    parent_of = {
        f"{r['schema_name']}.{r['relation_name']}": r["parent"]
        for r in rows
        if r["is_partition"] and r["parent"]
    }
    for relation in relations.values():
        relation.reads_from = sorted(
            {parent_of.get(d, d) for d in relation.reads_from} - {relation.name}
        )
    return relations


# ---------------------------------------------------------------- layers


def layer(name: str) -> str:
    schema = name.split(".", 1)[0]
    if schema in ("raw_capture", "control") or schema.startswith("raw_"):
        return "capture"
    if schema.startswith("silver"):
        return "silver"
    if schema == "app_api":
        return "app"
    if schema in ("gold", "gold_glossary"):
        return "serving"
    return "gold"


#: Tables nearly every source references. Drawn as compact boxes wherever they
#: are referenced, so each diagram shows its links without repeating them in full.
SHARED = (
    "raw_capture.response_capture",
    "control.ingestion_run",
    "silver_ref.dim_geo_entity",
    "silver_ref.dim_geo_current",
    "silver_ref.dim_geo",
    "silver_ref.dim_time",
    "silver_ref.dim_geo_type",
    "gold_glossary.dim_source_system",
)

_KEY_COLUMN = re.compile(
    r"(^|_)(geo_sk|geo_id|year|period_start|observation_date|metric_code|metric_key|measure|"
    r"measure_code|value|run_id|capture_id|release_id|vintage_year|dataset_code)$"
)


def key_columns(
    relation: Relation, limit: int = 7
) -> tuple[list[tuple[str, str, str]], int]:
    """Up to ``limit`` columns worth drawing: keys first, then geography, time, metric and value."""
    fk_columns = {c for cols, _ in relation.foreign_keys for c in cols}
    pk = set(relation.primary_key)
    picked: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    def take(name: str, kind: str, badge: str) -> None:
        if name not in seen and len(picked) < limit:
            seen.add(name)
            picked.append((name, kind, badge))

    for name, kind, _ in relation.columns:
        if name in pk:
            take(name, kind, "PK")
    for name, kind, _ in relation.columns:
        if name in fk_columns:
            take(name, kind, "FK")
    for name, kind, _ in relation.columns:
        if _KEY_COLUMN.search(name):
            take(name, kind, "")
    return picked, len(relation.columns)


# ---------------------------------------------------------------- domains


@dataclass
class Domain:
    id: str
    title: str
    caption: str
    members: list[str] = field(default_factory=list)
    source_code: str | None = None


_SOURCE_CAPTION = (
    "{publisher}. Revisions keep every release; the latest relation serves the newest; "
    "metric_publisher hands the metric list to the catalog."
)


def _schemas_of(names: list[str]) -> set[str]:
    return {n.split(".", 1)[0] for n in names if "." in n}


def build_domains(relations: dict[str, Relation]) -> list[Domain]:
    names = sorted(relations)
    by_schema: dict[str, list[str]] = defaultdict(list)
    for n in names:
        by_schema[n.split(".", 1)[0]].append(n)

    domains: list[Domain] = [
        Domain(
            "backbone",
            "Capture backbone",
            "Every provider response is stored once as bytes and described once; every pipeline run is one "
            "ingestion_run. Silver revisions point back at both, so any served number traces to the response "
            "it came from.",
            [n for n in by_schema.get("raw_capture", [])]
            + [
                n
                for n in by_schema.get("control", [])
                if re.search(
                    r"\.(ingestion_(request|run)|capture_quarantine|publisher_ready_event|"
                    r"serving_refresh_|schema_migration_state|data_quality_)",
                    n,
                )
            ],
        ),
        Domain(
            "geography",
            "Reference geography",
            "One row per place with yearly attribute and boundary versions and a bridge of relationships "
            "between places. Facts join it by geo_sk; serving views and map tiles read it through "
            "gold_glossary and gold.",
            by_schema.get("silver_ref", [])
            + [
                n
                for n in by_schema.get("gold_glossary", []) + by_schema.get("gold", [])
                if re.search(r"\.(geo_|dim_geo|dim_geography|tile)", n)
            ],
        ),
    ]
    claimed_schemas = {"raw_capture", "silver_ref"}
    for source in WAREHOUSE_SOURCES:
        if source.code in ("CENSUS_DECENNIAL", "CENSUS_GEO"):
            continue
        schemas = _schemas_of(source.raw + source.silver + source.gold) - {
            "control",
            "raw_capture",
        }
        if source.code == "CENSUS_ACS":
            schemas |= {"raw_census", "silver_census", "gold_census"}
        claimed_schemas |= schemas
        controls = [c for c in source.control if c in relations]
        domain_id = source.code.lower().replace("_", "-")
        members = [n for s in sorted(schemas) for n in by_schema.get(s, [])] + controls
        if source.code == "CENSUS_ACS":
            title, caption = (
                "Census ACS and decennial",
                (
                    "ACS and the 2000/1990 censuses share one chain: raw_census describes variables, responses "
                    "replay into silver_census.observation_revision, transform into fact_demographics, and are "
                    "served from gold_census.rpt_acs_observations (one partition per year) and mv_acs_latest "
                    "(newest period only)."
                ),
            )
        else:
            title, caption = (
                source.name,
                _SOURCE_CAPTION.format(publisher=source.publisher),
            )
        domains.append(Domain(domain_id, title, caption, members, source.code))
    contract = re.compile(
        r"\.(fact_observation|v_metric_latest_by_geo|v_metric_timeseries_by_geo)$"
    )
    domains.append(
        Domain(
            "catalog",
            "Catalog, serving contract and app",
            "gold_glossary harvests each source's metric_publisher into dim_metric_catalog, which the API and "
            "web read. gold holds provider-neutral contract views; app_api holds accounts and saved analyses.",
            [
                n
                for s in ("gold_glossary", "gold", "app_api")
                for n in by_schema.get(s, [])
                if not re.search(r"\.(geo_|dim_geo|dim_geography|tile)", n)
            ]
            + [
                n
                for s in ("gold_census", "gold_bls", "gold_fred")
                for n in by_schema.get(s, [])
                if contract.search(n)
            ],
        )
    )
    claimed_schemas |= {"gold_glossary", "gold", "app_api", "control"}
    claimed = {m for d in domains for m in d.members}
    leftovers = [
        n
        for n in names
        if n not in claimed and n.split(".", 1)[0] not in claimed_schemas
    ] + [n for n in names if n not in claimed and n.split(".", 1)[0] == "control"]
    if leftovers:
        domains.append(
            Domain(
                "other",
                "Not yet described",
                "Relations in schemas or ledgers the source inventory does not name yet. A new source appears "
                "here until it is added to apps/api/services/warehouse_inventory.py.",
                sorted(set(leftovers)),
            )
        )
    for d in domains:
        d.members = list(dict.fromkeys(m for m in d.members if m in relations))
    return [d for d in domains if d.members]


# ---------------------------------------------------------------- layout

BOX_WIDTH = 236
COLUMN_GAP = 74
HEADER = 24
ROW = 16
VERTICAL_GAP = 16
COLUMN_LIMIT = 1100


def _edges(
    domain: Domain, relations: dict[str, Relation]
) -> tuple[set[str], list[dict[str, Any]]]:
    members = set(domain.members)
    nodes = set(members)
    edges: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for m in domain.members:
        relation = relations[m]
        for cols, ref in relation.foreign_keys:
            if (
                ref == m
                or (ref not in members and ref not in SHARED)
                or ref not in relations
            ):
                continue
            key = (ref, m, "fk")
            if key in seen:
                continue
            seen.add(key)
            nodes.add(ref)
            edges.append({"from": ref, "to": m, "kind": "fk", "columns": cols})
        for dep in relation.reads_from:
            if (
                dep == m
                or (dep not in members and dep not in SHARED)
                or dep not in relations
            ):
                continue
            key = (dep, m, "reads")
            if key in seen:
                continue
            seen.add(key)
            nodes.add(dep)
            edges.append({"from": dep, "to": m, "kind": "reads", "columns": []})
    return nodes, edges


def layout_domain(domain: Domain, relations: dict[str, Relation]) -> dict[str, Any]:
    """Columns by dependency depth, ordered to reduce crossings, wrapped when tall."""
    nodes, edges = _edges(domain, relations)
    members = set(domain.members)
    shared = {n for n in nodes if n not in members}
    preds: dict[str, set[str]] = defaultdict(set)
    for e in edges:
        preds[e["to"]].add(e["from"])
    rank: dict[str, int] = {}

    def rank_of(n: str, stack: tuple[str, ...] = ()) -> int:
        if n in rank:
            return rank[n]
        if n in shared:
            rank[n] = 0
            return 0
        if n in stack:
            return 0
        base = {"capture": 1, "silver": 2}.get(layer(n), 3)
        rank[n] = max([rank_of(p, stack + (n,)) + 1 for p in preds[n]] + [base])
        return rank[n]

    for n in nodes:
        rank_of(n)
    used = {v: i for i, v in enumerate(sorted(set(rank.values())))}
    rank = {n: used[v] for n, v in rank.items()}
    cols: dict[int, list[str]] = defaultdict(list)
    for n in sorted(nodes, key=lambda n: (layer(n), n)):
        cols[rank[n]].append(n)

    def height(n: str) -> int:
        if n in shared:
            return HEADER + 6
        picked, total = key_columns(relations[n])
        return HEADER + 6 + ROW * (len(picked) + (1 if total > len(picked) else 0)) + 4

    position: dict[str, int] = {}
    for c in sorted(cols):
        for i, n in enumerate(cols[c]):
            position[n] = i
    for _ in range(3):
        for c in sorted(cols)[1:]:

            def centre(n: str) -> float:
                ps = [position[p] for p in preds[n] if p in position]
                return sum(ps) / len(ps) if ps else position.get(n, 0)

            cols[c].sort(key=centre)
            for i, n in enumerate(cols[c]):
                position[n] = i
    wrapped: list[list[str]] = []
    for c in sorted(cols):
        current: list[str] = []
        used_height = 0
        for n in cols[c]:
            if current and used_height + height(n) > COLUMN_LIMIT:
                wrapped.append(current)
                current, used_height = [], 0
            current.append(n)
            used_height += height(n) + VERTICAL_GAP
        wrapped.append(current)
    boxes = []
    x, max_y = 10, 0
    for column in wrapped:
        y = 40
        for n in column:
            relation = relations[n]
            picked, total = key_columns(relation)
            boxes.append(
                {
                    "name": n,
                    "layer": layer(n),
                    "kind": relation.kind,
                    "shared": n in shared,
                    "partitions": relation.partitions,
                    "x": x,
                    "y": y,
                    "width": BOX_WIDTH,
                    "height": height(n),
                    "columns": [] if n in shared else [list(p) for p in picked],
                    "more_columns": 0 if n in shared else total - len(picked),
                }
            )
            y += height(n) + VERTICAL_GAP
        max_y = max(max_y, y)
        x += BOX_WIDTH + COLUMN_GAP
    return {
        "id": domain.id,
        "title": domain.title,
        "caption": domain.caption,
        "source_code": domain.source_code,
        "tables": sum(1 for m in domain.members if "view" not in relations[m].kind),
        "views": sum(1 for m in domain.members if "view" in relations[m].kind),
        "width": x - COLUMN_GAP + 10,
        "height": max_y + 10,
        "boxes": boxes,
        "edges": edges,
    }


# ---------------------------------------------------------------- the atlas

_CACHE_SECONDS = 300
_cache_lock = threading.Lock()
_cache: tuple[float, dict[str, Any]] | None = None


def build_atlas(relations: dict[str, Relation]) -> dict[str, Any]:
    domains = build_domains(relations)
    domain_of = {m: d.id for d in domains for m in d.members}
    return {
        "stats": {
            "schemas": len({r.schema for r in relations.values()}),
            "tables": sum(1 for r in relations.values() if "view" not in r.kind),
            "views": sum(1 for r in relations.values() if "view" in r.kind),
            "partitions": sum(r.partitions for r in relations.values()),
            "foreign_keys": sum(len(r.foreign_keys) for r in relations.values()),
        },
        "domains": [layout_domain(d, relations) for d in domains],
        "relations": [
            {
                "name": r.name,
                "layer": layer(r.name),
                "kind": r.kind,
                "domain": domain_of.get(r.name),
                "columns": [list(c) for c in r.columns],
                "primary_key": r.primary_key,
                "uniques": r.uniques,
                "foreign_keys": [
                    {"columns": cols, "references": ref} for cols, ref in r.foreign_keys
                ],
                "reads_from": r.reads_from,
                "partition_key": r.partition_key,
                "partitions": r.partitions,
                "estimated_rows": r.estimated_rows,
                "index_count": r.index_count,
            }
            for r in sorted(relations.values(), key=lambda r: r.name)
        ],
    }


def warehouse_atlas(db: Session, *, now: float | None = None) -> dict[str, Any]:
    """The atlas, read from the catalog at most once per ``_CACHE_SECONDS`` per process."""
    global _cache
    now = time.monotonic() if now is None else now
    with _cache_lock:
        if _cache is not None and now - _cache[0] < _CACHE_SECONDS:
            return _cache[1]
    atlas = build_atlas(read_catalog(db))
    atlas["read_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with _cache_lock:
        _cache = (now, atlas)
    return atlas


def clear_atlas_cache() -> None:
    global _cache
    with _cache_lock:
        _cache = None
