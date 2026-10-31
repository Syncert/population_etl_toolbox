"use client";

// The admin warehouse page: what every source is, what it holds now, and
// every table and view in the warehouse with its keys.
//
// Nothing on this page is a hand-kept copy. The source map joins the API's
// declared inventory to the live metric catalog and to coverage measured from
// each source's serving relation; the schema atlas is the Postgres catalog,
// read when the page is opened. A new source, grain, year or table appears
// here the next time the page loads.
//
// Admin only, and the API is what enforces it: every read carries the held
// credential, and the API answers 403 for an account not on the deployment's
// admin list (404 when no admin is named at all). The page only explains
// those answers.

import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { RefreshCw } from "lucide-react";
import { ApiError, apiErrorMessage, apiFetch } from "../lib/api/client";
import { useStoredToken } from "../lib/apiToken";
import { formatNumber } from "../lib/format";
import {
  LAYER_LABELS,
  MARK_MEANING,
  clip,
  coverageByColumn,
  edgePath,
  formatWatermark,
  missingYears,
  runLimited,
  shortType,
  yearSpan,
} from "../lib/adminWarehouse";
import type {
  AtlasDomain,
  AtlasRelation,
  SourceCoverage,
  WarehouseSchema,
  WarehouseSource,
  WarehouseSources,
} from "../lib/adminWarehouse";

type Tab = "sources" | "schema";

/** Coverage requests in flight at once; each is one grouped scan server-side. */
const COVERAGE_CONCURRENCY = 3;

function refusal(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 404) {
      return "The admin pages are turned off on this deployment. Name an admin in API_ADMIN_ACCOUNTS (an operator label or an email address) and restart the API.";
    }
    if (error.status === 403) {
      return "Admin only. This account is not on the deployment's admin list (API_ADMIN_ACCOUNTS).";
    }
    if (error.status === 401) {
      return "Your sign-in has expired or the token was not recognised. Sign in again, or paste an operator token on Saved analyses.";
    }
  }
  return apiErrorMessage(error);
}

function formatCount(value: number | null | undefined): string {
  return value == null ? "" : formatNumber(value, { maximumFractionDigits: 0 });
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString().slice(0, 16).replace("T", " ") + " UTC";
}

export default function AdminWarehousePage() {
  const { token, resolved } = useStoredToken();
  const [tab, setTab] = useState<Tab>("sources");
  const [sources, setSources] = useState<WarehouseSources | null>(null);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    try {
      if (window.location.hash === "#schema") setTab("schema");
    } catch {
      // No location in an unusual embedding: the default tab stands.
    }
  }, []);

  useEffect(() => {
    if (!resolved || !token) return;
    const controller = new AbortController();
    setError("");
    apiFetch<WarehouseSources>("/admin/warehouse/sources", { token, signal: controller.signal })
      .then(setSources)
      .catch((caught) => {
        if (!controller.signal.aborted) setError(refusal(caught));
      });
    return () => controller.abort();
  }, [resolved, token]);

  const choose = (next: Tab) => {
    setTab(next);
    try {
      window.history.replaceState(null, "", next === "schema" ? "#schema" : window.location.pathname);
    } catch {
      // History unavailable: the tab still changes.
    }
  };

  return (
    <main className="admin-warehouse" data-testid="admin-warehouse">
      <header className="aw-head">
        <p className="aw-eyebrow">Admin</p>
        <h1>What is in the warehouse</h1>
        <p className="aw-lede">
          Every source, the grains and years it holds, and every table and view with its keys. Read
          live from the warehouse each time this page opens, so it changes as the warehouse does.
        </p>
      </header>

      {!resolved ? (
        <p className="aw-note">Checking for a sign-in…</p>
      ) : !token ? (
        <div className="aw-panel" data-testid="admin-warehouse-signed-out">
          <h2>Admin only</h2>
          <p>
            Sign in with an admin account, or paste an operator token on{" "}
            <Link href="/saved">Saved analyses</Link>, then come back to this page.
          </p>
        </div>
      ) : error ? (
        <div className="aw-panel aw-refused" role="alert" data-testid="admin-warehouse-refused">
          <h2>Not available</h2>
          <p>{error}</p>
        </div>
      ) : (
        <>
          <div className="aw-tabs" role="tablist" aria-label="Warehouse views">
            <button
              type="button"
              role="tab"
              id="aw-tab-sources"
              aria-selected={tab === "sources"}
              aria-controls="aw-panel-sources"
              onClick={() => choose("sources")}
            >
              Source map
            </button>
            <button
              type="button"
              role="tab"
              id="aw-tab-schema"
              aria-selected={tab === "schema"}
              aria-controls="aw-panel-schema"
              onClick={() => choose("schema")}
            >
              Schema atlas
            </button>
          </div>
          {tab === "sources" ? (
            <section id="aw-panel-sources" role="tabpanel" aria-labelledby="aw-tab-sources">
              {sources ? <SourceMap data={sources} token={token} /> : <p className="aw-note">Loading sources…</p>}
            </section>
          ) : (
            <section id="aw-panel-schema" role="tabpanel" aria-labelledby="aw-tab-schema">
              <SchemaAtlas token={token} />
            </section>
          )}
        </>
      )}
    </main>
  );
}

// ------------------------------------------------------------------ source map

function SourceMap({ data, token }: { data: WarehouseSources; token: string }) {
  const [coverage, setCoverage] = useState<Record<string, SourceCoverage>>({});
  const [failed, setFailed] = useState<Record<string, string>>({});
  const [measuring, setMeasuring] = useState<Record<string, boolean>>({});
  const [topic, setTopic] = useState("");
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState<string>("");

  const measure = useCallback(
    async (code: string, refresh = false) => {
      setMeasuring((current) => ({ ...current, [code]: true }));
      try {
        const result = await apiFetch<SourceCoverage>(`/admin/warehouse/coverage/${encodeURIComponent(code)}`, {
          token,
          params: refresh ? { refresh: true } : {},
        });
        setCoverage((current) => ({ ...current, [code]: result }));
        setFailed((current) => {
          const { [code]: _dropped, ...rest } = current;
          return rest;
        });
      } catch (caught) {
        setFailed((current) => ({ ...current, [code]: refusal(caught) }));
      } finally {
        setMeasuring((current) => ({ ...current, [code]: false }));
      }
    },
    [token],
  );

  useEffect(() => {
    void runLimited(
      data.sources.map((source) => source.code),
      COVERAGE_CONCURRENCY,
      (code) => measure(code),
    );
  }, [data, measure]);

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return data.sources.filter((source) => {
      if (topic && source.topic !== topic) return false;
      if (!needle) return true;
      return [source.code, source.name, source.publisher, source.measures, ...source.datasets, ...source.gold]
        .join(" ")
        .toLowerCase()
        .includes(needle);
    });
  }, [data, topic, query]);

  const totals = useMemo(() => {
    const metrics = data.sources.reduce((sum, source) => sum + (source.catalog?.metrics || 0), 0);
    const measured = Object.values(coverage).filter((entry) => entry.status === "ok").length;
    return { metrics, measured };
  }, [data, coverage]);

  return (
    <div className="aw-sources">
      <p className="aw-facts">
        <span>
          <b>{data.sources.length}</b> sources
        </span>
        <span>
          <b>{formatCount(totals.metrics)}</b> catalog metrics
        </span>
        <span>
          <b>{totals.measured}</b> measured live
        </span>
        <span>Catalog read {formatDate(data.catalog_read_at)}</span>
      </p>
      <div className="aw-filters">
        <label>
          Topic{" "}
          <select value={topic} onChange={(event) => setTopic(event.target.value)}>
            <option value="">All topics</option>
            {data.topics.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Search{" "}
          <input
            type="search"
            value={query}
            placeholder="source, dataset, table…"
            onChange={(event) => setQuery(event.target.value)}
          />
        </label>
      </div>
      <p className="aw-legend-text">
        Each grain cell shows the years held, measured from the source&apos;s serving relation. Its mark says
        how the figure is made: <b>n</b> {MARK_MEANING.n}; <b>d</b> {MARK_MEANING.d}; <b>p</b> {MARK_MEANING.p}.
        A grey cell is declared but holds nothing yet.
      </p>
      <div className="aw-table-wrap">
        <table className="aw-table" data-testid="admin-source-map">
          <thead>
            <tr>
              <th scope="col">Source</th>
              {data.grains.map(([code, label]) => (
                <th scope="col" key={code} className="aw-grain-head" title={label}>
                  {label}
                </th>
              ))}
              <th scope="col">Metrics</th>
              <th scope="col">Newest</th>
              <th scope="col">Refresh</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((source) => (
              <SourceRow
                key={source.code}
                source={source}
                grains={data.grains}
                coverage={coverage[source.code]}
                failure={failed[source.code]}
                measuring={Boolean(measuring[source.code])}
                open={open === source.code}
                onToggle={() => setOpen(open === source.code ? "" : source.code)}
                onRemeasure={() => void measure(source.code, true)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SourceRow({
  source,
  grains,
  coverage,
  failure,
  measuring,
  open,
  onToggle,
  onRemeasure,
}: {
  source: WarehouseSource;
  grains: [string, string][];
  coverage: SourceCoverage | undefined;
  failure: string | undefined;
  measuring: boolean;
  open: boolean;
  onToggle: () => void;
  onRemeasure: () => void;
}) {
  const marks = Object.fromEntries(source.grains.map((g) => [g.grain, g.mark]));
  const held = coverageByColumn(coverage);
  const live = coverage?.status === "ok";
  return (
    <>
      <tr className={open ? "aw-row aw-open" : "aw-row"}>
        <th scope="row">
          <button type="button" className="aw-name" aria-expanded={open} onClick={onToggle}>
            <span className="aw-source-name">{source.name}</span>
            <span className="aw-publisher">
              {source.publisher} · <code>{source.code}</code>
            </span>
          </button>
        </th>
        {grains.map(([code, label]) => {
          const mark = marks[code];
          const cell = held[code];
          if (!mark && !cell) return <td key={code} className="aw-cell aw-none" aria-label={`${label}: not published`} />;
          const span = cell ? yearSpan(cell.first, cell.last) : "";
          const empty = live && !cell;
          const gaps = cell ? missingYears(cell.years) : [];
          const title = [
            `${label}${mark ? `: ${MARK_MEANING[mark] || mark}` : ""}`,
            span ? `Held ${span}${gaps.length ? `, missing ${gaps.join(", ")}` : ""}` : empty ? "Declared, nothing held yet" : "",
            cell?.rows != null ? `${formatCount(cell.rows)} rows` : "",
          ]
            .filter(Boolean)
            .join(" · ");
          return (
            <td key={code} className={`aw-cell aw-mark-${mark || "x"}${empty ? " aw-empty" : ""}`} title={title}>
              <span className="aw-mark">{mark || "·"}</span>
              <span className="aw-years">
                {span || (live ? "—" : measuring ? "…" : "")}
                {gaps.length ? <sup title={`missing ${gaps.join(", ")}`}>*</sup> : null}
              </span>
            </td>
          );
        })}
        <td className="aw-num">{formatCount(source.catalog?.metrics)}</td>
        <td className="aw-newest">{formatWatermark(source.catalog?.newest_watermark)}</td>
        <td className="aw-cadence">{source.cadence}</td>
      </tr>
      {open ? (
        <tr className="aw-detail">
          <td colSpan={grains.length + 4}>
            <SourceDetail
              source={source}
              coverage={coverage}
              failure={failure}
              measuring={measuring}
              onRemeasure={onRemeasure}
            />
          </td>
        </tr>
      ) : null}
    </>
  );
}

function SourceDetail({
  source,
  coverage,
  failure,
  measuring,
  onRemeasure,
}: {
  source: WarehouseSource;
  coverage: SourceCoverage | undefined;
  failure: string | undefined;
  measuring: boolean;
  onRemeasure: () => void;
}) {
  const lineage: [string, string[]][] = [
    ["Raw capture", source.raw],
    ["Control", source.control],
    ["Silver", source.silver],
    ["Gold", source.gold],
  ];
  return (
    <div className="aw-detail-body">
      <div className="aw-detail-cols">
        <div>
          <h3>What it sources</h3>
          <ul className="aw-list">
            {source.datasets.map((dataset) => (
              <li key={dataset}>{dataset}</li>
            ))}
          </ul>
          <p>{source.measures}</p>
          <dl className="aw-dl">
            <dt>Topic</dt>
            <dd>{source.topic}</dd>
            <dt>Publisher releases</dt>
            <dd>{source.release}</dd>
            <dt>Loader</dt>
            <dd>
              <code>{source.module}</code> · DAG <code>{source.dag}</code> · {source.cadence}
            </dd>
            <dt>Declared years</dt>
            <dd>
              {source.declared_years.map(([first, last]) => yearSpan(first ?? null, last ?? null)).join(", ")}
              {source.years_note ? ` (${source.years_note})` : ""}
            </dd>
            <dt>Credential</dt>
            <dd>{source.credential ? <code>{source.credential}</code> : "None"}</dd>
            {source.catalog ? (
              <>
                <dt>Catalog</dt>
                <dd>
                  {formatCount(source.catalog.metrics)} metrics ({formatCount(source.catalog.current_metrics)} current
                  {source.catalog.stale_metrics ? `, ${formatCount(source.catalog.stale_metrics)} stale` : ""}); grains{" "}
                  {source.catalog.geo_grains.join(", ") || "none"}; time grains {source.catalog.time_grains.join(", ") || "none"};
                  last harvested {formatDate(source.catalog.last_harvested)}
                </dd>
              </>
            ) : null}
          </dl>
        </div>
        <div>
          <h3>Where it lands</h3>
          <ol className="aw-lineage">
            {lineage
              .filter(([, tables]) => tables.length)
              .map(([stage, tables]) => (
                <li key={stage}>
                  <span className="aw-stage">{stage}</span>
                  {tables.map((table) => (
                    <code key={table}>{table}</code>
                  ))}
                </li>
              ))}
          </ol>
          <h3>
            Held now{" "}
            <button type="button" className="aw-refresh" onClick={onRemeasure} disabled={measuring}>
              <RefreshCw size={13} aria-hidden="true" /> {measuring ? "Measuring…" : "Measure now"}
            </button>
          </h3>
          {failure ? <p className="aw-warn">{failure}</p> : null}
          {coverage ? <CoverageTable coverage={coverage} /> : !failure ? <p className="aw-note">Measuring…</p> : null}
        </div>
      </div>
    </div>
  );
}

function CoverageTable({ coverage }: { coverage: SourceCoverage }) {
  const meta = (
    <p className="aw-meta">
      {coverage.relation ? (
        <>
          From <code>{coverage.relation}</code>{" "}
        </>
      ) : null}
      {coverage.status === "ok"
        ? `· measured ${formatDate(coverage.measured_at)}${coverage.cached ? " (cached)" : ""} in ${formatCount(coverage.elapsed_ms)} ms`
        : null}
    </p>
  );
  if (coverage.status !== "ok") {
    return (
      <>
        {meta}
        <p className={coverage.status === "timed_out" || coverage.status === "unavailable" ? "aw-warn" : "aw-note"}>
          {coverage.note || coverage.status}
        </p>
      </>
    );
  }
  return (
    <>
      {meta}
      {coverage.note ? <p className="aw-note">{coverage.note}</p> : null}
      {coverage.grains.length === 0 ? (
        <p className="aw-warn">The serving relation holds no rows.</p>
      ) : (
        <table className="aw-mini">
          <thead>
            <tr>
              <th scope="col">Grain</th>
              <th scope="col">Years</th>
              <th scope="col">Missing</th>
              <th scope="col">Rows</th>
            </tr>
          </thead>
          <tbody>
            {coverage.grains.map((grain) => {
              const gaps = missingYears(grain.years);
              return (
                <tr key={grain.grain || "unknown"}>
                  <th scope="row">{grain.grain || "(none)"}</th>
                  <td>{yearSpan(grain.first_year, grain.last_year)}</td>
                  <td>{gaps.length ? clip(gaps.join(", "), 60) : ""}</td>
                  <td className="aw-num">{formatCount(grain.row_count)}</td>
                </tr>
              );
            })}
          </tbody>
          <tfoot>
            <tr>
              <th scope="row">All</th>
              <td colSpan={2} />
              <td className="aw-num">
                {coverage.row_count_is_estimate ? "≈ " : ""}
                {formatCount(coverage.row_count)}
              </td>
            </tr>
          </tfoot>
        </table>
      )}
    </>
  );
}

// ---------------------------------------------------------------- schema atlas

function SchemaAtlas({ token }: { token: string }) {
  const [schema, setSchema] = useState<WarehouseSchema | null>(null);
  const [error, setError] = useState("");
  const [domainId, setDomainId] = useState("backbone");
  const [query, setQuery] = useState("");
  const [focus, setFocus] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    apiFetch<WarehouseSchema>("/admin/warehouse/schema", { token, signal: controller.signal })
      .then((loaded) => {
        setSchema(loaded);
        if (!loaded.domains.some((domain) => domain.id === "backbone") && loaded.domains[0]) {
          setDomainId(loaded.domains[0].id);
        }
      })
      .catch((caught) => {
        if (!controller.signal.aborted) setError(refusal(caught));
      });
    return () => controller.abort();
  }, [token]);

  const relationByName = useMemo(
    () => new Map((schema?.relations || []).map((relation) => [relation.name, relation])),
    [schema],
  );
  const matches = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!schema || needle.length < 2) return [];
    return schema.relations
      .filter(
        (relation) =>
          relation.name.toLowerCase().includes(needle) ||
          relation.columns.some(([name]) => name.toLowerCase().includes(needle)),
      )
      .slice(0, 60);
  }, [schema, query]);

  if (error) return <p className="aw-warn" role="alert">{error}</p>;
  if (!schema) return <p className="aw-note">Reading the warehouse catalog…</p>;
  const domain = schema.domains.find((entry) => entry.id === domainId) || schema.domains[0];

  const openRelation = (name: string) => {
    const relation = relationByName.get(name);
    if (relation?.domain) setDomainId(relation.domain);
    setFocus(name);
  };

  return (
    <div className="aw-atlas">
      <p className="aw-facts">
        <span>
          <b>{schema.stats.schemas}</b> schemas
        </span>
        <span>
          <b>{schema.stats.tables}</b> tables
        </span>
        <span>
          <b>{schema.stats.views}</b> views
        </span>
        <span>
          <b>{schema.stats.partitions}</b> partitions
        </span>
        <span>
          <b>{schema.stats.foreign_keys}</b> foreign keys
        </span>
        <span>Read {formatDate(schema.read_at)}</span>
      </p>
      <div className="aw-picker" role="group" aria-label="Diagram">
        {schema.domains.map((entry) => (
          <button
            type="button"
            key={entry.id}
            aria-pressed={entry.id === domain?.id}
            onClick={() => {
              setDomainId(entry.id);
              setFocus("");
            }}
          >
            {entry.title}
          </button>
        ))}
      </div>
      {domain ? <DomainDiagram domain={domain} focus={focus} onPick={openRelation} /> : null}
      {focus && relationByName.get(focus) ? <RelationCard relation={relationByName.get(focus)!} onPick={openRelation} /> : null}
      <div className="aw-search">
        <label>
          Find a table, view or column{" "}
          <input type="search" value={query} placeholder="e.g. geo_sk, fact_demographics" onChange={(event) => setQuery(event.target.value)} />
        </label>
        {matches.length ? (
          <ul className="aw-matches">
            {matches.map((relation) => (
              <li key={relation.name}>
                <button type="button" onClick={() => openRelation(relation.name)}>
                  <code>{relation.name}</code>
                </button>{" "}
                <span className="aw-meta">
                  {relation.kind} · {relation.columns.length} columns
                  {relation.partitions ? ` · ${relation.partitions} partitions` : ""}
                </span>
              </li>
            ))}
          </ul>
        ) : null}
      </div>
    </div>
  );
}

function DomainDiagram({
  domain,
  focus,
  onPick,
}: {
  domain: AtlasDomain;
  focus: string;
  onPick: (name: string) => void;
}) {
  const [hover, setHover] = useState("");
  const boxes = useMemo(() => new Map(domain.boxes.map((box) => [box.name, box])), [domain]);
  const lit = hover || focus;
  return (
    <figure className="aw-figure">
      <figcaption>
        <b>{domain.title}</b> · {domain.tables} tables, {domain.views} views. {domain.caption}
      </figcaption>
      <p className="aw-legend">
        {Object.entries(LAYER_LABELS).map(([layer, label]) => (
          <span key={layer} className="aw-key">
            <i className={`aw-swatch L-${layer}`} aria-hidden="true" />
            {label}
          </span>
        ))}
        <span className="aw-key">Solid line: foreign key. Dashed: a view reads it. Click a box for its columns.</span>
      </p>
      <div className="aw-well">
        <svg
          className="aw-erd"
          viewBox={`0 0 ${domain.width} ${domain.height}`}
          width={domain.width}
          height={domain.height}
          role="img"
          aria-label={`${domain.title}: ${domain.boxes.length} tables and views and their keys`}
        >
          <defs>
            <marker id="aw-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" className="aw-arrowhead" />
            </marker>
          </defs>
          {domain.edges.map((edge, index) => {
            const from = boxes.get(edge.from);
            const to = boxes.get(edge.to);
            if (!from || !to) return null;
            const on = lit && (edge.from === lit || edge.to === lit);
            return (
              <path
                key={`${edge.from}-${edge.to}-${index}`}
                className={`aw-edge aw-edge-${edge.kind}${on ? " aw-edge-on" : ""}`}
                d={edgePath(from, to, edge.columns[0])}
                markerEnd="url(#aw-arrow)"
              >
                <title>
                  {edge.kind === "fk"
                    ? `${edge.to}.${edge.columns.join(", ")} references ${edge.from}`
                    : `${edge.to} reads ${edge.from}`}
                </title>
              </path>
            );
          })}
          {domain.boxes.map((box) => {
            const [schemaName, relationName] = box.name.split(".");
            const tag = box.shared
              ? schemaName
              : [box.kind === "view" ? "view" : "", box.partitions ? `${box.partitions} parts` : ""].filter(Boolean).join(" · ");
            return (
              <g
                key={box.name}
                className={`aw-box L-${box.layer}${box.shared ? " aw-shared" : ""}${box.name === lit ? " aw-box-on" : ""}`}
                tabIndex={0}
                role="button"
                aria-label={`${box.name}, ${box.kind}`}
                onMouseEnter={() => setHover(box.name)}
                onMouseLeave={() => setHover("")}
                onClick={() => onPick(box.name)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onPick(box.name);
                  }
                }}
              >
                <title>{`${box.name} (${box.kind}${box.shared ? ", shared" : ""})`}</title>
                <rect className="aw-bx" x={box.x} y={box.y} width={box.width} height={box.height} rx={5} />
                <rect className="aw-hd" x={box.x} y={box.y} width={box.width} height={24} rx={5} />
                <rect className="aw-hd" x={box.x} y={box.y + 18} width={box.width} height={6} />
                <text className="aw-tn" x={box.x + 8} y={box.y + 16}>
                  {clip(relationName || box.name, 30)}
                </text>
                <text className="aw-ts" x={box.x + box.width - 8} y={box.y + 16} textAnchor="end">
                  {tag}
                </text>
                {box.columns.map(([name, type, badge], index) => {
                  const y = box.y + 24 + 6 + 16 * index + 11;
                  return (
                    <Fragment key={name}>
                      {badge ? (
                        <text className={`aw-bd aw-b-${badge.toLowerCase()}`} x={box.x + 8} y={y}>
                          {badge}
                        </text>
                      ) : null}
                      <text className="aw-cn" x={box.x + 30} y={y}>
                        {clip(name, 22)}
                      </text>
                      <text className="aw-ct" x={box.x + box.width - 8} y={y} textAnchor="end">
                        {shortType(type)}
                      </text>
                    </Fragment>
                  );
                })}
                {box.more_columns > 0 ? (
                  <text className="aw-more" x={box.x + 30} y={box.y + 24 + 6 + 16 * box.columns.length + 11}>
                    +{box.more_columns} more columns
                  </text>
                ) : null}
              </g>
            );
          })}
        </svg>
      </div>
    </figure>
  );
}

function RelationCard({ relation, onPick }: { relation: AtlasRelation; onPick: (name: string) => void }) {
  const keyed = new Set(relation.primary_key);
  const referencing = new Set(relation.foreign_keys.flatMap((fk) => fk.columns));
  return (
    <section className="aw-panel aw-relation" aria-label={relation.name} data-testid="admin-relation-card">
      <h3>
        <code>{relation.name}</code> <span className="aw-meta">{relation.kind}</span>
      </h3>
      <p className="aw-meta">
        {LAYER_LABELS[relation.layer] || relation.layer}
        {relation.estimated_rows != null ? ` · about ${formatCount(relation.estimated_rows)} rows` : ""}
        {relation.partitions ? ` · ${relation.partitions} partitions by ${relation.partition_key}` : ""}
        {relation.index_count ? ` · ${relation.index_count} indexes` : ""}
      </p>
      <table className="aw-mini">
        <thead>
          <tr>
            <th scope="col">Column</th>
            <th scope="col">Type</th>
            <th scope="col">Key</th>
          </tr>
        </thead>
        <tbody>
          {relation.columns.map(([name, type, notNull]) => (
            <tr key={name}>
              <th scope="row">
                <code>{name}</code>
              </th>
              <td>
                {type}
                {notNull ? " not null" : ""}
              </td>
              <td>{keyed.has(name) ? "PK" : referencing.has(name) ? "FK" : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {relation.uniques.length ? <p className="aw-meta">Unique: {relation.uniques.join("; ")}</p> : null}
      {relation.foreign_keys.length ? (
        <p className="aw-meta">
          References:{" "}
          {relation.foreign_keys.map((fk, index) => (
            <Fragment key={`${fk.references}-${index}`}>
              {index ? ", " : ""}
              <button type="button" className="aw-link" onClick={() => onPick(fk.references)}>
                {fk.references}
              </button>{" "}
              ({fk.columns.join(", ")})
            </Fragment>
          ))}
        </p>
      ) : null}
      {relation.reads_from.length ? (
        <p className="aw-meta">
          Reads:{" "}
          {relation.reads_from.map((name, index) => (
            <Fragment key={name}>
              {index ? ", " : ""}
              <button type="button" className="aw-link" onClick={() => onPick(name)}>
                {name}
              </button>
            </Fragment>
          ))}
        </p>
      ) : null}
    </section>
  );
}
