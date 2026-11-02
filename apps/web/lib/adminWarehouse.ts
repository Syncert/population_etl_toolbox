// The admin warehouse page's data shapes and the pure pieces of its rendering.
//
// Everything the page shows is read from the API on each visit:
// `/admin/warehouse/sources` (the declared inventory joined to the live metric
// catalog), `/admin/warehouse/coverage/{code}` (years per grain, measured from
// each source's serving relation) and `/admin/warehouse/schema` (every table
// and view, read from the Postgres catalog). Nothing here is a second copy of
// the warehouse's description; it only decides how to draw it.

export interface GrainMark {
  grain: string;
  mark: string;
}

export interface CatalogSummary {
  metrics: number;
  current_metrics: number;
  stale_metrics: number;
  geo_grains: string[];
  time_grains: string[];
  newest_watermark: string | null;
  newest_publication: string | null;
  last_harvested: string | null;
}

export interface WarehouseSource {
  code: string;
  name: string;
  publisher: string;
  topic: string;
  module: string;
  dag: string;
  cadence: string;
  release: string;
  grains: GrainMark[];
  declared_years: number[][];
  years_note: string;
  datasets: string[];
  measures: string;
  raw: string[];
  control: string[];
  silver: string[];
  gold: string[];
  credential: string;
  catalog_source_code: string | null;
  catalog: CatalogSummary | null;
}

export interface WarehouseSources {
  grains: [string, string][];
  topics: string[];
  sources: WarehouseSource[];
  catalog_read_at: string;
}

export interface GrainCoverage {
  grain: string | null;
  column: string;
  first_year: number | null;
  last_year: number | null;
  years: number[];
  row_count: number | null;
}

export interface SourceCoverage {
  source_code: string;
  status: "ok" | "shared" | "not_served" | "timed_out" | "unavailable";
  relation: string | null;
  method: string | null;
  note: string | null;
  grains: GrainCoverage[];
  row_count: number | null;
  row_count_is_estimate: boolean;
  measured_at: string;
  elapsed_ms: number;
  cached: boolean;
}

export interface AtlasBox {
  name: string;
  layer: string;
  kind: string;
  shared: boolean;
  partitions: number;
  x: number;
  y: number;
  width: number;
  height: number;
  columns: [string, string, string][];
  more_columns: number;
}

export interface AtlasEdge {
  from: string;
  to: string;
  kind: "fk" | "reads";
  columns: string[];
}

export interface AtlasDomain {
  id: string;
  title: string;
  caption: string;
  source_code: string | null;
  tables: number;
  views: number;
  width: number;
  height: number;
  boxes: AtlasBox[];
  edges: AtlasEdge[];
}

export interface AtlasRelation {
  name: string;
  layer: string;
  kind: string;
  domain: string | null;
  columns: [string, string, boolean][];
  primary_key: string[];
  uniques: string[];
  foreign_keys: { columns: string[]; references: string }[];
  reads_from: string[];
  partition_key: string | null;
  partitions: number;
  estimated_rows: number | null;
  index_count: number;
}

export interface WarehouseSchema {
  stats: { schemas: number; tables: number; views: number; partitions: number; foreign_keys: number };
  domains: AtlasDomain[];
  relations: AtlasRelation[];
  read_at: string;
}

/** What each grain mark means, for the legend and cell titles. */
export const MARK_MEANING: Readonly<Record<string, string>> = {
  n: "the publisher's own figure",
  d: "built by the warehouse from finer records",
  p: "some programs or areas only",
};

/** The colour family of each warehouse layer (see admin.css). */
export const LAYER_LABELS: Readonly<Record<string, string>> = {
  capture: "Raw capture and control",
  silver: "Silver (cleaned, revisioned)",
  gold: "Gold (published)",
  serving: "Serving (catalog and contract views)",
  app: "App storage",
};

/** "2009–2024", "2021", or "" when nothing is held. */
export function yearSpan(first: number | null, last: number | null): string {
  if (first == null || last == null) return "";
  return first === last ? String(first) : `${first}–${last}`;
}

/**
 * A source watermark as a reader would write it. Most sources record a period
 * ("2025-12", "2024"); a few record when the publisher last changed the data,
 * as Unix seconds (CDC's "1780605223"). Those are shown as a UTC date.
 */
export function formatWatermark(value: string | null | undefined): string {
  if (!value) return "";
  const trimmed = value.trim();
  if (/^\d{10,13}$/.test(trimmed)) {
    const seconds = trimmed.length > 10 ? Number(trimmed) / 1000 : Number(trimmed);
    const date = new Date(seconds * 1000);
    if (!Number.isNaN(date.getTime())) return date.toISOString().slice(0, 10);
  }
  return trimmed;
}

/** Years missing between the first and last held year. */
export function missingYears(years: readonly number[]): number[] {
  if (years.length < 2) return [];
  const held = new Set(years);
  const gaps: number[] = [];
  for (let year = years[0]!; year <= years[years.length - 1]!; year += 1) {
    if (!held.has(year)) gaps.push(year);
  }
  return gaps;
}

/** Fold measured grains onto the page's columns: span per column, rows summed. */
export function coverageByColumn(
  coverage: SourceCoverage | undefined,
): Record<string, { first: number | null; last: number | null; years: number[]; rows: number | null; grains: string[] }> {
  const folded: Record<
    string,
    { first: number | null; last: number | null; years: number[]; rows: number | null; grains: string[] }
  > = {};
  if (!coverage || coverage.status !== "ok") return folded;
  for (const grain of coverage.grains) {
    const cell = (folded[grain.column] ||= { first: null, last: null, years: [], rows: null, grains: [] });
    if (grain.first_year != null) cell.first = cell.first == null ? grain.first_year : Math.min(cell.first, grain.first_year);
    if (grain.last_year != null) cell.last = cell.last == null ? grain.last_year : Math.max(cell.last, grain.last_year);
    cell.years = [...new Set([...cell.years, ...grain.years])].sort((a, b) => a - b);
    if (grain.row_count != null) cell.rows = (cell.rows || 0) + grain.row_count;
    if (grain.grain) cell.grains.push(grain.grain);
  }
  return folded;
}

/** Run `task` over `items`, at most `limit` at a time. */
export async function runLimited<T>(
  items: readonly T[],
  limit: number,
  task: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const item = items[next++]!;
      await task(item);
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
}

const HEADER = 24;
const ROW = 16;

/** The y of a named column's row inside a box, or the header when not drawn. */
export function columnAnchor(box: AtlasBox, column: string | undefined): number {
  if (!column || box.shared) return box.y + 12;
  const index = box.columns.findIndex(([name]) => name === column);
  return index < 0 ? box.y + 12 : box.y + HEADER + 6 + ROW * index + 8;
}

/** SVG path for an edge: right side of `from` to the left side of `to`. */
export function edgePath(from: AtlasBox, to: AtlasBox, toColumn?: string): string {
  const y1 = from.y + 12;
  const y2 = columnAnchor(to, toColumn);
  if (to.x > from.x) {
    const x1 = from.x + from.width;
    const x2 = to.x;
    const dx = Math.max(30, (x2 - x1) * 0.5);
    return `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
  }
  // Same column, or a wrapped one: loop out to the right.
  const x1 = from.x + from.width;
  const x2 = to.x + to.width;
  return `M${x1},${y1} C${x1 + 46},${y1} ${x2 + 46},${y2} ${x2},${y2}`;
}

/** Postgres type names shortened to fit a box. */
export function shortType(type: string): string {
  let short = type
    .replace("timestamp with time zone", "timestamptz")
    .replace("timestamp without time zone", "timestamp")
    .replace("character varying", "varchar")
    .replace("double precision", "float8")
    .replace(/^boolean$/, "bool")
    .replace(/^integer$/, "int")
    .replace(/^bigint$/, "int8")
    .replace(/^smallint$/, "int2")
    .replace(/geometry\([^)]*\)/i, "geometry");
  if (short.length > 14) short = `${short.slice(0, 13)}…`;
  return short;
}

/** Truncate a label to `max` characters with an ellipsis. */
export function clip(text: string, max: number): string {
  return text.length <= max ? text : `${text.slice(0, max - 1)}…`;
}
