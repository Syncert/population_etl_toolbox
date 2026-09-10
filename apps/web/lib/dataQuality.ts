// Source coverage and data-quality models.
//
// The warehouse publishes a data-quality signal and the API serves it; this
// module presents that signal and never recomputes it. In particular there
// is no universal quality score here, and no ratio standing in for one: a
// single number over unlike measures would be a client-authored judgement
// wearing the appearance of a published fact.
//
// The second rule is that the distinct states stay distinct. "Current",
// "stale", "retired", and "never published" are four different facts about a
// source, and none of them is zero. Where the API publishes no dedicated
// quality resource for a kind of evidence, this module says where that
// evidence actually lives rather than inventing a surface for it.

import type { MetricSummary } from "./api/types";

/** Per-source publication state, exactly as `/catalog/freshness` rolls it up. */
export interface SourceFreshness {
  source_code: string;
  metric_count: number;
  current_count: number;
  stale_count: number;
  retired_count: number;
  latest_publication_time?: string | null;
  latest_harvested_at?: string | null;
  [key: string]: unknown;
}

export interface FreshnessRow {
  sourceCode: string;
  metricCount: number;
  currentCount: number;
  staleCount: number;
  retiredCount: number;
  /**
   * Metrics the rollup counts but places in none of the three published
   * states. Surfaced rather than folded into one of them, because a metric
   * whose state the warehouse did not publish is not thereby current.
   */
  unclassifiedCount: number;
  publishedAt: string;
  harvestedAt: string;
  /** Shared request-state vocabulary value, for the status pill. */
  state: string;
  /** What the counts say, in words. Never a score. */
  summary: string;
}

function count(value: unknown): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric >= 0 ? numeric : 0;
}

function text(value: unknown): string {
  return typeof value === "string" && value ? value : "";
}

/**
 * One source's published freshness rollup as a row.
 *
 * The state is decided by what the warehouse published, in order of what a
 * reader most needs to know: nothing published at all, then any stale
 * metric, then any metric the rollup left unclassified, then current.
 */
export function freshnessRow(source: SourceFreshness | null | undefined): FreshnessRow {
  const metricCount = count(source?.metric_count);
  const currentCount = count(source?.current_count);
  const staleCount = count(source?.stale_count);
  const retiredCount = count(source?.retired_count);
  const unclassifiedCount = Math.max(
    0,
    metricCount - currentCount - staleCount - retiredCount,
  );
  const publishedAt = text(source?.latest_publication_time);
  const harvestedAt = text(source?.latest_harvested_at);

  let state = "ok";
  let summary = `${currentCount} current of ${metricCount} published metrics`;
  if (metricCount === 0) {
    state = "idle";
    summary = "no metrics published for this source";
  } else if (!publishedAt) {
    // Never presented as healthy: without a publication time nothing
    // establishes that the current count describes a recent publication.
    state = "idle";
    summary = `${metricCount} metrics, but no publication time published`;
  } else if (staleCount > 0) {
    state = "warn";
    summary = `${staleCount} stale of ${metricCount} published metrics`;
  } else if (unclassifiedCount > 0) {
    state = "warn";
    summary = `${unclassifiedCount} of ${metricCount} metrics carry no published freshness state`;
  }

  return {
    sourceCode: text(source?.source_code),
    metricCount,
    currentCount,
    staleCount,
    retiredCount,
    unclassifiedCount,
    publishedAt,
    harvestedAt,
    state,
    summary,
  };
}

export function freshnessRows(
  items: SourceFreshness[] | null | undefined,
): FreshnessRow[] {
  return (Array.isArray(items) ? items : [])
    .map((item) => freshnessRow(item))
    .sort((left, right) => left.sourceCode.localeCompare(right.sourceCode));
}

export interface CoverageSegment {
  label: string;
  count: number;
  /** Share of the source's metric count, for the bar's width only. */
  share: number;
}

/**
 * The composition of one source's metric count.
 *
 * These shares size a bar; they are not a quality measurement, and each
 * segment carries its own published count so the bar is never the only way
 * to read the number.
 */
export function coverageSegments(row: FreshnessRow | null | undefined): CoverageSegment[] {
  const total = row?.metricCount || 0;
  if (!row || total === 0) {
    return [];
  }
  const segments = [
    { label: "current", count: row.currentCount },
    { label: "stale", count: row.staleCount },
    { label: "retired", count: row.retiredCount },
    { label: "no published state", count: row.unclassifiedCount },
  ];
  return segments
    .filter((segment) => segment.count > 0)
    .map((segment) => ({ ...segment, share: segment.count / total }));
}

export interface MetricQualityRow {
  metricCode: string;
  displayName: string;
  freshness: string;
  publishedAt: string;
  harvestedAt: string;
  watermark: string;
  contractVersion: string;
}

/**
 * One metric's published quality context. A field the publisher did not
 * publish stays empty; the caller renders "not published" rather than a
 * placeholder that would state something the source did not.
 */
export function metricQualityRows(
  metrics: MetricSummary[] | null | undefined,
): MetricQualityRow[] {
  return (Array.isArray(metrics) ? metrics : []).map((metric) => ({
    metricCode: metric.metric_code,
    displayName: text(metric.metric_display_name),
    freshness: text(metric.freshness_state),
    publishedAt: text(metric.publication_time),
    harvestedAt: text(metric.harvested_at),
    watermark: text(metric.source_watermark),
    contractVersion: text(metric.publisher_contract_version),
  }));
}

export interface EvidenceLocation {
  /** The kind of quality evidence a reader is looking for. */
  kind: string;
  /** Where the API actually publishes it. */
  publishedBy: string;
  /** How to inspect it, in this application. */
  inspectHere: string;
  /** What it means, including what it is not. */
  meaning: string;
}

/**
 * Where each kind of quality evidence lives.
 *
 * The API publishes a per-source freshness rollup and per-metric provenance;
 * suppression, non-reporting, revision, and coverage evidence live on the
 * observation rows themselves rather than in a separate quality resource. A
 * reader who cannot find a surface for one of them should be told where it
 * really is, not shown a surface this client invented for it.
 */
export const EVIDENCE_LOCATIONS: EvidenceLocation[] = [
  {
    kind: "Freshness",
    publishedBy: "/catalog/freshness and each metric's freshness_state",
    inspectHere: "The source table and metric table on this page.",
    meaning:
      "Whether the warehouse considers a source's published metrics current. A metric with no published freshness state is unknown, not fresh.",
  },
  {
    kind: "Revisions and as-released values",
    publishedBy: "/observations/releases and scope=as_released",
    inspectHere: "The explorer's Publication control, per measure.",
    meaning:
      "Every release a metric published, so an analysis can be reproduced as a specific release stated it.",
  },
  {
    kind: "Suppression and missing values",
    publishedBy: "value and value_status on each observation row",
    inspectHere: "The explorer's observation table, per measure and geography.",
    meaning:
      "A value the source withheld or never published. It is null with a published reason, and is never a zero.",
  },
  {
    kind: "Reporting participation",
    publishedBy: "the coverage object on FBI UCR observation rows",
    inspectHere: "The explorer, when exploring an FBI UCR measure.",
    meaning:
      "Which agencies reported for a period. A period nobody reported is not-reported with a null value, not zero crime.",
  },
  {
    kind: "Geography and time coverage",
    publishedBy: "valid_geo_grains and valid_time_grains on each metric",
    inspectHere: "The catalog's provenance panel and the explorer's metadata view.",
    meaning:
      "The grains a measure is published for. A grain a measure does not publish is not a gap in the place, it is a measure that does not describe it.",
  },
  {
    kind: "Definition and contract changes",
    publishedBy: "publisher_contract_version and source_watermark on each metric",
    inspectHere: "The metric table on this page and the catalog's provenance panel.",
    meaning:
      "The publisher contract a measure was harvested under. A changed version means the definition may have moved.",
  },
];

/**
 * Quality questions this API publishes no resource for.
 *
 * Stated so a reader does not mistake absence for a clean bill of health,
 * and so nothing here fabricates a surface to answer them.
 */
export const UNPUBLISHED_EVIDENCE: string[] = [
  "A single quality score, index, or grade for a source or a measure. None is published, and computing one here would be a client-authored judgement wearing the appearance of a published fact.",
  "Completeness against an external expectation of how many rows a source should have published. The warehouse publishes what it received, not what it expected.",
];
