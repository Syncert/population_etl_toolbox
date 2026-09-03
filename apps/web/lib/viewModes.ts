// Which presentation modes a selected measure actually supports.
//
// The explorer used to render every mode for every selection: the map drew
// for a national series it had no polygon for and merely declined to colour,
// and a quality view existed for measures publishing no quality state. A
// mode presented where nothing can answer it reads as "no data" rather than
// "not applicable", which are different facts.
//
// Every decision here is read from published evidence — the measure's own
// catalog row, the source's declared observation routes, and the vector
// layer's published fields — never from a source name or a client-authored
// list of which measures are mappable.

import { metricProvenance, metricQualityState } from "./catalog";
import type { ExplorerSource } from "./explorerSources";
import type { MetricSummary } from "./api/types";

export const EXPLORER_VIEW_MODES = [
  "map",
  "trend",
  "table",
  "metadata",
  "quality",
  "export",
] as const;

export type ExplorerViewMode = (typeof EXPLORER_VIEW_MODES)[number];

export interface ViewModeState {
  supported: boolean;
  /**
   * Why the mode cannot answer, in terms of what is or is not published.
   * Empty when the mode is supported.
   */
  reason: string;
}

export type ViewModeSupport = Record<ExplorerViewMode, ViewModeState>;

export interface ViewModeInputs {
  /** The selected measure's published catalog row. */
  metric?: MetricSummary | null;
  /** The active source's capability-derived access declarations. */
  source?: ExplorerSource | null;
  /** The selected geography grain. */
  geoLevel?: string | null;
  /** Field names the discovered vector layer publishes, if discovery ran. */
  tileFields?: string[] | null;
  /** Rows currently loaded for the selection. */
  rowCount?: number;
}

const SUPPORTED: ViewModeState = { supported: true, reason: "" };

function unsupported(reason: string): ViewModeState {
  return { supported: false, reason };
}

/**
 * The geography grains the discovered vector layer can be drawn at.
 *
 * The boundary publishes one polygon layer whose features carry the
 * geography attribution fields; a feature with `county_fips` is a county and
 * one without it is a state, which is the filter the map already applies.
 * Nothing the boundary publishes identifies a national geometry, so a
 * national series has no spatial presentation here — it is not a map that
 * failed to colour.
 */
export function spatialGrains(tileFields: string[] | null | undefined): string[] {
  const names = new Set((tileFields || []).map((field) => String(field).toLowerCase()));
  const grains: string[] = [];
  if (names.has("state_fips")) {
    grains.push("STATE");
  }
  if (names.has("county_fips")) {
    grains.push("COUNTY");
  }
  return grains;
}

/** Whether the source declares a route that answers one geography's history. */
export function servesHistory(source: ExplorerSource | null | undefined): boolean {
  if (!source) {
    return false;
  }
  return source.accessShape === "neutral" || source.timeseriesParameters.length > 0;
}

/**
 * The support verdict for every mode, each with the published reason it
 * cannot answer. Callers render a mode only where `supported`, and show the
 * reason for the rest rather than letting a mode go silently missing.
 */
export function describeViewModes({
  metric = null,
  source = null,
  geoLevel = "",
  tileFields = null,
  rowCount = 0,
}: ViewModeInputs = {}): ViewModeSupport {
  const level = String(geoLevel || "").toUpperCase();
  const grains = spatialGrains(tileFields);

  let map: ViewModeState;
  if (grains.length === 0) {
    map = unsupported(
      "no vector tile layer with published geography fields was discovered",
    );
  } else if (!level) {
    map = unsupported("no geography grain is selected");
  } else if (!grains.includes(level)) {
    map = unsupported(
      `the tile boundary publishes no ${level.toLowerCase()} geometry, so this series is not spatial`,
    );
  } else {
    map = SUPPORTED;
  }

  const trend = servesHistory(source)
    ? SUPPORTED
    : unsupported(
        `${source?.title || "This source"} declares no route that answers one geography's history`,
      );

  const metadata = metric
    ? SUPPORTED
    : unsupported("no measure is selected, so there are no published semantics to show");

  // Quality is the measure's own published freshness and provenance. A
  // measure publishing neither gets no quality view rather than an empty one
  // that could read as "nothing is wrong".
  const publishesFreshness = metricQualityState(metric).state !== "idle";
  const publishesProvenance = metricProvenance(metric).length > 0;
  const quality =
    publishesFreshness || publishesProvenance
      ? SUPPORTED
      : unsupported("this measure publishes no freshness or provenance state");

  const hasRows = rowCount > 0;
  const table = hasRows
    ? SUPPORTED
    : unsupported("no observations are loaded for this selection");
  const exportMode = hasRows
    ? SUPPORTED
    : unsupported("no observations are loaded to export");

  return { map, trend, table, metadata, quality, export: exportMode };
}

/** The modes to present, in the catalog's declared order. */
export function supportedViewModes(support: ViewModeSupport): ExplorerViewMode[] {
  return EXPLORER_VIEW_MODES.filter((mode) => support[mode].supported);
}

/** The modes that are not presented, each with its published reason. */
export function unsupportedViewModes(
  support: ViewModeSupport,
): { mode: ExplorerViewMode; reason: string }[] {
  return EXPLORER_VIEW_MODES.filter((mode) => !support[mode].supported).map((mode) => ({
    mode,
    reason: support[mode].reason,
  }));
}
