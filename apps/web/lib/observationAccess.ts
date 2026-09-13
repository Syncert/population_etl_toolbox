// How the explorer reads observations for one capability-discovered source.
//
// Two declared access shapes (see ./explorerSources) are turned into request
// descriptions here, and the provider-neutral envelope is mapped onto the
// row shape the explorer view models read. Two rules bound everything in
// this module:
//
// 1. A filter reaches a request only when the source's capability entry
//    declares it. The neutral resource rejects an undeclared filter with a
//    422 precisely so it is never silently ignored, and a filter dropped
//    here would silently widen the answer instead.
// 2. Nothing is invented, aggregated, or collapsed. A stratified source
//    publishes several series per geography; this module reports that fact
//    so the caller can decline to map or chart them as one, rather than
//    keeping whichever row happened to arrive last.
//
// Scope is part of the same discipline. `scope=latest` is the source's own
// latest publication; `scope=as_released` reads every published release and
// is offered only where the capability entry declares `/observations/releases`
// and the neutral `scope` parameter. An unpinned as-released answer carries
// one series per release, which is reported as a stratification rather than
// collapsed to whichever release sorted last.

import type { QueryParams } from "./api/client";
import type { ExplorerSource } from "./explorerSources";
import { NEUTRAL_OBSERVATIONS_PATH, RELEASES_PATH } from "./explorerSources";
import type { ObservationRow } from "./explorerViewModel";

export interface ObservationRequest {
  resource: string;
  params: QueryParams;
}

/** `scope=latest` reads the source's own declared latest semantics. */
export const SCOPE_LATEST = "latest";
/**
 * `scope=as_released` reads every published release, each row carrying its
 * release identity. Pinning one with `release=` reproduces the analysis as
 * that release published it.
 */
export const SCOPE_AS_RELEASED = "as_released";

export type ObservationScope = typeof SCOPE_LATEST | typeof SCOPE_AS_RELEASED;

interface ScopedQuery {
  /** Defaults to `latest`; `as_released` only where the source declares it. */
  scope?: ObservationScope;
  /**
   * A release identity from `/observations/releases`. Sent only alongside
   * `scope=as_released` — the API answers `release` without it with a 422,
   * because "the latest publication, but an older one" is a contradiction.
   */
  release?: string;
}

export interface LatestObservationQuery extends ScopedQuery {
  metricCode: string;
  geoLevel?: string;
  stateFips?: string;
  limit?: string | number;
  /**
   * Ask the resource for one row per geography rather than the source's
   * whole latest publication. Sent only where the capability entry
   * declares the parameter, and never with an as-released read.
   */
  newestPerGeography?: boolean;
  /** Selected values for the source's own declared dimension filters. */
  dimensions?: Record<string, string>;
}

export interface HistoryObservationQuery extends ScopedQuery {
  metricCode: string;
  geoId: string;
  limit?: string | number;
  dimensions?: Record<string, string>;
}

export interface ReleaseListQuery {
  metricCode: string;
  limit?: string | number;
}

function declaredOnly(
  source: ExplorerSource,
  candidates: QueryParams,
  allowed: string[],
): QueryParams {
  const params: QueryParams = {};
  for (const [name, value] of Object.entries(candidates)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    if (allowed.includes(name)) {
      params[name] = value;
    }
  }
  return params;
}

function dimensionParams(
  names: string[],
  dimensions: Record<string, string> | undefined,
): QueryParams {
  const params: QueryParams = {};
  for (const name of names) {
    const value = dimensions?.[name];
    if (value) {
      params[name] = value;
    }
  }
  return params;
}

/**
 * Whether this source can answer an as-released question at all: the release
 * listing and the neutral `scope` parameter must both be declared for it.
 */
export function servesAsReleased(source: ExplorerSource | null | undefined): boolean {
  return Boolean(source?.servesReleases && source.supportsAsReleased);
}

/** The dimension controls that apply under one scope. */
export function scopedDimensionFilters(
  source: ExplorerSource | null | undefined,
  scope: ObservationScope,
): string[] {
  if (!source) {
    return [];
  }
  // An as-released read always goes through the neutral resource, so the
  // filters it accepts are the neutral ones even for a source-scoped source.
  return scope === SCOPE_AS_RELEASED && servesAsReleased(source)
    ? source.neutralDimensionFilters
    : source.dimensionFilters;
}

/**
 * `newest_per_geography`, where the caller asked for it and the resource
 * declares it.
 *
 * A source whose latest publication is a series -- Census PEP publishes
 * every estimated year of the current vintage -- answers several rows per
 * geography under `scope=latest`. That is the whole publication, and it is
 * the right default; a map needs one value per polygon. Asking the resource
 * for that is the same ranking its own distribution bins apply, so the
 * legend and the coloured polygons describe the same rows.
 *
 * It travels only with `scope=latest`: an as-released read is one series per
 * release, and reducing it per geography would show whichever release sorted
 * last as the value. The resource refuses that combination, and this client
 * does not send it.
 */
function newestPerGeographyParams(
  source: ExplorerSource,
  query: LatestObservationQuery,
): QueryParams {
  if (
    !query.newestPerGeography ||
    !source.supportsNewestPerGeography ||
    asReleased(source, query)
  ) {
    return {};
  }
  return { newest_per_geography: "true" };
}

/**
 * The `/observations/releases` request for one metric, or `null` when the
 * source's capability entry does not declare that route. A null answer is
 * the honest "this source publishes no release listing here"; nothing here
 * may guess a release identity.
 */
export function buildReleaseListRequest(
  source: ExplorerSource | null | undefined,
  query: ReleaseListQuery,
): ObservationRequest | null {
  if (!source?.servesReleases) {
    return null;
  }
  return {
    resource: RELEASES_PATH,
    params: { metric_code: query.metricCode, limit: query.limit },
  };
}

/**
 * The scope parameters a neutral request carries. `release` travels only
 * with `scope=as_released`, and only when the source declares that it can
 * be pinned.
 */
function scopeParams(source: ExplorerSource, query: ScopedQuery): QueryParams {
  if (query.scope !== SCOPE_AS_RELEASED || !servesAsReleased(source)) {
    return { scope: SCOPE_LATEST };
  }
  return {
    scope: SCOPE_AS_RELEASED,
    release: source.supportsReleasePin ? query.release : undefined,
  };
}

/** True when this query asks the neutral resource for an as-released read. */
function asReleased(source: ExplorerSource, query: ScopedQuery): boolean {
  return query.scope === SCOPE_AS_RELEASED && servesAsReleased(source);
}

/** The row field carrying a row's own release identity. */
export const RELEASE_DIMENSION = "release";

/**
 * The axes along which a loaded answer can carry more than one series per
 * geography. Under `scope=as_released` the release is one of them: every
 * published release answers, so a choropleth join or a single-line chart
 * would keep whichever release arrived last unless one is pinned. Adding it
 * here lets the caller decline for the same reason, and name the release
 * control as the filter that resolves it.
 */
export function stratificationDimensions(
  dimensionFilters: string[] | null | undefined,
  scope: ObservationScope,
): string[] {
  const names = dimensionFilters || [];
  return scope === SCOPE_AS_RELEASED ? [...names, RELEASE_DIMENSION] : [...names];
}

/**
 * The dimension narrowing a built request actually carried.
 *
 * Read from the request rather than from the selection it was built out of,
 * for the reason the reduction and the state are: `dimensionParams` sends
 * only the names the capability declares under this scope, and a
 * source-scoped read sends none at all, so a selection can outlive the
 * request that would have carried it -- a stratum chosen under
 * `scope=as_released` and still set after the reader returns to the latest
 * publication, where the source declares no such filter.
 *
 * What a saved view has to record is what it asked for. A document carrying
 * a filter the request never sent replays a narrower set than the view
 * showed, and one missing a filter the request did send replays a wider one:
 * a stratified measure read for one stratum reopening as every stratum the
 * source publishes is a different population (WEB-081).
 */
export function dimensionsCarriedBy(
  request: ObservationRequest | null | undefined,
  dimensionFilters: string[] | null | undefined,
): Record<string, string> {
  const carried: Record<string, string> = {};
  for (const name of dimensionFilters || []) {
    const value = request?.params?.[name];
    if (typeof value === "string" && value) {
      carried[name] = value;
    }
  }
  return carried;
}

/**
 * The cross-geography "latest published values" request for one metric.
 *
 * The source-scoped shape keeps its own route and parameter discipline; the
 * neutral shape asks `/observations` with `scope=latest` and only the
 * filters the capability declares.
 */
export function buildLatestObservationRequest(
  source: ExplorerSource,
  query: LatestObservationQuery,
): ObservationRequest {
  const shared: QueryParams = {
    geo_level: query.geoLevel,
    state_fips: query.stateFips,
  };
  const released = asReleased(source, query);

  if (source.accessShape === "source-scoped" && !released) {
    return {
      resource: `/${source.segment}/observations/latest`,
      params: {
        metric_code: query.metricCode,
        limit: query.limit,
        ...declaredOnly(source, shared, source.latestParameters),
      },
    };
  }

  // Both the neutral shape and every as-released read answer here; the
  // filters a source-scoped source may carry across are its declared
  // neutral ones, not the parameters of the route it left behind.
  const allowed = released ? source.neutralFilters : source.requestFilters;
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      ...scopeParams(source, query),
      limit: query.limit,
      ...newestPerGeographyParams(source, query),
      ...declaredOnly(source, shared, allowed),
      ...dimensionParams(scopedDimensionFilters(source, query.scope || SCOPE_LATEST), query.dimensions),
    },
  };
}

/**
 * The single-geography history request.
 *
 * The neutral resource has no separate timeseries route: `scope=latest` over
 * one `geo_id` is the source's currently published series for that
 * geography, which is what the panel labels it.
 */
export function buildHistoryObservationRequest(
  source: ExplorerSource,
  query: HistoryObservationQuery,
): ObservationRequest {
  const released = asReleased(source, query);

  if (source.accessShape === "source-scoped" && !released) {
    return {
      resource: `/${source.segment}/observations/timeseries`,
      params: {
        metric_code: query.metricCode,
        geo_id: query.geoId,
        limit: query.limit,
      },
    };
  }

  const allowed = released ? source.neutralFilters : source.requestFilters;
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      ...scopeParams(source, query),
      limit: query.limit,
      ...declaredOnly(source, { geo_id: query.geoId }, allowed),
      ...dimensionParams(scopedDimensionFilters(source, query.scope || SCOPE_LATEST), query.dimensions),
    },
  };
}

/**
 * The bound a newest-value read falls back to when the resource cannot
 * reduce for us. Large enough to hold any one geography's currently
 * published series, and reported rather than assumed: the caller is told
 * whether the resource did the reducing.
 */
export const NEWEST_VALUE_FALLBACK_LIMIT = "1000";

export interface NewestValueRequest extends ObservationRequest {
  /**
   * True when the resource itself reduced the answer to the geography's
   * newest published period, so the single row it returns is that value.
   * False when this is a bounded page the caller must reduce, and may not
   * have received whole.
   */
  reducedByResource: boolean;
}

/**
 * One geography's newest published value for one measure.
 *
 * A card that shows a place's population wants a number, not a series. Every
 * observation order this API serves is ascending, so taking the last row of
 * a bounded page is the newest value only when the whole publication fitted
 * inside the page. Census PEP's latest publication is every estimated year
 * of the current vintage -- about 54 rows per county for `POPESTIMATE` --
 * so a 50-row page ended around 2020 and the card showed a four-year-old
 * estimate as the place's population.
 *
 * `newest_per_geography=true` is the resource's own answer to this question
 * (API-066): it ranks inside the source's own relation, which is the only
 * place that knows how its periods order, and it is the same ranking the
 * explorer's map and the distribution bins apply. Where a capability entry
 * declares it, this asks for exactly one row. Where it does not, the read
 * stays a bounded page and says so, because inventing the reduction here
 * would be this client asserting an order the API did not publish.
 */
export function buildNewestValueRequest(
  source: ExplorerSource,
  query: { metricCode: string; geoId: string },
): NewestValueRequest {
  const reducedByResource = Boolean(source.supportsNewestPerGeography);
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      scope: SCOPE_LATEST,
      limit: reducedByResource ? "1" : NEWEST_VALUE_FALLBACK_LIMIT,
      ...(reducedByResource ? { newest_per_geography: "true" } : {}),
      ...declaredOnly(source, { geo_id: query.geoId }, source.neutralFilters),
    },
    reducedByResource,
  };
}


/**
 * The history panel's status line, honest about the page bound.
 *
 * A bounded read that is presented as a whole answer is the defect this
 * exists to prevent: a geography whose publication is longer than the pages
 * the client will fetch charts a prefix, and "N historical observations"
 * reads as the history rather than as the part of it that arrived. The map
 * panel has said this since it was written; the trend did not (WEB-036).
 */
export function describeHistoryLoad(
  loaded: number,
  total: number | null,
  complete: boolean,
  acrossReleases: boolean,
): string {
  const context = acrossReleases ? " across published releases" : "";
  if (!complete && total !== null) {
    return (
      `loaded ${loaded} of ${total} historical observations${context}; ` +
      "the page bound cut the answer short, so the trend is incomplete"
    );
  }
  return `${loaded} historical observation${loaded === 1 ? "" : "s"}${context}`;
}


/**
 * One geography's settled history: each period as its newest release left it.
 *
 * A source whose latest relation keeps one row per geography -- Census ACS
 * holds only the newest vintage -- has no history under `scope=latest`, so a
 * geography's trend is every release that published it, reduced to the
 * newest release of each period. This client used to do that reduction
 * itself, and to do it had to decide which release identity is newer from
 * its spelling -- a rule the warehouse publishes and every dispatch entry
 * declares, which a guess can contradict (`2023.10` and `2023.9` order one
 * way as numbers and the other as text).
 *
 * API-081 serves the reduction, ranked by the source's own declared release
 * order. `null` where the capability entry does not declare the parameter,
 * so a deployment on an older API keeps the client-side fallback rather than
 * losing its trend, and so this client never sends something undeclared.
 * A pinned release is never carried: the resource refuses that pair, because
 * one pins a single release and the other asks for the newest of every
 * period.
 */
export function buildSettledHistoryRequest(
  source: ExplorerSource | null | undefined,
  query: HistoryObservationQuery,
): ObservationRequest | null {
  // The route's own declarations, not the release listing: pinning a release
  // needs `/observations/releases` to discover an identity, and a settled
  // history pins nothing.
  if (!source || !source.supportsSettledHistory || !source.supportsAsReleased) {
    return null;
  }
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      scope: SCOPE_AS_RELEASED,
      newest_release_per_period: "true",
      limit: query.limit,
      ...declaredOnly(source, { geo_id: query.geoId }, source.neutralFilters),
      ...dimensionParams(
        scopedDimensionFilters(source, SCOPE_AS_RELEASED),
        query.dimensions,
      ),
    },
  };
}


function firstText(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === "string" && value !== "") {
      return value;
    }
  }
  return null;
}

/**
 * The period a neutral row covers, spelled exactly as published: a single
 * date when the bounds agree, an explicit range when they do not. CDC
 * publishes multi-year periods, and rendering only one bound would state a
 * narrower period than the source did.
 */
export function observationPeriodLabel(row: ObservationRow | null | undefined): string {
  const start = firstText(row?.period_start);
  const end = firstText(row?.period_end);
  if (start && end) {
    return start === end ? start : `${start} – ${end}`;
  }
  return (
    firstText(start, end, row?.period, row?.observation_date) || ""
  );
}

/**
 * Map the provider-neutral envelope onto the row shape the explorer view
 * models read, without overwriting anything the row already published and
 * without touching `value` (text, or `null` when nothing was published).
 * Source-scoped rows already carry this shape and pass through unchanged.
 */
export function normalizeObservationRows(
  source: ExplorerSource | null | undefined,
  items: ObservationRow[] | null | undefined,
): ObservationRow[] {
  const rows = Array.isArray(items) ? items : [];
  if (!source || source.accessShape !== "neutral") {
    return rows;
  }

  return rows.map((row) => {
    const uncertainty = (row.uncertainty || {}) as Record<string, unknown>;
    const period = observationPeriodLabel(row);
    const normalized: ObservationRow = { ...row };

    if (normalized.period === undefined && period) {
      normalized.period = period;
    }
    // The chart and table read a single ordering date; the published end of
    // the period is the point the observation is current as of.
    if (normalized.observation_date === undefined) {
      normalized.observation_date = firstText(row.period_end, row.period_start);
    }
    if (normalized.units === undefined && row.unit !== undefined) {
      normalized.units = row.unit;
    }
    if (normalized.source === undefined && row.source_code !== undefined) {
      normalized.source = row.source_code;
    }
    if (normalized.margin_of_error === undefined && uncertainty.margin_of_error !== undefined) {
      normalized.margin_of_error = uncertainty.margin_of_error;
    }
    if (
      normalized.margin_of_error_pct === undefined &&
      uncertainty.margin_of_error_pct !== undefined
    ) {
      normalized.margin_of_error_pct = uncertainty.margin_of_error_pct;
    }
    return normalized;
  });
}

function compareReleases(left: unknown, right: unknown): number {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
    return leftNumber - rightNumber;
  }
  return String(left ?? "").localeCompare(String(right ?? ""));
}

/**
 * One row per period from an unpinned as-released read, in period order:
 * the newest release of each period. A source whose latest relation keeps
 * one row per geography (ACS holds only the newest vintage) has a
 * geography's history only across its releases, and a period revised in a
 * later release shows as last published. Release identities compare as
 * numbers where both sides are numeric (a vintage year, a watermark) and as
 * text otherwise (an as-of date).
 */
export function collapseToNewestRelease(rows: ObservationRow[]): ObservationRow[] {
  const newestByPeriod = new Map<string, ObservationRow>();
  for (const row of rows) {
    const period = observationOrderingDate(row);
    if (!period) {
      continue;
    }
    const current = newestByPeriod.get(period);
    if (!current || compareReleases(row.release, current.release) > 0) {
      newestByPeriod.set(period, row);
    }
  }
  return [...newestByPeriod.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, row]) => row);
}

/** The date a row is ordered by: the published period end, else its start. */
function observationOrderingDate(row: ObservationRow): string | null {
  return firstText(row.observation_date, row.period_end, row.period_start);
}

/** How many distinct periods a set of rows spans. */
/**
 * The one period every row describes, or `""` where they differ.
 *
 * What a view can honestly say its figures are *for*. A publication that
 * spans several periods -- Census PEP's latest vintage carries every
 * estimated year -- has no single period, and naming one of them would make
 * the others read as that period's values (WEB-069).
 */
export function sharedObservationPeriod(
  rows: ObservationRow[] | null | undefined,
): string {
  const periods = new Set<string>();
  for (const row of rows || []) {
    const period = observationPeriodLabel(row);
    if (period) {
      periods.add(period);
    }
  }
  return periods.size === 1 ? [...periods][0]! : "";
}

export function countObservationPeriods(rows: ObservationRow[] | null | undefined): number {
  const periods = new Set<string>();
  for (const row of rows || []) {
    const period = observationOrderingDate(row);
    if (period) {
      periods.add(period);
    }
  }
  return periods.size;
}

/**
 * One row per geography: the newest period each geography publishes, in
 * first-seen geography order. A source whose latest publication is a
 * series (Census PEP publishes every estimated year of the current vintage)
 * answers several rows per geography under `scope=latest`; a map colours
 * one value per polygon, and the API's own distribution and comparison
 * routes rank the same way, so the map and its legend count the same rows.
 * A row without a geography or a period is left out. Rows carrying the same
 * period for one geography keep the first published, so nothing here
 * chooses between them.
 */
export function newestPerGeography(rows: ObservationRow[] | null | undefined): ObservationRow[] {
  const newestByGeo = new Map<string, ObservationRow>();
  for (const row of rows || []) {
    const geoId = firstText(row.geo_id);
    const period = observationOrderingDate(row);
    if (!geoId || !period) {
      continue;
    }
    const current = newestByGeo.get(geoId);
    const currentPeriod = current ? observationOrderingDate(current) : null;
    if (!current || (currentPeriod !== null && period.localeCompare(currentPeriod) > 0)) {
      newestByGeo.set(geoId, row);
    }
  }
  return [...newestByGeo.values()];
}

/**
 * The published coverage fields, in the order the envelope declares them.
 *
 * Not a client-authored list of "the interesting ones": these are the six the
 * neutral envelope's `ObservationCoverage` publishes, and an export that
 * carried a subset would be this client deciding which part of a source's
 * participation basis a reader may have.
 */
export const OBSERVATION_COVERAGE_FIELDS = [
  "participation_status",
  "coverage_percent",
  "coverage_basis",
  "population",
  "participated_population",
  "population_denominator",
] as const;

/** One published coverage field on a row, or `""` when the source published none. */
export function observationCoverageValue(
  row: ObservationRow | null | undefined,
  field: string,
): string {
  const coverage = (row?.coverage || {}) as Record<string, unknown>;
  const value = coverage[field];
  return value === undefined || value === null ? "" : String(value);
}

/**
 * True when any loaded row published a participation status.
 *
 * Read from the answer rather than from a list of sources: a source that
 * begins publishing coverage is shown it without an edit here, and one that
 * does not grows no empty column.
 */
export function publishesCoverage(rows: ObservationRow[] | null | undefined): boolean {
  return (rows || []).some((row) => observationCoverageValue(row, "participation_status") !== "");
}

/**
 * The published uncertainty fields, in the order the envelope declares them.
 *
 * The seven `ObservationUncertainty` publishes, across the three sources that
 * publish any: Census ACS's margin of error and its percentage, CDC's
 * confidence bounds, and USDA NASS's coefficient of variation with the status
 * and symbol that qualify it. Not a client-authored shortlist -- an export
 * carrying a subset would be this client deciding which part of a source's
 * own statement of precision a reader may have (WEB-053).
 */
export const OBSERVATION_UNCERTAINTY_FIELDS = [
  "margin_of_error",
  "margin_of_error_pct",
  "confidence_lower",
  "confidence_upper",
  "cv_value",
  "cv_status",
  "cv_symbol",
] as const;

/**
 * One published uncertainty field on a row, or `""` when the source published
 * none.
 *
 * The nested envelope first, then the row itself: the source-scoped shapes
 * carry `margin_of_error` at the top level and no `uncertainty` object at
 * all, and normalization lifts those same two names for the chart.
 */
export function observationUncertaintyValue(
  row: ObservationRow | null | undefined,
  field: string,
): string {
  const uncertainty = (row?.uncertainty || {}) as Record<string, unknown>;
  const value = uncertainty[field] !== undefined ? uncertainty[field] : row?.[field];
  return value === undefined || value === null ? "" : String(value);
}

/**
 * The uncertainty fields a surface presenting the margin itself still needs.
 *
 * The profile product decodes the margin with `marginOfErrorText`, which
 * knows the Census sentinel margins (`-555555555` is a controlled estimate,
 * not a negative interval) that a field-value join cannot. It needs
 * everything else the row published beside that, and it must not get a
 * second list: derived here, so a field added to
 * `OBSERVATION_UNCERTAINTY_FIELDS` reaches every surface that reads it
 * (WEB-060).
 */
export const OBSERVATION_UNCERTAINTY_BEYOND_MARGIN: readonly string[] =
  OBSERVATION_UNCERTAINTY_FIELDS.filter((field) => !field.startsWith("margin_of_error"));

/**
 * True when any loaded row published any uncertainty field.
 *
 * Read from the answer rather than from a list of sources, exactly as
 * `publishesCoverage` is: a source that begins publishing an interval is
 * shown it without an edit here, and one that publishes none grows no empty
 * column.
 */
export function publishesUncertainty(rows: ObservationRow[] | null | undefined): boolean {
  return (rows || []).some((row) =>
    OBSERVATION_UNCERTAINTY_FIELDS.some(
      (field) => observationUncertaintyValue(row, field) !== "",
    ),
  );
}

/**
 * A row's published uncertainty, as `field value` pairs for the fields it
 * published.
 *
 * Named rather than composed: a margin, a confidence interval and a
 * coefficient of variation are not interchangeable, and rendering them into
 * one notation -- `12.1 – 13.9`, `± 1.5` -- would be this client deciding
 * what three sources' numbers mean.
 */
export function observationUncertaintyLabel(
  row: ObservationRow | null | undefined,
  fields: readonly string[] = OBSERVATION_UNCERTAINTY_FIELDS,
): string {
  return fields.map((field) => [field, observationUncertaintyValue(row, field)])
    .filter(([, value]) => value !== "")
    .map(([field, value]) => `${String(field).replaceAll("_", " ")} ${value}`)
    .join(" · ");
}

/**
 * The declared dimensions a row published, as `name value` pairs.
 *
 * The presentation this repository already chose for the seven uncertainty
 * fields rather than seven columns, applied to the declared dimensions a
 * table does not give a column of its own: joined `"name value"` pairs, each
 * under the source's own published name, with a field the row did not
 * publish left out rather than shown empty (WEB-061).
 */
export function observationDimensionLabel(
  row: ObservationRow | null | undefined,
  names: readonly string[],
): string {
  return names
    .map((name) => [name, observationDimensionValue(row, name)])
    .filter(([, value]) => value !== "")
    .map(([name, value]) => `${String(name).replaceAll("_", " ")} ${value}`)
    .join(" · ");
}


/** A declared dimension's published value on one row, or `""` when absent. */
export function observationDimensionValue(
  row: ObservationRow | null | undefined,
  name: string,
): string {
  const dimensions = (row?.dimensions || {}) as Record<string, unknown>;
  const value = dimensions[name] !== undefined ? dimensions[name] : row?.[name];
  return value === undefined || value === null ? "" : String(value);
}

/**
 * The distinct published values of one declared dimension across the loaded
 * rows, sorted for deterministic rendering. These are provider-published
 * values read back from the answer, never a client-authored option list.
 */
export function observationDimensionOptions(
  rows: ObservationRow[] | null | undefined,
  name: string,
): string[] {
  const values = new Set<string>();
  for (const row of rows || []) {
    const value = observationDimensionValue(row, name);
    if (value) {
      values.add(value);
    }
  }
  return [...values].sort();
}

export interface ObservationStratification {
  /** Distinct declared-dimension signatures present in the loaded rows. */
  seriesCount: number;
  /** True when a geography carries more than one series in this answer. */
  stratified: boolean;
  /** The declared dimensions that actually vary, so the caller can name them. */
  varyingDimensions: string[];
}

/**
 * Whether the loaded rows resolve to one value per geography.
 *
 * A stratified source (CDC strata and adjustment statuses, FBI UCR subject
 * types, USDA NASS domains) returns several rows per geography, and both the
 * choropleth join and a single-line chart would keep whichever arrived last.
 * The caller uses this to decline rather than collapse, and to name the
 * declared filters that would narrow the selection.
 */
export function describeStratification(
  rows: ObservationRow[] | null | undefined,
  dimensionFilters: string[] | null | undefined,
): ObservationStratification {
  const names = (dimensionFilters || []).filter(Boolean);
  const items = Array.isArray(rows) ? rows : [];
  if (names.length === 0 || items.length === 0) {
    return { seriesCount: items.length === 0 ? 0 : 1, stratified: false, varyingDimensions: [] };
  }

  const signatures = new Set<string>();
  const valuesByName = new Map<string, Set<string>>(names.map((name) => [name, new Set()]));
  for (const row of items) {
    const signature: string[] = [];
    for (const name of names) {
      const value = observationDimensionValue(row, name);
      signature.push(`${name}=${value}`);
      valuesByName.get(name)!.add(value);
    }
    signatures.add(signature.join("|"));
  }

  return {
    seriesCount: signatures.size,
    stratified: signatures.size > 1,
    varyingDimensions: names.filter((name) => (valuesByName.get(name)?.size || 0) > 1),
  };
}

/**
 * What a selected state did and did not narrow (WEB-075).
 *
 * A state narrows three things on this screen: the map (a selected state is
 * the whole map), the geography picker (the county and place lists are
 * "select a state first" until one is chosen), and — only where the source
 * declares `state_fips` as an observation filter — the rows themselves.
 *
 * Census PEP is the source where those come apart. The relation the neutral
 * route reads (`gold_pep.population_estimate_latest`) carries `geo_id` and
 * `geo_type` and no fips columns, so the capability entry correctly declares
 * no `state_fips`, and `buildLatestObservationRequest` correctly drops it.
 * The state control was disabled there at every grain, so the county and
 * place pickers said "select a state first" and could never be given one: a
 * PEP county's history was reachable only by clicking the map, and places —
 * the grain PEP alone publishes — not at all.
 *
 * The control is now usable wherever the picker and the map need it, which
 * makes this sentence necessary: a reader looking at one state's map and a
 * nation's rows has to be told which is which.
 */
export function stateScopeNote({
  stateSelected,
  narrowsRows,
  sourceTitle,
}: {
  stateSelected: boolean;
  narrowsRows: boolean;
  sourceTitle?: string | null;
}): string {
  if (!stateSelected || narrowsRows) {
    return "";
  }
  const source = (sourceTitle || "").trim() || "This source";
  return (
    `${source} declares no state filter for its observations, so these rows ` +
    "are national: the selected state narrows the map and the geography list " +
    "only."
  );
}

/**
 * Every geography picker asks the catalog for active geographies only.
 *
 * A geography a new boundary vintage stops listing is published as
 * `geography_state: "retired"` rather than dropped (DB-038), because the
 * served relations still hold its observations and a catalog that hid it
 * would leave rows nothing could name. That is the right answer for a client
 * resolving a served row; it is the wrong default for a picker, whose whole
 * question is "which geography do I want to look at now". Naming the choice
 * once here keeps the six pickers from drifting apart on it.
 */
export const ACTIVE_GEOGRAPHIES_ONLY = { active_only: "true" } as const;
