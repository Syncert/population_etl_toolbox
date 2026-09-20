// The workbench's own rules: what a series is, which series may join one
// chart, which axis each one is drawn against, and which presentations the
// current selection can actually answer.
//
// Everything here is a pure function over published evidence — a capability
// entry, a catalog row, the rows the API returned — so the page renders a
// decision this module made rather than making decisions while it renders.
// That is the same split `lib/viewModes.ts` makes for the explorer, and it
// exists for the same reason: "a presentation is offered only where it can
// answer" is a rule a reader must be able to read, and a rule spread across
// JSX is a rule nobody can read.
//
// Three invariants this module exists to hold:
//
// 1. **Nothing is rolled up.** No function here derives a coarser grain from
//    finer rows, sums across geographies, or averages anything. The
//    compatibility policy says it in the API's own words — "derived values
//    must not be summed across geographies" — and a county-only measure
//    asked for at STATE is answered "not published at STATE", never
//    synthesised.
// 2. **Nothing is normalised to share an axis.** Units decide axes. A series
//    is never indexed to a base period or rescaled so two measures fit one
//    scale, because the resulting line is not a published measure.
// 3. **A stratified source joins only with its dimensions pinned.** An
//    unpinned answer is N series wearing one name; the explorer reports that
//    rather than keeping whichever row arrived last (WEB-014), and the
//    workbench refuses to add the series at all.

import type { ExplorerSource } from "./explorerSources";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { GEO_GRAIN_LABELS } from "./geographyPicker";
import { observationPeriodLabel } from "./observationAccess";
import type { ObservationScope } from "./observationAccess";
import { formatNumber } from "./format";

/**
 * The most series one composition carries.
 *
 * Eight because that is the point past which a legend stops being readable
 * and a shared link stops being a link, and because the matrix route the
 * cross-sectional presentations read (WB-4) is bounded at the same number.
 * One ceiling, stated once, so the two halves of the screen cannot disagree
 * about what a composition is.
 */
export const MAX_WORKBENCH_SERIES = 8;

/**
 * The most value axes one longitudinal chart carries.
 *
 * Two, because a chart has a left edge and a right edge. A third unit is not
 * squeezed onto one of them — that is the chart lying about comparability —
 * it turns the presentation into small multiples, which the page says on
 * screen rather than silently rendering.
 */
export const MAX_VALUE_AXES = 2;

/** What a series' axis is labelled when its measure publishes no unit. */
export const UNPUBLISHED_UNIT_LABEL = "unit not published";

/** One measure, at one geography, as the workbench reads it. */
export interface WorkbenchSeries {
  /** The explorer source key, so the series can be re-resolved on reopen. */
  sourceKey: string;
  sourceCode: string;
  metricCode: string;
  scope: ObservationScope;
  /** A pinned release identity, where the source declares one. */
  release?: string;
  /** The grain this series' geography is at — never a grain to roll up to. */
  geoLevel: string;
  geoId: string;
  /** The source's own declared dimension filters, each pinned to one value. */
  filters: Record<string, string>;
}

/**
 * A series' stable identity.
 *
 * Every field that changes which published rows the series is, and nothing
 * else: two series differing only in the order their filters were chosen are
 * one series. Used as a React key, as the dedupe key when a series is added,
 * and as the join key between a plotted line and its legend entry.
 */
export function seriesKey(series: WorkbenchSeries): string {
  const filters = Object.keys(series.filters || {})
    .sort()
    .map((name) => `${name}=${series.filters[name]}`)
    .join("&");
  return [
    series.sourceKey,
    series.metricCode,
    series.scope,
    series.release || "",
    series.geoLevel,
    series.geoId,
    filters,
  ].join("|");
}

export function sameSeries(
  left: WorkbenchSeries,
  right: WorkbenchSeries,
): boolean {
  return seriesKey(left) === seriesKey(right);
}

/**
 * The source's declared dimension filters this series has not pinned.
 *
 * Read from the capability entry's own declaration, never from a list here,
 * so a dimension this application has never heard of still refuses the
 * series rather than being dropped. A filter pinned to an empty string is
 * unpinned: "every stratum" is what an absent filter already means.
 */
export function unpinnedDimensions(
  source: ExplorerSource | null | undefined,
  filters: Record<string, string> | null | undefined,
): string[] {
  const declared = source
    ? source.accessShape === "neutral"
      ? source.neutralDimensionFilters
      : source.dimensionFilters
    : [];
  const pinned = filters || {};
  return [...declared]
    .filter((name) => !pinned[name])
    .sort();
}

/** Whether a series may join the composition, and why not when it may not. */
export interface SeriesAdmission {
  admitted: boolean;
  /** One sentence, in the publisher's terms. "" when admitted. */
  reason: string;
}

/**
 * Whether one more series may join, decided before any request is built.
 *
 * The refusals are ordered so the reason a reader is given is the one they
 * can act on: a duplicate first (nothing to fix but the selection), then the
 * ceiling (remove one), then the geography (choose one), then the unpinned
 * dimensions (pin them). Refusing here rather than at request time is what
 * keeps the "N series left unplotted" state off the screen entirely for a
 * stratified source — the explorer reports that state because it is reading
 * one measure and must show what it got; the workbench is composing, and a
 * composition can decline to include an ambiguous member.
 */
export function admitSeries({
  source,
  candidate,
  existing,
}: {
  source: ExplorerSource | null | undefined;
  candidate: WorkbenchSeries;
  existing: readonly WorkbenchSeries[];
}): SeriesAdmission {
  if ((existing || []).some((series) => sameSeries(series, candidate))) {
    return {
      admitted: false,
      reason: "This measure is already on the chart at this geography.",
    };
  }
  if ((existing || []).length >= MAX_WORKBENCH_SERIES) {
    return {
      admitted: false,
      reason:
        `A composition carries at most ${MAX_WORKBENCH_SERIES} series. ` +
        "Remove one before adding another.",
    };
  }
  if (!candidate.geoId) {
    return {
      admitted: false,
      reason:
        "Choose the geography this series is read at. A series is one " +
        "measure at one geography; nothing is rolled up from a finer grain.",
    };
  }
  const unpinned = unpinnedDimensions(source, candidate.filters);
  if (unpinned.length > 0) {
    const names = unpinned.join(", ");
    return {
      admitted: false,
      reason:
        `${source?.title || candidate.sourceCode} publishes several series ` +
        `per geography, separated by ${names}. Pin ` +
        `${unpinned.length === 1 ? "it" : "each of them"} to one value: an ` +
        "unpinned answer is many published lines, and drawing it as one " +
        "would combine measures the source publishes separately.",
    };
  }
  return { admitted: true, reason: "" };
}

// ---------------------------------------------------------------------------
// Units decide axes
// ---------------------------------------------------------------------------

/** One value axis: its unit, which edge it is drawn on, and what is on it. */
export interface ValueAxis {
  unit: string;
  side: "left" | "right";
  seriesKeys: string[];
  /** True when the unit is the measure's absence of one, not a published one. */
  unpublished: boolean;
}

export interface AxisAssignment {
  axes: ValueAxis[];
  /**
   * True when the selection publishes more than two distinct units, so one
   * chart cannot carry them honestly and the page draws small multiples.
   */
  smallMultiples: boolean;
  /** What the reader is told about the assignment. "" when nothing is. */
  note: string;
}

/**
 * Which axis each series is drawn against, from the units alone.
 *
 * The units are compared case-insensitively but reported as published: two
 * sources spelling "Percent" and "percent" publish the same unit, and
 * drawing them on two axes would present one quantity as two. A measure
 * publishing no unit gets its own axis labelled as such rather than being
 * assumed to share one — Census ACS publishes no units at all, and assuming
 * is exactly what the compatibility policy's `unknown` verdict refuses to do.
 *
 * Nothing is ever normalised, indexed or rescaled to make a third unit fit.
 */
export function assignValueAxes(
  entries: readonly { key: string; unit: string | null | undefined }[],
): AxisAssignment {
  const order: string[] = [];
  const byFold = new Map<string, { unit: string; keys: string[] }>();
  for (const entry of entries || []) {
    const published = typeof entry.unit === "string" ? entry.unit.trim() : "";
    const fold = published ? published.toLocaleLowerCase() : "";
    if (!byFold.has(fold)) {
      byFold.set(fold, { unit: published, keys: [] });
      order.push(fold);
    }
    byFold.get(fold)!.keys.push(entry.key);
  }

  const axes: ValueAxis[] = order.map((fold, index) => {
    const group = byFold.get(fold)!;
    return {
      unit: group.unit || UNPUBLISHED_UNIT_LABEL,
      side: index === 0 ? "left" : "right",
      seriesKeys: [...group.keys],
      unpublished: group.unit === "",
    };
  });

  if (axes.length > MAX_VALUE_AXES) {
    return {
      axes,
      smallMultiples: true,
      note:
        `These series publish ${axes.length} different units ` +
        `(${axes.map((axis) => axis.unit).join(", ")}). A chart has two ` +
        "edges, so they are drawn as small multiples — one panel per unit — " +
        "rather than rescaled onto a shared axis. Nothing here is indexed or " +
        "normalised.",
    };
  }

  const note =
    axes.length === MAX_VALUE_AXES
      ? `Two units are published (${axes
          .map((axis) => `${axis.unit} on the ${axis.side}`)
          .join(", ")}). The two axes have their own scales; the lines' ` +
        "relative heights carry no meaning."
      : "";

  return { axes, smallMultiples: false, note };
}

// ---------------------------------------------------------------------------
// Which presentations the selection can answer
// ---------------------------------------------------------------------------

/** Presentations with time on the horizontal axis, one geography per series. */
export const LONGITUDINAL_PRESENTATIONS = ["line", "bar"] as const;

/** Presentations reading one newest value per geography at one shared grain. */
export const CROSS_SECTIONAL_PRESENTATIONS = [
  "scatter",
  "ranking",
  "correlation",
] as const;

/** One measure laid out as geographies by periods. */
export const MATRIX_PRESENTATIONS = ["heatmap"] as const;

export const WORKBENCH_PRESENTATIONS = [
  ...LONGITUDINAL_PRESENTATIONS,
  ...CROSS_SECTIONAL_PRESENTATIONS,
  ...MATRIX_PRESENTATIONS,
] as const;

export type WorkbenchPresentation = (typeof WORKBENCH_PRESENTATIONS)[number];

export function isLongitudinal(
  presentation: string | null | undefined,
): presentation is (typeof LONGITUDINAL_PRESENTATIONS)[number] {
  return (LONGITUDINAL_PRESENTATIONS as readonly string[]).includes(
    String(presentation),
  );
}

export function isCrossSectional(
  presentation: string | null | undefined,
): presentation is (typeof CROSS_SECTIONAL_PRESENTATIONS)[number] {
  return (CROSS_SECTIONAL_PRESENTATIONS as readonly string[]).includes(
    String(presentation),
  );
}

export const PRESENTATION_LABELS: Record<WorkbenchPresentation, string> = {
  line: "Line",
  bar: "Bar over time",
  scatter: "Scatter",
  ranking: "Bar ranking",
  correlation: "Correlation",
  heatmap: "Geography × period heatmap",
};

export interface PresentationState {
  available: boolean;
  /** Why it is not offered, in the publisher's or the API's terms. */
  reason: string;
}

export type PresentationOffer = Record<
  WorkbenchPresentation,
  PresentationState
>;

/** What one selected series contributes to the presentation decision. */
export interface PresentationSeriesFacts {
  key: string;
  metricCode: string;
  /** Distinct periods this series published, after the unpublished are gone. */
  periodCount: number;
}

/**
 * Which presentations the current selection can answer, and why not.
 *
 * Every unavailable presentation carries a reason rather than disappearing,
 * which is `describeViewModes`' rule: a control that vanishes teaches a
 * reader that the screen is unreliable, and a control that is present and
 * explained teaches them what the publication does.
 *
 * `crossSectionalReason` is passed in rather than computed here because what
 * decides it is the API's verdict on the selection — a preflight, a matrix
 * refusal — which this module does not fetch. Passing `""` means the caller
 * has established the cross-sectional presentations can be served.
 */
export function presentationOffer({
  series,
  facts,
  crossSectionalReason = "",
  correlationReason = "",
  heatmapReason = "",
}: {
  series: readonly WorkbenchSeries[];
  facts: readonly PresentationSeriesFacts[];
  crossSectionalReason?: string;
  /**
   * The correlation's own reason, separate since WB-5: a scatter and a
   * ranking read `/comparison` for one chosen pair, while a correlation reads
   * `/comparison/matrix` for up to eight measures at once. One reason for
   * both would have to be the stricter of the two, which would withhold a
   * correlation the API would serve.
   */
  correlationReason?: string;
  heatmapReason?: string;
}): PresentationOffer {
  const count = (series || []).length;
  const plotted = (facts || []).filter((fact) => fact.periodCount > 0);
  const empty = count === 0;

  const withoutAnyPeriod = (facts || []).filter(
    (fact) => fact.periodCount === 0,
  );
  const withoutTwoPeriods = (facts || []).filter(
    (fact) => fact.periodCount < 2,
  );

  const noSeries = "Add a measure to draw anything.";

  const line: PresentationState = empty
    ? { available: false, reason: noSeries }
    : plotted.length === 0
      ? {
          available: false,
          reason:
            "None of the selected series published a value, so there is " +
            "nothing to draw. An unpublished period is not a value of zero.",
        }
      : withoutTwoPeriods.length > 0
        ? {
            available: false,
            reason:
              `A line needs at least two published periods per series; ` +
              `${withoutTwoPeriods
                .map((fact) => fact.metricCode)
                .join(", ")} published fewer. A bar will draw what there is.`,
          }
        : { available: true, reason: "" };

  const bar: PresentationState = empty
    ? { available: false, reason: noSeries }
    : withoutAnyPeriod.length === (facts || []).length && facts.length > 0
      ? {
          available: false,
          reason:
            "None of the selected series published a value, so there is " +
            "nothing to draw.",
        }
      : { available: true, reason: "" };

  const crossSectional: PresentationState = empty
    ? { available: false, reason: noSeries }
    : crossSectionalReason
      ? { available: false, reason: crossSectionalReason }
      : { available: true, reason: "" };

  const heatmap: PresentationState = empty
    ? { available: false, reason: noSeries }
    : heatmapReason
      ? { available: false, reason: heatmapReason }
      : { available: true, reason: "" };

  const correlation: PresentationState = empty
    ? { available: false, reason: noSeries }
    : correlationReason
      ? { available: false, reason: correlationReason }
      : { available: true, reason: "" };

  return {
    line,
    bar,
    scatter: crossSectional,
    ranking: crossSectional,
    correlation,
    heatmap,
  };
}

/** The presentations a reader may pick right now. */
export function availablePresentations(
  offer: PresentationOffer,
): WorkbenchPresentation[] {
  return WORKBENCH_PRESENTATIONS.filter(
    (presentation) => offer[presentation].available,
  );
}

/** The ones that are listed with a reason instead. */
export function unavailablePresentations(
  offer: PresentationOffer,
): { presentation: WorkbenchPresentation; reason: string }[] {
  return WORKBENCH_PRESENTATIONS.filter(
    (presentation) => !offer[presentation].available,
  ).map((presentation) => ({
    presentation,
    reason: offer[presentation].reason,
  }));
}

// ---------------------------------------------------------------------------
// From rows to a plotted series
// ---------------------------------------------------------------------------

/** One point on a longitudinal chart: a published value at a published period. */
export interface PlottedPoint {
  period: string;
  /** Epoch milliseconds, or `null` for a period no date parser can read. */
  time: number | null;
  value: number;
  row: ObservationRow;
}

/** One series ready to draw, with everything its legend entry must say. */
export interface PlottedSeries {
  key: string;
  series: WorkbenchSeries;
  /** The catalog's display name, or the code when none was published. */
  label: string;
  unit: string;
  /** True when `unit` is this module's placeholder, not a published unit. */
  unitUnpublished: boolean;
  points: PlottedPoint[];
  /** Rows whose period published no number, counted rather than plotted. */
  droppedPeriods: number;
  /** True when the page bound cut this series' history short. */
  truncated: boolean;
}

/**
 * Turn one series' rows into points, dropping nothing silently.
 *
 * `publishedNumber` rejects the absent value before any coercion, for the
 * reason `TimeSeriesChart` spells out: `Number(null)` and `Number("")` are
 * both `0`, and a suppressed period joining the line at zero describes a
 * different series than the source published. The rows that fall out are
 * counted into `droppedPeriods` so the chart can say so.
 *
 * Points are ordered by published period text, which is ISO for every source
 * the analysis surface serves, so the order is chronological without this
 * module deciding what a date is.
 */
export function buildPlottedSeries({
  series,
  rows,
  label,
  unit,
  truncated = false,
}: {
  series: WorkbenchSeries;
  rows: readonly ObservationRow[];
  label?: string | null;
  unit?: string | null;
  truncated?: boolean;
}): PlottedSeries {
  const points: PlottedPoint[] = [];
  let dropped = 0;
  for (const row of rows || []) {
    const value = publishedNumber(row?.value);
    if (value === null) {
      dropped += 1;
      continue;
    }
    const period = observationPeriodLabel(row);
    const parsed = Date.parse(period);
    points.push({
      period,
      time: Number.isFinite(parsed) ? parsed : null,
      value,
      row,
    });
  }
  points.sort((left, right) => left.period.localeCompare(right.period));

  const published = typeof unit === "string" ? unit.trim() : "";
  return {
    key: seriesKey(series),
    series,
    label: label || series.metricCode,
    unit: published || UNPUBLISHED_UNIT_LABEL,
    unitUnpublished: published === "",
    points,
    droppedPeriods: dropped,
    truncated,
  };
}

/**
 * The legend sentence for one series: everything a reader needs to know what
 * they are looking at, and nothing they would have to go and look up.
 *
 * Source, measure, grain and geography, in that order, because that is the
 * order the question is asked in — which publisher, of what, at what grain,
 * where. The unit rides the axis rather than the legend; the scope and a
 * pinned release ride here because two series of one measure at two releases
 * are otherwise indistinguishable.
 */
export function describeSeries(
  plotted: PlottedSeries,
  geographyName?: string | null,
): string {
  const { series } = plotted;
  const grain =
    GEO_GRAIN_LABELS[series.geoLevel]?.one || series.geoLevel || "geography";
  const where = geographyName || series.geoId;
  const parts = [
    `${series.sourceCode} · ${plotted.label}`,
    `${grain}: ${where}`,
  ];
  if (series.release) {
    parts.push(`release ${series.release}`);
  } else if (series.scope === "as_released") {
    parts.push("newest release of each period");
  }
  const pinned = Object.keys(series.filters || {}).sort();
  if (pinned.length > 0) {
    parts.push(
      pinned.map((name) => `${name}: ${series.filters[name]}`).join(", "),
    );
  }
  return parts.join(" — ");
}

/**
 * What a chart's accessible label must say: how much it draws, and how much
 * it does not.
 *
 * The second half is the part that matters. A chart that says "3 series" and
 * nothing else reads, to someone who cannot see it, as three complete
 * histories — which is exactly the reading the dropped-period count and the
 * truncation flag exist to prevent.
 */
export function describeChart(
  presentation: WorkbenchPresentation,
  plotted: readonly PlottedSeries[],
): string {
  const drawn = (plotted || []).filter((entry) => entry.points.length > 0);
  const dropped = (plotted || []).reduce(
    (total, entry) => total + entry.droppedPeriods,
    0,
  );
  const truncated = (plotted || []).filter((entry) => entry.truncated).length;
  const units = new Set((plotted || []).map((entry) => entry.unit));

  const parts = [
    `${PRESENTATION_LABELS[presentation]} chart of ${drawn.length} series`,
  ];
  if (units.size > 0) {
    parts.push(`in ${[...units].join(" and ")}`);
  }
  if (dropped > 0) {
    parts.push(
      `${dropped} period${dropped === 1 ? "" : "s"} published no value and ` +
        `${dropped === 1 ? "is" : "are"} not plotted`,
    );
  }
  if (truncated > 0) {
    parts.push(
      `${truncated} series ${truncated === 1 ? "was" : "were"} cut short by ` +
        "the page bound, so the history shown is a prefix",
    );
  }
  return `${parts.join("; ")}.`;
}

// ---------------------------------------------------------------------------
// Cross-sectional: one shared grain, one newest value per geography (WB-2)
// ---------------------------------------------------------------------------

/**
 * Why the cross-sectional presentations cannot be served, or `""`.
 *
 * The three cases, in the order a reader meets them:
 *
 * - **Fewer than two measures.** A scatter, a ranking and a correlation are
 *   all statements about two or more measures read at one grain.
 * - **More than two measures.** `/comparison` is a pair by contract, and the
 *   matrix route that answers more arrives with WB-5. This interim state is
 *   one the plan ships deliberately, and it is stated rather than hidden.
 * - **No shared grain.** The intersection of the measures' published grains
 *   is empty, so there is no level to read them at. The publishers' own
 *   declaration, reported as such.
 *
 * The *preflight* verdict is deliberately not decided here. It is the API's,
 * it is presented through `describePreflight` unchanged, and a screen that
 * paraphrased it would be this application restating a compatibility
 * decision it does not make.
 */
export function crossSectionalRefusal({
  series,
  sharedGrains,
  chosenPair = null,
}: {
  series: readonly WorkbenchSeries[];
  sharedGrains: readonly string[];
  /**
   * The pair the reader picked when more than two measures are selected.
   * Before WB-5 this case was refused outright; the matrix route now answers
   * every pair's verdict, so the screen offers a chooser and draws one chart
   * rather than a grid of small scatters nobody can read.
   */
  chosenPair?: readonly [string, string] | null;
}): string {
  const measures = new Set((series || []).map((entry) => entry.metricCode));
  if (measures.size < 2) {
    return (
      "A cross-sectional reading needs at least two measures at one shared " +
      "grain. Add another measure."
    );
  }
  if (measures.size > 2 && !chosenPair) {
    return (
      `${measures.size} measures are selected. A scatter and a ranking are ` +
      "each about one pair — `/comparison` aligns two measures by contract — " +
      "so choose which pair to draw. The correlation reads all of them at " +
      "once through the matrix route."
    );
  }
  if ((sharedGrains || []).length === 0) {
    return (
      "These two measures publish no geography grain in common, so there is " +
      "no level to read them at together. The publishers' declaration, not a " +
      "limit of this screen."
    );
  }
  return "";
}

/**
 * The two measures a cross-sectional reading is about, in selection order.
 *
 * `null` unless exactly two distinct measures are selected. Several series of
 * one measure at several geographies is a longitudinal composition, not a
 * pair: `/comparison` reads one newest value per geography for each of two
 * measures, so the geographies are the route's answer rather than the
 * reader's selection.
 */
export function crossSectionalPair(
  series: readonly WorkbenchSeries[],
  chosen: readonly [string, string] | null = null,
): [string, string] | null {
  const codes: string[] = [];
  for (const entry of series || []) {
    if (!codes.includes(entry.metricCode)) {
      codes.push(entry.metricCode);
    }
  }
  if (chosen) {
    // Honoured only where both codes are still selected: a chooser left over
    // from a measure the reader has since removed must not name it.
    return codes.includes(chosen[0]) && codes.includes(chosen[1])
      ? [chosen[0], chosen[1]]
      : null;
  }
  return codes.length === 2 ? [codes[0] as string, codes[1] as string] : null;
}

/** Every unordered pair of the selected measures, for the pair chooser. */
export function selectablePairs(
  series: readonly WorkbenchSeries[],
): [string, string][] {
  const codes: string[] = [];
  for (const entry of series || []) {
    if (!codes.includes(entry.metricCode)) {
      codes.push(entry.metricCode);
    }
  }
  const pairs: [string, string][] = [];
  for (let left = 0; left < codes.length; left += 1) {
    for (let right = left + 1; right < codes.length; right += 1) {
      pairs.push([codes[left] as string, codes[right] as string]);
    }
  }
  return pairs;
}

/** A national measure offered as a reference line on a cross-sectional bar. */
export interface ReferenceLineOffer {
  eligible: boolean;
  /** Why it is not offered, or how it will be drawn. Never empty. */
  reason: string;
}

/**
 * Whether a NATIONAL-only measure may be drawn as a reference line.
 *
 * Three conditions, and each exists to stop a specific wrong picture:
 *
 * 1. **Its only published grain is NATIONAL.** A measure published at STATE
 *    *and* nationally belongs on the axis as a geography like any other; it
 *    does not need a reference line, and drawing it as one would hide the
 *    state values it publishes.
 * 2. **Its unit equals the axis's unit.** A line is a position on the value
 *    axis, so drawing a measure in different units at that position asserts a
 *    comparison the units refuse — the same thing the `units` compatibility
 *    rule fails a pair for.
 * 3. **The chart is cross-sectional.** On a time axis a national measure is
 *    an ordinary series with its own history; flattening it to one line would
 *    discard periods it published.
 *
 * An eligible line is labelled with its own period, and never enters
 * `/comparison` or a correlation: it is one geography, and a correlation over
 * one point is not a statistic.
 */
export function referenceLineOffer({
  grains,
  unit,
  axisUnit,
  presentation,
}: {
  /** The measure's published `valid_geo_grains`, normalised. */
  grains: readonly string[];
  unit: string | null | undefined;
  /** The unit of the axis the bar chart is drawn against. */
  axisUnit: string | null | undefined;
  presentation: WorkbenchPresentation;
}): ReferenceLineOffer {
  if (!isCrossSectional(presentation)) {
    return {
      eligible: false,
      reason:
        "A national measure is an ordinary series on a time axis, with its " +
        "own published history. A reference line is offered only on a " +
        "cross-sectional chart.",
    };
  }
  const published = [...(grains || [])];
  if (published.length === 0) {
    return {
      eligible: false,
      reason:
        "This measure publishes no geography grain, so it cannot be shown to " +
        "be national. Unknown is not national.",
    };
  }
  const others = published.filter((grain) => grain !== "NATIONAL");
  if (others.length > 0) {
    return {
      eligible: false,
      reason:
        `This measure publishes geographies at ${others.join(", ")}, so it ` +
        "belongs on the axis as those geographies rather than as one line.",
    };
  }
  const own = typeof unit === "string" ? unit.trim() : "";
  const axis = typeof axisUnit === "string" ? axisUnit.trim() : "";
  if (!own || !axis) {
    return {
      eligible: false,
      reason:
        "A reference line sits on the value axis, so its unit must be the " +
        "axis's. One of the two publishes no unit, so they cannot be shown " +
        "to agree.",
    };
  }
  if (own.toLocaleLowerCase() !== axis.toLocaleLowerCase()) {
    return {
      eligible: false,
      reason:
        `This measure publishes ${own} and the axis is ${axis}. A line drawn ` +
        "at a position on an axis in another unit asserts a comparison the " +
        "units refuse.",
    };
  }
  return {
    eligible: true,
    reason:
      `Drawn as a horizontal line in ${own}, labelled with its own period. ` +
      "It is one national value, not a geography on this axis, and it is " +
      "never sent to the comparison or entered into a correlation.",
  };
}

// ---------------------------------------------------------------------------
// The geography x period heatmap (WB-2)
// ---------------------------------------------------------------------------

/**
 * The most cells one heatmap draws, per side.
 *
 * Sixty by sixty is 3,600 rects, which renders and reads. Past it the cells
 * are narrower than their own borders, so the picture stops carrying the
 * values it claims to. The cap is reported with the narrowing that would fit
 * — a state, a year range — rather than silently truncating, because a
 * heatmap quietly showing the first sixty counties of 3,143 is a picture of
 * Alabama labelled as the country.
 */
export const MAX_HEATMAP_GEOGRAPHIES = 60;
export const MAX_HEATMAP_PERIODS = 60;

/** One cell: a published value, or the stated absence of one. */
export interface HeatmapCell {
  geoId: string;
  period: string;
  /** `null` where the measure published no value for this geography-period. */
  value: number | null;
  /** The source's own word for why, where it published one. */
  valueStatus: string | null;
  release: string | null;
}

export interface HeatmapModel {
  /** Row keys, in the order the rows are drawn. */
  geographies: { geoId: string; name: string }[];
  /** Column keys, chronological. */
  periods: string[];
  cells: HeatmapCell[];
  minValue: number | null;
  maxValue: number | null;
  /** Cells with a published number. */
  valueCount: number;
  /** Cells the measure published no number for. */
  unpublishedCount: number;
  /** True when the cap cut the answer down. */
  capped: boolean;
  /** What the cap did and what would fit, or "". */
  capNote: string;
}

const EMPTY_HEATMAP: HeatmapModel = {
  geographies: [],
  periods: [],
  cells: [],
  minValue: null,
  maxValue: null,
  valueCount: 0,
  unpublishedCount: 0,
  capped: false,
  capNote: "",
};

/**
 * One measure's settled history laid out as geographies by periods.
 *
 * Every geography-period pair in the drawn rectangle becomes a cell, whether
 * or not a row arrived for it, because the absence is the thing a reader most
 * needs to see: a heatmap that only drew the rows it received would show a
 * ragged block and leave "not published" indistinguishable from the edge of
 * the data.
 *
 * A cell whose row published no number carries `value: null` and the source's
 * own `value_status` where there is one. It is never `0`, and its colour is
 * never a colour on the scale — the WEB-078 rule, one layer over from the
 * choropleth: a withheld value and no observation are different statements,
 * and both are different from a low value.
 *
 * Nothing is aggregated. Where a geography-period somehow carries two rows,
 * the first is kept and the second ignored rather than summed or averaged,
 * because a cell is one published value and a derived one would be this
 * client authoring a fact. The rows are keyed through a nested map rather
 * than a joined string, so a geography identity containing any separator this
 * module might have chosen cannot collide with another.
 */
export function heatmapModel({
  rows,
  geographyNames = {},
}: {
  rows: readonly ObservationRow[];
  geographyNames?: Record<string, string>;
}): HeatmapModel {
  if (!rows || rows.length === 0) {
    return EMPTY_HEATMAP;
  }

  const periodSet = new Set<string>();
  const byGeography = new Map<string, Map<string, ObservationRow>>();
  for (const row of rows) {
    const geoId = String(row?.geo_id ?? "");
    const period = observationPeriodLabel(row);
    if (!geoId || !period) {
      continue;
    }
    periodSet.add(period);
    let periodsOf = byGeography.get(geoId);
    if (!periodsOf) {
      periodsOf = new Map<string, ObservationRow>();
      byGeography.set(geoId, periodsOf);
    }
    if (!periodsOf.has(period)) {
      periodsOf.set(period, row);
    }
  }

  const allPeriods = [...periodSet].sort((left, right) =>
    left.localeCompare(right),
  );
  const allGeographies = [...byGeography.keys()].sort((left, right) =>
    (geographyNames[left] || left).localeCompare(geographyNames[right] || right),
  );

  const periods = allPeriods.slice(0, MAX_HEATMAP_PERIODS);
  const geographies = allGeographies
    .slice(0, MAX_HEATMAP_GEOGRAPHIES)
    .map((geoId) => ({ geoId, name: geographyNames[geoId] || geoId }));

  const capped =
    allPeriods.length > periods.length ||
    allGeographies.length > geographies.length;
  const capNote = capped
    ? [
        allGeographies.length > geographies.length
          ? `${allGeographies.length} geographies published values and this ` +
            `heatmap draws the first ${MAX_HEATMAP_GEOGRAPHIES}. Narrow to a ` +
            "state to see the rest."
          : "",
        allPeriods.length > periods.length
          ? `${allPeriods.length} periods were published and this heatmap ` +
            `draws the earliest ${MAX_HEATMAP_PERIODS}. Narrow the year range ` +
            "to see the rest."
          : "",
      ]
        .filter(Boolean)
        .join(" ")
    : "";

  const cells: HeatmapCell[] = [];
  let minValue: number | null = null;
  let maxValue: number | null = null;
  let valueCount = 0;
  let unpublishedCount = 0;

  for (const geography of geographies) {
    const periodsOf = byGeography.get(geography.geoId);
    for (const period of periods) {
      const row = periodsOf?.get(period);
      const value = row ? publishedNumber(row.value) : null;
      if (value === null) {
        unpublishedCount += 1;
      } else {
        valueCount += 1;
        minValue = minValue === null ? value : Math.min(minValue, value);
        maxValue = maxValue === null ? value : Math.max(maxValue, value);
      }
      cells.push({
        geoId: geography.geoId,
        period,
        value,
        valueStatus:
          row && typeof row.value_status === "string" && row.value_status
            ? row.value_status
            : null,
        release:
          row && row.release !== null && row.release !== undefined
            ? String(row.release)
            : null,
      });
    }
  }

  return {
    geographies,
    periods,
    cells,
    minValue,
    maxValue,
    valueCount,
    unpublishedCount,
    capped,
    capNote,
  };
}

// ---------------------------------------------------------------------------
// The correlation, on screen (WB-5)
// ---------------------------------------------------------------------------

/** The measure count `/comparison/matrix` serves between, mirrored here. */
export const MIN_MATRIX_METRICS = 2;
export const MAX_MATRIX_METRICS = 8;

/** Whether a correlation may be asked for, and what to say when it may not. */
export interface CorrelationEligibility {
  eligible: boolean;
  /** Which route would answer it. `null` when none would. */
  route: "correlation" | "matrix" | null;
  /** Why not, in the API's or the publication's terms. "" when eligible. */
  reason: string;
}

/**
 * Whether the current selection can be asked for a correlation.
 *
 * The order of the refusals is the order a reader can act on them, and each
 * one names something the API or the publication decided rather than
 * something this screen preferred:
 *
 * 1. **Fewer than two, or more than eight, measures.** The bounds
 *    `/comparison/matrix` declares.
 * 2. **A source the analysis routes decline.** Carried from the capability
 *    entry, so CDC, USDA NASS and FBI UCR are named before a request is made
 *    rather than after a 422.
 * 3. **An incomparable pair.** The preflight's own verdict, presented as the
 *    rules worded it.
 *
 * The route follows from the count, because the API's shape does:
 * `/comparison/correlation` answers a pair and `/comparison/matrix` answers
 * three to eight.
 *
 * **What this deliberately does not check is the presentation on screen.**
 * The plan asks the control to be absent for "a longitudinal composition",
 * and the literal reading — refuse while a line or bar is selected — makes
 * the control unreachable, because the correlation *is* one of the
 * presentations a reader selects. So the refusal moved to where it is true
 * and useful: `CORRELATION_IS_ACROSS_GEOGRAPHIES` rides the panel, telling a
 * reader who arrived from a line chart that this coefficient is measured
 * across geographies at the shared grain and is not a correlation of the two
 * histories they were just looking at. The statistic the plan declines to
 * offer is still not offered; it is named rather than silently absent.
 */
export const CORRELATION_IS_ACROSS_GEOGRAPHIES =
  "This coefficient is measured across geographies at the shared grain — one " +
  "newest value per geography for each measure — not across the periods on " +
  "the line chart. A correlation between two histories of one geography is a " +
  "different statistic, with a shared time trend able to produce a " +
  "coefficient on its own, and this page does not offer it.";

export function correlationEligibility({
  series,
  analysisRefusals = {},
  declaredRoutes = {},
  preflightBlocking = [],
  preflightRead = true,
}: {
  series: readonly WorkbenchSeries[];
  /** Per source code, the reason the analysis routes decline it. */
  analysisRefusals?: Record<string, string>;
  /**
   * Per source code, which correlation route the capability entry declares.
   *
   * Read from the contract rather than inferred from the refusal above. The
   * two used to be one question — a source either declared the whole aligned
   * analysis surface or none of it — so the panel asked about
   * `/comparison/preflight` and sent a request to `/comparison/correlation`.
   * They are separate declarations now (API-138), and a source declaring one
   * without the other must be refused on the route it lacks rather than on
   * the route it has. A source missing from this map declares neither: an
   * unknown capability is not a capability.
   */
  declaredRoutes?: Record<string, { correlation?: boolean; matrix?: boolean }>;
  /** The failed preflight rules, for a pair. Empty when comparable. */
  preflightBlocking?: readonly { rule: string; reason: string }[];
  /** False while the verdict for a pair has not come back yet. */
  preflightRead?: boolean;
}): CorrelationEligibility {
  const measures = [...new Set((series || []).map((entry) => entry.metricCode))];
  if (measures.length < MIN_MATRIX_METRICS) {
    return {
      eligible: false,
      route: null,
      reason:
        `A correlation is a statistic about at least ${MIN_MATRIX_METRICS} ` +
        "measures read at one grain. Add another measure.",
    };
  }
  if (measures.length > MAX_MATRIX_METRICS) {
    return {
      eligible: false,
      route: null,
      reason:
        `The matrix route answers between ${MIN_MATRIX_METRICS} and ` +
        `${MAX_MATRIX_METRICS} measures; ${measures.length} are selected. ` +
        "Remove some before asking for a correlation.",
    };
  }

  const declined = [
    ...new Set(
      (series || [])
        .filter((entry) => analysisRefusals[entry.sourceCode])
        .map((entry) => entry.sourceCode),
    ),
  ];
  if (declined.length > 0) {
    return {
      eligible: false,
      route: null,
      // The API's own sentence, not a paraphrase of it.
      reason: declined
        .map((code) => analysisRefusals[code] as string)
        .join(" "),
    };
  }

  const route = measures.length === 2 ? "correlation" : "matrix";

  const undeclared = [
    ...new Set(
      (series || [])
        .filter((entry) => !declaredRoutes[entry.sourceCode]?.[route])
        .map((entry) => entry.sourceCode),
    ),
  ];
  if (undeclared.length > 0) {
    return {
      eligible: false,
      route: null,
      reason:
        `The API declares no ${
          route === "correlation" ? "/comparison/correlation" : "/comparison/matrix"
        } route for ${undeclared.join(", ")}, so a coefficient over ` +
        `${measures.length} measures cannot be asked for here.`,
    };
  }

  if (route === "correlation") {
    if (!preflightRead) {
      return {
        eligible: false,
        route: null,
        reason:
          "Checking whether these two measures may be read together before " +
          "asking for a coefficient.",
      };
    }
    if (preflightBlocking.length > 0) {
      return {
        eligible: false,
        route: null,
        reason: preflightBlocking.map((rule) => rule.reason).join("; "),
      };
    }
  }

  return { eligible: true, route, reason: "" };
}

/** One line of the correlation panel, ready to render. */
export interface CorrelationReading {
  label: string;
  /** The formatted coefficient, or the reason there is none. */
  value: string;
  /** True when this reading is an API computation rather than a published
   *  figure, which is every coefficient on this panel. */
  derived: boolean;
}

/**
 * How many decimal places a coefficient is shown to.
 *
 * Three. Two hides the difference between 0.412 and 0.418, which is the kind
 * of difference a reader comparing two cells of a matrix is looking at; four
 * implies a precision the inputs' own uncertainty does not support — and the
 * uncertainty caveat travels with the answer saying exactly that.
 */
export const CORRELATION_PRECISION = 3;

export function formatCoefficient(value: number | null | undefined): string {
  return value === null || value === undefined || !Number.isFinite(value)
    ? ""
    : Number(value).toFixed(CORRELATION_PRECISION);
}

/**
 * What the panel says where the API published no pair count.
 *
 * `n` is optional in the contract (`CorrelationStatistic.n?: number`), and
 * the client used to read it as `Number(statistic.n ?? 0)`. That rendered an
 * unpublished count as "0 paired geographies" -- a statement the API never
 * made, and one a reader cannot tell from a genuine zero. The handoff's rule
 * is that a value the source did not publish is never a zero; every other
 * `?? 0` under `apps/web` is a counter or an index, and this one was a
 * published statistic.
 */
export const PAIRED_GEOGRAPHIES_NOT_PUBLISHED = "Not published";

/**
 * The pair count, or `null` where the API published none.
 *
 * Absence and a zero are different answers and are kept apart here, so every
 * reading that depends on the count can refuse together rather than each
 * inventing its own fallback.
 */
export function pairedGeographies(
  statistic: CorrelationLike | null | undefined,
): number | null {
  const value = statistic?.n;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * The caveat line's fallback: how many geographies a coefficient was measured
 * over, or that the API did not say.
 *
 * Shared by the pair list and the matrix tooltip so one absent count cannot
 * read two ways on one screen.
 */
export function pairedGeographiesText(
  statistic: CorrelationLike | null | undefined,
): string {
  const n = pairedGeographies(statistic);
  return n === null
    ? "Paired geographies not published."
    : `${formatNumber(n)} paired geographies.`;
}

/**
 * The panel's readings, in the order they are read.
 *
 * `n` first, because a coefficient's meaning depends on how many pairs it was
 * measured over and a reader who sees the number first anchors on it. Then
 * both coefficients, then the coverage and contemporaneity the caveats
 * elaborate.
 *
 * A null coefficient shows the reason rather than an em-dash: "no coefficient
 * is reported" with nothing after it teaches a reader that the screen is
 * broken, and the API already sent the reason in `caveats`.
 */
export function correlationReadings(
  statistic: CorrelationLike | null | undefined,
  {
    geographiesA,
    geographiesB,
    nullReason = "",
  }: {
    geographiesA?: number | null;
    geographiesB?: number | null;
    nullReason?: string;
  } = {},
): CorrelationReading[] {
  if (!statistic) {
    return [];
  }
  const n = pairedGeographies(statistic);
  const readings: CorrelationReading[] = [
    {
      label: "Paired geographies",
      value: n === null ? PAIRED_GEOGRAPHIES_NOT_PUBLISHED : formatNumber(n),
      derived: false,
    },
  ];

  for (const [label, value] of [
    ["Pearson r", statistic.pearson_r],
    ["Spearman ρ", statistic.spearman_rho],
  ] as const) {
    const formatted = formatCoefficient(value as number | null | undefined);
    readings.push({
      label,
      value: formatted || nullReason || "not reported for these pairs",
      derived: true,
    });
  }

  const coverage = [geographiesA, geographiesB]
    .map((count) => (typeof count === "number" ? count : null))
    .filter((count): count is number => count !== null);
  if (coverage.length > 0) {
    readings.push({
      label: "Coverage",
      value:
        (n === null
          ? `${PAIRED_GEOGRAPHIES_NOT_PUBLISHED}, of `
          : `${formatNumber(n)} paired of `) +
        `${coverage.map((count) => formatNumber(count)).join(" and ")} published`,
      derived: false,
    });
  }

  const contemporaneous =
    typeof statistic.contemporaneous_pairs === "number"
      ? statistic.contemporaneous_pairs
      : null;
  readings.push({
    label: "Contemporaneous pairs",
    value:
      contemporaneous === null || n === null
        ? PAIRED_GEOGRAPHIES_NOT_PUBLISHED
        : `${formatNumber(contemporaneous)} of ${formatNumber(n)}`,
    derived: false,
  });

  return readings;
}

/** The shape both `/comparison/correlation` and a matrix cell share. */
export interface CorrelationLike {
  n?: number;
  contemporaneous_pairs?: number;
  pearson_r?: number | null;
  spearman_rho?: number | null;
  periods_differ?: boolean;
}

// ---------------------------------------------------------------------------
// The correlation matrix, as a heatmap
// ---------------------------------------------------------------------------

/** One cell of the pairwise correlation matrix. */
export interface CorrelationMatrixCell {
  metricCodeA: string;
  metricCodeB: string;
  /** `null` on the declined cells and on the diagonal. */
  value: number | null;
  /** Why a cell carries no coefficient: declined, or not measurable. */
  reason: string;
  /** True where the compatibility policy declined the pair outright. */
  declined: boolean;
  /** True on the diagonal, where a measure meets itself. */
  identity: boolean;
  /** The pair count, or `null` where the API published none for this cell. */
  n: number | null;
}

export interface CorrelationMatrixModel {
  codes: string[];
  cells: CorrelationMatrixCell[];
  /** How many cells carry a coefficient. */
  measuredCount: number;
  declinedCount: number;
}

/**
 * The pairwise matrix as a square grid, from `/comparison/matrix`'s answer.
 *
 * The diagonal is filled with `identity: true` and no coefficient rather than
 * with `1`. A measure correlates perfectly with itself by arithmetic, not by
 * measurement, and a grid whose diagonal reads 1.000 invites the eye to
 * calibrate the rest of the scale against a number nothing measured.
 *
 * A declined cell carries `declined: true` and the failed rules' own words.
 * It is drawn in the not-published colour — never a colour on the diverging
 * scale — for the reason the heatmap's own cells are: a declined pair and a
 * coefficient near zero are different statements, and colour must not conflate
 * them.
 *
 * `which` chooses the coefficient; both are served for every comparable pair,
 * and the panel lets a reader switch because Pearson and Spearman disagreeing
 * is itself information.
 */
export function correlationMatrixModel({
  codes,
  pairs,
  which = "pearson_r",
}: {
  codes: readonly string[];
  pairs: readonly {
    metric_code_a?: string;
    metric_code_b?: string;
    comparable?: boolean;
    rules?: readonly { rule: string; status: string; reason: string }[];
    statistic?: CorrelationLike | null;
  }[];
  which?: "pearson_r" | "spearman_rho";
}): CorrelationMatrixModel {
  const byPair = new Map<string, (typeof pairs)[number]>();
  for (const pair of pairs || []) {
    const a = String(pair.metric_code_a ?? "");
    const b = String(pair.metric_code_b ?? "");
    if (!a || !b) {
      continue;
    }
    // Stored under both orders: the route answers each unordered pair once,
    // and the grid asks for it from both sides of the diagonal.
    byPair.set(`${a}␟${b}`, pair);
    byPair.set(`${b}␟${a}`, pair);
  }

  const cells: CorrelationMatrixCell[] = [];
  let measuredCount = 0;
  let declinedCount = 0;

  for (const rowCode of codes) {
    for (const columnCode of codes) {
      if (rowCode === columnCode) {
        cells.push({
          metricCodeA: rowCode,
          metricCodeB: columnCode,
          value: null,
          reason:
            "A measure against itself. Perfect by arithmetic, not by " +
            "measurement, so no coefficient is drawn here.",
          declined: false,
          identity: true,
          n: null,
        });
        continue;
      }
      const pair = byPair.get(`${rowCode}␟${columnCode}`);
      if (!pair) {
        cells.push({
          metricCodeA: rowCode,
          metricCodeB: columnCode,
          value: null,
          reason: "The matrix carried no answer for this pair.",
          declined: false,
          identity: false,
          n: null,
        });
        continue;
      }
      if (pair.comparable === false) {
        declinedCount += 1;
        cells.push({
          metricCodeA: rowCode,
          metricCodeB: columnCode,
          value: null,
          reason: (pair.rules || [])
            .filter((rule) => rule.status === "fail")
            .map((rule) => rule.reason)
            .join("; "),
          declined: true,
          identity: false,
          n: null,
        });
        continue;
      }
      const statistic = pair.statistic || null;
      const value =
        statistic && statistic[which] !== null && statistic[which] !== undefined
          ? Number(statistic[which])
          : null;
      if (value !== null) {
        measuredCount += 1;
      }
      cells.push({
        metricCodeA: rowCode,
        metricCodeB: columnCode,
        value,
        reason:
          value === null
            ? "These pairs cannot carry a coefficient; the caveats say why."
            : "",
        declined: false,
        identity: false,
        n: pairedGeographies(statistic),
      });
    }
  }

  return { codes: [...codes], cells, measuredCount, declinedCount };
}
