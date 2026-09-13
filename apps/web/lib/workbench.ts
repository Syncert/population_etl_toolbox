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
  heatmapReason = "",
}: {
  series: readonly WorkbenchSeries[];
  facts: readonly PresentationSeriesFacts[];
  crossSectionalReason?: string;
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

  return {
    line,
    bar,
    scatter: crossSectional,
    ranking: crossSectional,
    correlation: crossSectional,
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
