// The observation table as the file a reader keeps.
//
// Extracted from `SourceExplorerPage` so the file's contents can be
// asserted. Neither this export's rows nor the profile product's were
// covered anywhere -- the only export coverage in the suite was that the
// button is enabled -- which is how the `margin_of_error`-only defect
// WEB-051 and WEB-053 fixed here survived unchanged in the profile product
// until WEB-060.
//
// The rule those rows established, restated because this is where it is
// enforced: every field `ObservationUncertainty` and `ObservationCoverage`
// publish travels, whether or not a given source publishes it. A file
// carrying a subset would be this client deciding which part of a source's
// participation basis, or which part of its uncertainty, a reader may have.

import {
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_FIELDS,
  observationCoverageValue,
  observationDimensionValue,
  observationPeriodLabel,
  observationUncertaintyValue,
} from "./observationAccess";
import { observationName, observationUnit } from "./explorerViewModel";
import type { ObservationRow } from "./explorerViewModel";
import { describeSeries } from "./workbench";
import type { PlottedSeries } from "./workbench";

export interface TabularExport {
  headings: string[];
  /**
   * Cells as the row published them. Deliberately `unknown`: an
   * `ObservationRow`'s source-specific fields are typed loosely, and the
   * caller's one escaping function takes `unknown` and never coerces an
   * absent value into anything.
   */
  rows: unknown[][];
}

/**
 * The loaded observations as headings and rows.
 *
 * `scope` is the read's own scope, which the explorer only ever holds as
 * `as_released` where the source declares releases, so the column cannot
 * claim a pinned read of a source that publishes none.
 *
 * `dimensions` is the field set `/catalog/capabilities` declares for the
 * source (`observation_dimensions`), one column each. It used to be the
 * source's *filterable* names, which is a different and much smaller list:
 * four of seven sources wrote no dimension column at all, and CDC wrote two
 * of fourteen. A file carrying a subset would be this client deciding which
 * part of a source's published description a reader may have, which is
 * WEB-051's rule and decides this outright (WEB-061).
 */
export function observationExport(
  rows: ObservationRow[] | null | undefined,
  options: { scope: string; dimensions?: readonly string[] },
): TabularExport {
  const dimensions = options.dimensions || [];
  const headings = [
    "geo_id",
    "geo_name",
    "period",
    "metric_code",
    "value",
    "value_status",
    "unit",
    "source",
    "dataset",
    ...OBSERVATION_UNCERTAINTY_FIELDS,
    ...OBSERVATION_COVERAGE_FIELDS,
    "scope",
    "release",
    "as_of",
    ...dimensions,
  ];
  return {
    headings,
    rows: (rows || []).map((item) => [
      item.geo_id,
      observationName(item),
      observationPeriodLabel(item),
      item.metric_code,
      item.value,
      item.value_status,
      observationUnit(item),
      item.source || item.source_code,
      item.dataset || item.dataset_code,
      ...OBSERVATION_UNCERTAINTY_FIELDS.map((field) =>
        observationUncertaintyValue(item, field),
      ),
      ...OBSERVATION_COVERAGE_FIELDS.map((field) => observationCoverageValue(item, field)),
      options.scope,
      item.release,
      item.as_of,
      ...dimensions.map((name) => observationDimensionValue(item, name)),
    ]),
  };
}

/**
 * A workbench composition as the file a reader keeps.
 *
 * One row per plotted value, carrying the same envelope columns
 * `observationExport` writes — every published uncertainty field (WEB-053)
 * and every published coverage field (WEB-051) travels, whether or not a
 * given source publishes one — plus the four a composition needs and a
 * single observation does not:
 *
 * - `series`, so eight measures' rows in one file are separable.
 * - `geo_level` and the series' pinned `geo_id`, because a composition's rows
 *   come from several geographies at several grains and the row's own
 *   attribution alone would not say which series it belongs to.
 * - `derived`, which is `true` only on the correlation rows. No source
 *   publishes a coefficient, and a file mixing published values with API
 *   computations and not saying which is which is the one thing the whole
 *   surface exists to prevent.
 *
 * The correlation rows come last, after every published value, and carry no
 * period or release — a coefficient describes a set of pairs rather than a
 * publication — so a reader sorting by `derived` gets the published half of
 * the file intact.
 */
export function workbenchExport(
  plotted: readonly PlottedSeries[],
  options: {
    dimensions?: readonly string[];
    /** The correlation's readings, where one was asked for and answered. */
    correlation?: {
      metricCodeA: string;
      metricCodeB: string;
      readings: readonly { label: string; value: string; derived: boolean }[];
    } | null;
    /** Names for the pinned geographies, where the catalog published one. */
    geographyNames?: Record<string, string>;
  } = {},
): TabularExport {
  const dimensions = options.dimensions || [];
  const headings = [
    "series",
    "geo_id",
    "geo_name",
    "geo_level",
    "period",
    "metric_code",
    "value",
    "value_status",
    "unit",
    "source",
    "dataset",
    ...OBSERVATION_UNCERTAINTY_FIELDS,
    ...OBSERVATION_COVERAGE_FIELDS,
    "scope",
    "release",
    "as_of",
    "derived",
    ...dimensions,
  ];

  const rows: unknown[][] = [];
  for (const entry of plotted) {
    const label = describeSeries(
      entry,
      (options.geographyNames || {})[entry.series.geoId],
    );
    for (const point of entry.points) {
      const item = point.row;
      rows.push([
        label,
        item.geo_id ?? entry.series.geoId,
        observationName(item) || (options.geographyNames || {})[entry.series.geoId] || "",
        item.geo_level ?? entry.series.geoLevel,
        point.period,
        item.metric_code ?? entry.series.metricCode,
        // The published text, never the parsed number: the parse is what the
        // chart plots, and the file carries what the source sent.
        item.value,
        item.value_status,
        observationUnit(item) || (entry.unitUnpublished ? "" : entry.unit),
        item.source || item.source_code || entry.series.sourceCode,
        item.dataset || item.dataset_code,
        ...OBSERVATION_UNCERTAINTY_FIELDS.map((field) =>
          observationUncertaintyValue(item, field),
        ),
        ...OBSERVATION_COVERAGE_FIELDS.map((field) =>
          observationCoverageValue(item, field),
        ),
        entry.series.scope,
        item.release,
        item.as_of,
        false,
        ...dimensions.map((name) => observationDimensionValue(item, name)),
      ]);
    }
  }

  const correlation = options.correlation;
  if (correlation) {
    const blanks = headings.length;
    for (const reading of correlation.readings) {
      const row: unknown[] = new Array(blanks).fill("");
      row[headings.indexOf("series")] =
        `${correlation.metricCodeA} against ${correlation.metricCodeB}`;
      row[headings.indexOf("metric_code")] =
        `${correlation.metricCodeA}|${correlation.metricCodeB}`;
      row[headings.indexOf("source")] = reading.label;
      row[headings.indexOf("value")] = reading.value;
      row[headings.indexOf("derived")] = reading.derived;
      rows.push(row);
    }
  }

  return { headings, rows };
}

/**
 * The file name for a composition, which says when it is a prefix.
 *
 * `observationExportFilename`'s rule for several series: a read the page
 * bound cut short is named `-partial-`, because the screen said so and the
 * file has to say it too (WEB-059). A composition is partial when *any* of
 * its series was truncated — a file whose third series is a prefix is a
 * partial file, whatever the other seven did.
 */
export function workbenchExportFilename(input: {
  presentation: string;
  plotted: readonly PlottedSeries[];
}): string {
  const truncated = input.plotted.filter((entry) => entry.truncated).length;
  const stem = `workbench-${input.presentation}-${input.plotted.length}-series`;
  return truncated > 0 ? `${stem}-partial-${truncated}-truncated.csv` : `${stem}.csv`;
}
