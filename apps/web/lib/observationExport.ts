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
