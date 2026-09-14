// What a link that names a measure gets, when the screen it opens cannot
// find that measure.
//
// The explorer and the comparison workspace both open from a link: a catalog
// row, a quality table, a profile card, a saved configuration. Each loads one
// source's metric catalog and then looks for the requested code in it. When
// the code was absent, both screens silently selected something else --
// `pickPreferredMetric` on the explorer, `items[0]` on either side of the
// comparison -- and said nothing. A reader who clicked "Explore" on
// `BLS:LAU:UNEMP_RATE` got Census ACS total population, and a saved BLS
// versus FRED comparison reopened as a real preflight on a pair nobody saved.
//
// Substituting is the one thing a link must not do: the whole point of naming
// a measure is that the screen opens on that measure. This module states the
// rule once, for both screens (WEB-072).

export interface RequestedMetricState {
  /** The measure to select. "" when the request cannot be honoured. */
  metricCode: string;
  /**
   * True when the caller should fall back to its own preferred measure --
   * only when no measure was requested at all.
   */
  chooseDefault: boolean;
  /** What to say on the page. "" when there is nothing to say. */
  notice: string;
}

export interface RequestedMetricInput {
  /** The measure the link asked for, if any. */
  requested?: string | null;
  /** The metric catalog that arrived for the source being shown. */
  items: readonly { metric_code?: string | null }[];
  /** How to name the source in the notice; "" falls back to neutral wording. */
  sourceTitle?: string | null;
}

export function requestedMetricState({
  requested,
  items,
  sourceTitle,
}: RequestedMetricInput): RequestedMetricState {
  const wanted = typeof requested === "string" ? requested.trim() : "";
  if (!wanted) {
    return { metricCode: "", chooseDefault: true, notice: "" };
  }
  const listed = (items || []).some((item) => item?.metric_code === wanted);
  if (listed) {
    return { metricCode: wanted, chooseDefault: false, notice: "" };
  }
  const source = (sourceTitle || "").trim() || "the source shown";
  return {
    metricCode: "",
    chooseDefault: false,
    notice:
      `This link asked for ${wanted}, which ${source} does not publish. ` +
      "Nothing was substituted: pick a measure below, or open the source " +
      "that publishes the one you wanted.",
  };
}
