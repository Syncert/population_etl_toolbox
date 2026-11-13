// A metric's name as a reader sees it: the API's plain label (A-2), with the
// metric code and the label's review status kept in attributes and hover
// text rather than the visible name (WEB-144).

import { displayMetricName, metricNameNote } from "../lib/format";

type Named = {
  metric_code?: string | null;
  metric_display_name?: string | null;
  metric_label?: string | null;
  metric_label_status?: string | null;
};

export function MetricName({ metric, code }: { metric?: Named | null; code?: string | null }) {
  const metricCode = String(code || metric?.metric_code || "");
  const status = metric?.metric_label_status || undefined;
  return (
    <span title={metricNameNote(metric, metricCode) || undefined} data-metric-code={metricCode || undefined} data-label-status={status}>
      {metric ? displayMetricName(metric) : metricCode || "Untitled metric"}
    </span>
  );
}
