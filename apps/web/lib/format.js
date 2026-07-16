export function displayMetricName(metric) {
  if (typeof metric === "object" && metric?.metric_code?.endsWith("B01003_001")) {
    return "Total population";
  }
  const value = typeof metric === "string" ? metric : metric?.metric_display_name;
  if (!value) return "Untitled metric";
  return value
    .replaceAll("!!", " - ")
    .replaceAll("!", " - ")
    .replace(/^Estimate\s*-\s*/i, "")
    .replace(/\s+/g, " ")
    .trim();
}
