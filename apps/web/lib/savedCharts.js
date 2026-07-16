export const SAVED_CHARTS_KEY = "economic-data-studio:saved-charts:v1";
export const BUILDER_DRAFT_KEY = "economic-data-studio:builder-draft:v1";

export function readSavedCharts() {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const value = JSON.parse(window.localStorage.getItem(SAVED_CHARTS_KEY) || "[]");
    return Array.isArray(value) ? value : [];
  } catch {
    return [];
  }
}

export function saveChart(chart) {
  const charts = readSavedCharts();
  const next = [chart, ...charts.filter((item) => item.id !== chart.id)].slice(0, 50);
  window.localStorage.setItem(SAVED_CHARTS_KEY, JSON.stringify(next));
  return next;
}
