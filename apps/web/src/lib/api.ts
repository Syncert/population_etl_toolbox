import { config } from "./config";
import { LatestObservationCollection, Metric } from "./types";

async function fetchJson<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(`${config.apiBaseUrl}${path}`, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as T;
  } catch {
    return null;
  }
}

export const apiClient = {
  getHealth: () => fetchJson<{ status: string }>("/health"),
  getMetrics: () => fetchJson<Metric[]>("/api/catalog/metrics"),
  getLatestPopulationByCounty: () =>
    fetchJson<LatestObservationCollection>(
      "/api/observations/latest?metric_id=population&geo_level=county",
    ),
};
