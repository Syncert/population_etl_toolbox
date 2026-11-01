// Many measures' settled histories for a few places, in one request.
//
// A place page draws each chapter's trends for the place and every level
// above it -- a city beside its county, its state and the nation. As single
// reads that was one request per trend and place, about sixteen for a city,
// queued six at a time. The API answers the same reads as one batch: each
// item is exactly what the single settled-history read
// (`buildSettledHistoryRequest`: `scope=as_released&newest_release_per_period=true`)
// returns for its pair, oldest period first (API-187).

import { apiFetch } from "./api/client";
import type { RequestOptions } from "./api/client";
import type { CollectionResponse, Observation } from "./api/types";
import type { ExplorerSource } from "./explorerSources";

/** The most metric codes the API reads in one history batch. */
export const HISTORY_BATCH_MOST_CODES = 12;
/** The most places the API reads in one history batch. */
export const HISTORY_BATCH_MOST_GEOGRAPHIES = 8;

export interface HistoryBatchItem {
  metric_code: string;
  geo_id: string;
  status: number;
  detail: string | null;
  observations: CollectionResponse<Observation> | null;
}

export const historyKey = (metricCode: string, geoId: string) => `${metricCode}|${geoId}`;

/**
 * Whether this source's history read is the one the batch answers: the
 * settled as-released read, filtered by `geo_id`. Any other source keeps its
 * own single read (a latest-only timeseries, for instance).
 */
export function batchesHistory(source: ExplorerSource | null): boolean {
  return Boolean(source?.supportsSettledHistory)
    && Boolean(source?.supportsAsReleased)
    && Boolean(source?.neutralFilters.includes("geo_id"));
}

/** Every pair of `metricCodes` and `geoIds`, by `historyKey`, read in batches. */
export async function getHistoriesBatch(
  metricCodes: string[],
  geoIds: string[],
  options: RequestOptions = {},
): Promise<Map<string, HistoryBatchItem>> {
  const codes = [...new Set(metricCodes)];
  const places = [...new Set(geoIds)];
  const chunks: [string[], string[]][] = [];
  for (let c = 0; c < codes.length; c += HISTORY_BATCH_MOST_CODES) {
    for (let g = 0; g < places.length; g += HISTORY_BATCH_MOST_GEOGRAPHIES) {
      chunks.push([codes.slice(c, c + HISTORY_BATCH_MOST_CODES), places.slice(g, g + HISTORY_BATCH_MOST_GEOGRAPHIES)]);
    }
  }
  const pages = await Promise.all(
    chunks.map(([chunkCodes, chunkPlaces]) =>
      apiFetch<{ items: HistoryBatchItem[] }>("/observations/history/batch", {
        ...options,
        params: { metric_codes: chunkCodes.join(","), geo_ids: chunkPlaces.join(",") },
      }),
    ),
  );
  const found = new Map<string, HistoryBatchItem>();
  for (const page of pages) {
    for (const item of Array.isArray(page?.items) ? page.items : []) {
      found.set(historyKey(item.metric_code, item.geo_id), item);
    }
  }
  return found;
}
