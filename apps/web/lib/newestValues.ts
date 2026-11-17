// Many measures' newest values for a few places, in one request.
//
// A place page reads every headline measure of a chapter for the place, its
// state and the nation. As single reads that was one request per measure and
// place, about 160 for a county, and the browser queues them six at a time,
// so the page waited on the queue, not on the warehouse. The API answers the
// same reads as one batch: each item is exactly what the single read
// (`buildNewestValueRequest`) returns for its pair (API-180).

import { apiFetch } from "./api/client";
import type { RequestOptions } from "./api/client";
import type { CollectionResponse, Observation } from "./api/types";
import type { ExplorerSource } from "./explorerSources";

/** The most metric codes the API reads in one batch. */
export const NEWEST_BATCH_MOST_CODES = 40;
/** The most places the API reads in one batch. */
export const NEWEST_BATCH_MOST_GEOGRAPHIES = 8;

export interface NewestBatchItem {
  metric_code: string;
  geo_id: string;
  status: number;
  detail: string | null;
  observations: CollectionResponse<Observation> | null;
}

export const newestKey = (metricCode: string, geoId: string) => `${metricCode}|${geoId}`;

/**
 * Whether the single read of this source's newest value is the read the batch
 * answers: reduced by the resource and filtered by `geo_id`. Any other source
 * keeps its own single read.
 */
export function batchesNewestValues(source: ExplorerSource | null): boolean {
  return Boolean(source?.supportsNewestPerGeography) && Boolean(source?.neutralFilters.includes("geo_id"));
}

/** Every pair of `metricCodes` and `geoIds`, by `newestKey`, read in batches. */
export async function getNewestValuesBatch(
  metricCodes: string[],
  geoIds: string[],
  options: RequestOptions = {},
): Promise<Map<string, NewestBatchItem>> {
  const codes = [...new Set(metricCodes)];
  const places = [...new Set(geoIds)];
  const chunks: [string[], string[]][] = [];
  for (let c = 0; c < codes.length; c += NEWEST_BATCH_MOST_CODES) {
    for (let g = 0; g < places.length; g += NEWEST_BATCH_MOST_GEOGRAPHIES) {
      chunks.push([codes.slice(c, c + NEWEST_BATCH_MOST_CODES), places.slice(g, g + NEWEST_BATCH_MOST_GEOGRAPHIES)]);
    }
  }
  const pages = await Promise.all(
    chunks.map(([chunkCodes, chunkPlaces]) =>
      apiFetch<{ items: NewestBatchItem[] }>("/observations/newest/batch", {
        ...options,
        params: { metric_codes: chunkCodes.join(","), geo_ids: chunkPlaces.join(",") },
      }),
    ),
  );
  const found = new Map<string, NewestBatchItem>();
  for (const page of pages) {
    for (const item of Array.isArray(page?.items) ? page.items : []) {
      found.set(newestKey(item.metric_code, item.geo_id), item);
    }
  }
  return found;
}
