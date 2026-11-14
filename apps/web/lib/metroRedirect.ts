// The named page for a metro addressed by its CBSA code (web-metro-pages),
// asked of the API from the web server. Only a five-digit code is asked
// about: a named segment renders as it is, so a named page costs no extra
// read. Any failure (the API down, slow or answering something unexpected)
// returns null and the page settles the address in the browser instead.

import { PLACE_SEGMENT } from "./placeChapters";

const CBSA_CODE = /^\d{5}$/;
const TIMEOUT_MS = 1500;

/** The API's origin as the web server reaches it (next.config's rewrite). */
export function serverApiOrigin(env: Record<string, string | undefined> = process.env): string {
  return (env.API_ORIGIN || env.NEXT_PUBLIC_API_ORIGIN || "http://localhost:8000").replace(/\/+$/, "");
}

/** `/metro/<name>` when `segment` is a code with a different named page. */
export async function canonicalMetroPath(
  segment: string,
  { fetcher = fetch, origin = serverApiOrigin() }: { fetcher?: typeof fetch; origin?: string } = {},
): Promise<string | null> {
  if (!CBSA_CODE.test(segment)) return null;
  try {
    const response = await fetcher(`${origin}/api/v1/catalog/geographies/metros/${segment}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!response.ok) return null;
    const body = await response.json();
    const own = body?.metro?.segment;
    if (typeof own !== "string" || own === segment || !PLACE_SEGMENT.test(own)) return null;
    return `/metro/${own}`;
  } catch {
    return null;
  }
}
