// The place a reader last looked at, kept for this browser tab only.
//
// Explainers show their worked example for it (explainer-pages). It lives in
// `sessionStorage`, never in a URL: an address that carried it would put a
// reader's place into every link they share and every history entry.

export interface LastPlace {
  geoId: string;
  name: string;
  level: string;
}

export const LAST_PLACE_KEY = "eds.lastPlace";

export function readLastPlace(storage: Pick<Storage, "getItem"> | null | undefined): LastPlace | null {
  try {
    const parsed = JSON.parse(storage?.getItem(LAST_PLACE_KEY) || "null");
    if (parsed && typeof parsed.geoId === "string" && parsed.geoId && typeof parsed.name === "string" && typeof parsed.level === "string") {
      return { geoId: parsed.geoId, name: parsed.name, level: parsed.level };
    }
  } catch {
    // Unreadable or blocked storage reads as no place.
  }
  return null;
}

export function rememberLastPlace(storage: Pick<Storage, "setItem"> | null | undefined, place: LastPlace): void {
  try {
    storage?.setItem(LAST_PLACE_KEY, JSON.stringify(place));
  } catch {
    // A browser with storage blocked simply has no last place.
  }
}
