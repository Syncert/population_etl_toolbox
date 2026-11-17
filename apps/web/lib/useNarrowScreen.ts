"use client";

import { useEffect, useState } from "react";

/**
 * True on a phone-width screen, after the first paint. A chart that scales a
 * wide drawing down to a phone draws its text at half size; a chart that asks
 * this lays itself out about as wide as the screen instead, so its text is
 * drawn near the size it was written at (website UX review D1).
 */
export function useNarrowScreen(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    // A browser without media queries keeps the wide layout.
    if (typeof window.matchMedia !== "function") return undefined;
    const query = window.matchMedia("(max-width: 640px)");
    const update = () => setNarrow(query.matches);
    update();
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);
  return narrow;
}

/** A drawing's width in its own units: narrow on a phone, wide otherwise. */
export const NARROW_DRAWING_WIDTH = 350;
