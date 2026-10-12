// Which places an explainer's worked example reads (explainer-pages).
//
// The nation always, and the reader's last-viewed place when there is one
// and the example measure publishes at its grain. Kept apart from the
// component so the rule is unit-tested.

import type { GeographySummary } from "./api/types";
import type { ObservationRow } from "./explorerViewModel";
import type { LastPlace } from "./lastPlace";

export interface ExampleTarget {
  key: "place" | "nation";
  name: string;
  geoId: string;
  /** Why no value is asked for, when `geoId` is empty. */
  message: string;
}

export interface ExampleRow extends ExampleTarget {
  row: ObservationRow | null;
}

export function lastPlaceRows(
  lastPlace: LastPlace | null,
  nation: GeographySummary | null,
  publishedGrains: string[] | null | undefined,
): { targets: ExampleTarget[]; note: string } {
  const grains = Array.isArray(publishedGrains) ? publishedGrains : null;
  const publishes = (level: string) => !grains || grains.includes(level);
  const targets: ExampleTarget[] = [];
  let note = "";
  if (!lastPlace) {
    note = "You have not looked at a place in this tab, so the example shows the national value.";
  } else if (lastPlace.level === "NATIONAL") {
    note = "";
  } else {
    targets.push({
      key: "place",
      name: lastPlace.name,
      geoId: publishes(lastPlace.level) ? lastPlace.geoId : "",
      message: publishes(lastPlace.level) ? "" : `Not published at ${lastPlace.level.toLowerCase()} grain`,
    });
  }
  targets.push({
    key: "nation",
    name: "United States",
    geoId: nation && publishes("NATIONAL") ? nation.geo_id : "",
    message: !nation ? "No national geography is published" : publishes("NATIONAL") ? "" : "Not published at national grain",
  });
  return { targets, note };
}
