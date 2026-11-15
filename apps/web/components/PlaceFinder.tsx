"use client";

// The home page's place search, for the nation's page (website UX review
// F1): "Find your place" in the header opens /us, which listed the states
// twenty screens down and offered no search. Its own chunk, so a place page
// carries only the import.

import PlaceSearch, { usePlaceCatalog } from "./PlaceSearch";

export default function PlaceFinder() {
  const { catalog, error } = usePlaceCatalog();
  return <PlaceSearch catalog={catalog} error={error} />;
}
