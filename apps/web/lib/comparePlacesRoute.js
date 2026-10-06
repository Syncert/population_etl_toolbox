// The four compare-two-places routes share one rendering (compare-two-places):
// `/us/<a>[/<county>]/vs/us/<b>[/<county>]`. Segments outside the place
// vocabulary are a 404 before anything is read.
import { notFound } from "next/navigation";

import ComparePlacesPage from "../components/ComparePlacesPage";
import { PLACE_SEGMENT } from "./placeChapters";
import { placeRouteTitle } from "./routeTitles";

function sides(params) {
  const segments = [params.state, params.county, params.otherState, params.otherCounty].filter(Boolean);
  if (!segments.every((segment) => PLACE_SEGMENT.test(segment))) notFound();
  return {
    a: { state: params.state, ...(params.county ? { county: params.county } : {}) },
    b: { state: params.otherState, ...(params.otherCounty ? { county: params.otherCounty } : {}) },
  };
}

export async function compareMetadata({ params }) {
  const { a, b } = sides(await params);
  return { title: `${placeRouteTitle(a.state, a.county)} and ${placeRouteTitle(b.state, b.county)}` };
}

export async function ComparePlacesRoute({ params }) {
  const { a, b } = sides(await params);
  return <ComparePlacesPage key={JSON.stringify([a, b])} a={a} b={b} />;
}
