// The compare-two-places routes share one rendering (compare-two-places):
// `/us/<a>[/<county>]/vs/us/<b>[/<county>]`, where either side may be the
// nation (`/us/texas/vs/us`, `/us/vs/us/texas`) and a county segment may
// name a city or town, as on the place page. A pair with a metro, which has
// no page yet, is `/compare/places?a=<geo_id>&b=<geo_id>`
// (web-cross-level-compare). Segments or identities outside the place
// vocabulary are a 404 before anything is read.
import { notFound } from "next/navigation";

import ComparePlacesPage from "../components/ComparePlacesPage";
import { PLACE_SEGMENT } from "./placeChapters";
import { COMPARE_GEO_ID } from "./placeComparison";
import { placeRouteTitle } from "./routeTitles";

function side(state, county) {
  return state ? { state, ...(county ? { county } : {}) } : {};
}

function sides(params) {
  const segments = [params.state, params.county, params.otherState, params.otherCounty].filter(Boolean);
  if (!segments.every((segment) => PLACE_SEGMENT.test(segment))) notFound();
  return { a: side(params.state, params.county), b: side(params.otherState, params.otherCounty) };
}

export async function compareMetadata({ params }) {
  const { a, b } = sides(await params);
  return { title: `${placeRouteTitle(a.state, a.county)} and ${placeRouteTitle(b.state, b.county)}` };
}

export async function ComparePlacesRoute({ params }) {
  const { a, b } = sides(await params);
  return <ComparePlacesPage key={JSON.stringify([a, b])} a={a} b={b} />;
}

function identities(searchParams) {
  const a = typeof searchParams.a === "string" ? searchParams.a : "";
  const b = typeof searchParams.b === "string" ? searchParams.b : "";
  if (!COMPARE_GEO_ID.test(a) || !COMPARE_GEO_ID.test(b)) notFound();
  return { a: { geoId: a }, b: { geoId: b } };
}

export async function compareIdentitiesMetadata() {
  return { title: "Compare two places" };
}

export async function CompareIdentitiesRoute({ searchParams }) {
  const { a, b } = identities(await searchParams);
  return <ComparePlacesPage key={JSON.stringify([a, b])} a={a} b={b} />;
}
