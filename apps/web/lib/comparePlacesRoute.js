// The compare-two-places routes share one rendering (compare-two-places):
// `/us/<a>[/<county>]/vs/us/<b>[/<county>]`, where either side may be the
// nation (`/us/texas/vs/us`, `/us/vs/us/texas`) and a county segment may
// name a city or town, as on the place page. A pair with a metro, which has
// no page yet, is `/compare/places?a=<geo_id>&b=<geo_id>`
// (web-cross-level-compare). Segments or identities outside the place
// vocabulary are a 404 before anything is read.
import { notFound } from "next/navigation";

import ComparePlacesPage from "../components/ComparePlacesPage";
import PlaceFinder from "../components/PlaceFinder";
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
  // The bare address names no pair: it starts a comparison instead of
  // answering "not found" (website UX review F6).
  if (searchParams.a === undefined && searchParams.b === undefined) return null;
  if (!COMPARE_GEO_ID.test(a) || !COMPARE_GEO_ID.test(b)) notFound();
  return { a: { geoId: a }, b: { geoId: b } };
}

function ComparePlacesStart() {
  return (
    <main className="page-shell compact-page" data-testid="compare-places-start">
      <div className="page-heading">
        <div className="section-kicker">Compare</div>
        <h1>Compare two places</h1>
        <p>
          Find the first place below. Its page has a line that reads
          &ldquo;Compare this place with&rdquo;, and each nearby place has a
          Compare link.
        </p>
      </div>
      <PlaceFinder />
    </main>
  );
}

export async function compareIdentitiesMetadata() {
  return { title: "Compare two places" };
}

export async function CompareIdentitiesRoute({ searchParams }) {
  const pair = identities(await searchParams);
  if (!pair) return <ComparePlacesStart />;
  const { a, b } = pair;
  return <ComparePlacesPage key={JSON.stringify([a, b])} a={a} b={b} />;
}
