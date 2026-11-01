import { notFound, redirect } from "next/navigation";

import PlacePage from "../../../components/PlacePage";
import { PLACE_SEGMENT } from "../../../lib/placeChapters";
import { metroRouteTitle } from "../../../lib/routeTitles";
import { canonicalMetroPath } from "../../../lib/metroRedirect";

// A metro or micro area's page (web-metro-pages): `/metro/<segment>`, the
// slug of its published name or its five-digit CBSA code. A code is answered
// with a redirect to the named page, decided on the server, so a link built
// from a `cbsa:` id (a city's "Part of", a map row) lands on one address.
// Where the server cannot ask the API, the page resolves the segment itself
// and settles on the canonical one in the browser.
export async function generateMetadata({ params }) {
  const { metro } = await params;
  if (!PLACE_SEGMENT.test(metro)) notFound();
  return { title: metroRouteTitle(metro) };
}

export default async function MetroPage({ params }) {
  const { metro } = await params;
  if (!PLACE_SEGMENT.test(metro)) notFound();
  const canonical = await canonicalMetroPath(metro);
  if (canonical) redirect(canonical);
  return <PlacePage key={`metro/${metro}`} metroSegment={metro} />;
}
