import { notFound } from "next/navigation";

import PlacePage from "../../../components/PlacePage";
import { PLACE_SEGMENT } from "../../../lib/placeChapters";
import { metroRouteTitle } from "../../../lib/routeTitles";

// A metro or micro area's page (web-metro-pages): `/metro/<segment>`, the
// slug of its published name or its five-digit CBSA code. The page resolves
// the segment through the API and settles on the canonical one.
export async function generateMetadata({ params }) {
  const { metro } = await params;
  if (!PLACE_SEGMENT.test(metro)) notFound();
  return { title: metroRouteTitle(metro) };
}

export default async function MetroPage({ params }) {
  const { metro } = await params;
  if (!PLACE_SEGMENT.test(metro)) notFound();
  return <PlacePage key={`metro/${metro}`} metroSegment={metro} />;
}
