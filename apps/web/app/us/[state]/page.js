import { notFound } from "next/navigation";

import PlacePage from "../../../components/PlacePage";
import { PLACE_SEGMENT } from "../../../lib/placeChapters";
import { placeRouteTitle } from "../../../lib/routeTitles";

// The segment is checked here, so an address no catalog row could ever
// answer is a real 404. A well-formed segment is resolved against the
// geography catalog in the browser, which states its own not-found.
export async function generateMetadata({ params }) {
  const { state } = await params;
  if (!PLACE_SEGMENT.test(state)) notFound();
  return { title: placeRouteTitle(state) };
}

export default async function StatePage({ params }) {
  const { state } = await params;
  if (!PLACE_SEGMENT.test(state)) notFound();
  return <PlacePage key={state} stateSegment={state} />;
}
