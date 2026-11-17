import { notFound } from "next/navigation";

import PlacePage from "../../../components/PlacePage";
import { PLACE_SEGMENT } from "../../../lib/placeChapters";
import { placeAddressIsUnknown } from "../../../lib/placeExists";
import { placeRouteTitle } from "../../../lib/routeTitles";

// The segment is checked here, so an address no catalog row could ever
// answer is a real 404. A well-formed segment is asked of the geography
// catalog from the server: a state the catalog does not publish is a 404
// too (website UX review F6). Where the server cannot ask, the browser
// resolves the address and states its own not-found.
export async function generateMetadata({ params }) {
  const { state } = await params;
  if (!PLACE_SEGMENT.test(state)) notFound();
  return { title: placeRouteTitle(state) };
}

export default async function StatePage({ params }) {
  const { state } = await params;
  if (!PLACE_SEGMENT.test(state)) notFound();
  if (await placeAddressIsUnknown(state)) notFound();
  return <PlacePage key={state} stateSegment={state} />;
}
