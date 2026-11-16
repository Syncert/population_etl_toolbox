import { notFound } from "next/navigation";

import PlacePage from "../../../../components/PlacePage";
import { PLACE_SEGMENT } from "../../../../lib/placeChapters";
import { placeAddressIsUnknown } from "../../../../lib/placeExists";
import { placeRouteTitle } from "../../../../lib/routeTitles";

export async function generateMetadata({ params }) {
  const { state, county } = await params;
  if (!PLACE_SEGMENT.test(state) || !PLACE_SEGMENT.test(county)) notFound();
  return { title: placeRouteTitle(state, county) };
}

export default async function CountyPage({ params }) {
  const { state, county } = await params;
  if (!PLACE_SEGMENT.test(state) || !PLACE_SEGMENT.test(county)) notFound();
  // A county, city or town the catalog does not publish is a real 404.
  if (await placeAddressIsUnknown(state, county)) notFound();
  return <PlacePage key={`${state}/${county}`} stateSegment={state} countySegment={county} />;
}
