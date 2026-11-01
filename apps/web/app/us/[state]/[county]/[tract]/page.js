import { notFound } from "next/navigation";

import TractPage from "../../../../../components/TractPage";
import { PLACE_SEGMENT } from "../../../../../lib/placeChapters";
import { tractRouteTitle } from "../../../../../lib/routeTitles";
import { TRACT_SEGMENT } from "../../../../../lib/tractPage";

function accepted(state, county, tract) {
  return PLACE_SEGMENT.test(state) && PLACE_SEGMENT.test(county) && TRACT_SEGMENT.test(tract);
}

export async function generateMetadata({ params }) {
  const { state, county, tract } = await params;
  if (!accepted(state, county, tract)) notFound();
  return { title: tractRouteTitle(state, county, tract) };
}

export default async function CensusTractPage({ params }) {
  const { state, county, tract } = await params;
  if (!accepted(state, county, tract)) notFound();
  return <TractPage key={`${state}/${county}/${tract}`} stateSegment={state} countySegment={county} tractSegment={tract} />;
}
