import { notFound } from "next/navigation";

import AgeMapPage from "../../../../components/AgeMapPage";
import { ageMapIndicator } from "../../../../lib/ageMap";
import { ageMapTitle } from "../../../../lib/routeTitles";

// `/map/age/<indicator>`: one age indicator from `/age-structure/map`
// (web-age-and-sex-structure). Only the indicators the API serves are
// pages; anything else is not found.
export async function generateMetadata({ params }) {
  const indicator = ageMapIndicator((await params).indicator);
  if (!indicator) notFound();
  return { title: ageMapTitle(indicator.label) };
}

export default async function AgeMapRoute({ params, searchParams }) {
  const indicator = ageMapIndicator((await params).indicator);
  if (!indicator) notFound();
  const { grain, state } = await searchParams;
  const requestedGrain = typeof grain === "string" && /^[A-Za-z_]{1,20}$/.test(grain) ? grain.toUpperCase() : undefined;
  const requestedState = typeof state === "string" && /^\d{2}$/.test(state) ? state : undefined;
  return <AgeMapPage key={indicator.id} indicator={indicator} requestedGrain={requestedGrain} requestedState={requestedState} />;
}
