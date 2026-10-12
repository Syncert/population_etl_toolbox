import { notFound } from "next/navigation";

import MeasureMapPage from "../../../components/MeasureMapPage";
import { measureMapTitle } from "../../../lib/routeTitles";

// A catalog identity is letters, digits and `:_.-` (CENSUS_ACS:acs5:B19013_001,
// USDA_NASS:corn_survey_annual:<sha>). Anything else cannot name a measure.
const METRIC_SEGMENT = /^[A-Za-z0-9_.:-]{1,200}$/;

function metricFrom(raw) {
  let decoded;
  try {
    decoded = decodeURIComponent(raw);
  } catch {
    return null;
  }
  return METRIC_SEGMENT.test(decoded) ? decoded : null;
}

export async function generateMetadata({ params }) {
  const metric = metricFrom((await params).metric);
  if (!metric) notFound();
  return { title: measureMapTitle(metric) };
}

export default async function MeasureMapRoute({ params, searchParams }) {
  const metric = metricFrom((await params).metric);
  if (!metric) notFound();
  const period = (await searchParams).period;
  const requested = typeof period === "string" && /^\d{4}-\d{2}-\d{2}$/.test(period) ? period : undefined;
  return <MeasureMapPage key={metric} metricCode={metric} requestedPeriod={requested} />;
}
