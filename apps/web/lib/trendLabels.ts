// What a trend chart says about its series (UAT, metro pages): how often it is
// published and who publishes it, in words. Kept out of `placeChapters` so the
// words load with the chart (`PlaceTrend`), not with every place page.

import type { TrendModel } from "./placeChapters";

const DAY_MS = 86_400_000;

function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)]!;
}

export type TrendFrequency = "monthly" | "quarterly" | "yearly" | "five-year estimates" | "multi-year" | "";

/** A period's usual length in days, in words; "" when it is not known. */
export function frequencyOf(days: number | null | undefined): TrendFrequency {
  if (days === null || days === undefined || !Number.isFinite(days)) return "";
  if (days <= 35) return "monthly";
  if (days <= 100) return "quarterly";
  if (days <= 370) return "yearly";
  if (days >= 1700 && days <= 1900) return "five-year estimates";
  return "multi-year";
}

/**
 * How often a drawn series is published, from the page's own line (or the
 * first line drawn): each point's own span from period start to end, or,
 * where points carry only a date, the usual gap between them.
 */
export function trendFrequency(model: TrendModel): TrendFrequency {
  const points = model.lines.find((line) => line.points.length)?.points || [];
  const spans = points.filter((point) => point.end >= point.time).map((point) => (point.end - point.time) / DAY_MS + 1);
  const times = points.map((point) => point.time);
  return frequencyOf(median(spans) ?? median(times.slice(1).map((time, index) => (time - times[index]!) / DAY_MS)));
}

/** Who publishes a series, in plain words, by its metric code's program. */
const TREND_SOURCES: readonly (readonly [string, string])[] = [
  ["CENSUS_ACS:acs5:", "U.S. Census Bureau, American Community Survey 5-year estimates"],
  ["CENSUS_ACS:acs1:", "U.S. Census Bureau, American Community Survey 1-year estimates"],
  ["CENSUS_ACS:", "U.S. Census Bureau, American Community Survey"],
  ["CENSUS_PEP:", "U.S. Census Bureau, Population Estimates Program"],
  ["CENSUS_SAIPE_SAHIE:saipe:", "U.S. Census Bureau, Small Area Income and Poverty Estimates (SAIPE)"],
  ["CENSUS_SAIPE_SAHIE:sahie:", "U.S. Census Bureau, Small Area Health Insurance Estimates (SAHIE)"],
  ["CENSUS_BPS:", "U.S. Census Bureau, Building Permits Survey"],
  ["BLS:LAU:", "Bureau of Labor Statistics, Local Area Unemployment Statistics (LAUS)"],
  ["BLS_QCEW:", "Bureau of Labor Statistics, Quarterly Census of Employment and Wages (QCEW)"],
  ["BLS:", "Bureau of Labor Statistics"],
  ["BEA:", "Bureau of Economic Analysis, regional economic accounts"],
  ["CDC:", "Centers for Disease Control and Prevention, PLACES model-based estimates"],
  ["FBI_UCR:", "FBI Uniform Crime Reporting Program"],
  ["USDA_NASS:", "USDA National Agricultural Statistics Service"],
];

/** The source line a trend chart states: the program in words, else its code. */
export function trendSourceName(metricCode: string | null | undefined, sourceCode?: string | null): string {
  const code = String(metricCode || "");
  const match = TREND_SOURCES.find(([prefix]) => code.startsWith(prefix));
  return match ? match[1] : String(sourceCode || "");
}
