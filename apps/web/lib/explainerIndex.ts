// Which explainer answers which caveat, for client components.
//
// A browser bundle cannot read `content/explainers/`, so the slugs, titles
// and caveat keys are listed here once more. `tests/frontend/unit/explainers.
// test.js` compares this list with the files, so the two cannot drift, and
// proves every caveat a chart surface declares resolves to an entry.

export interface ExplainerEntry {
  slug: string;
  title: string;
  caveatKeys: readonly string[];
}

export const EXPLAINER_INDEX: readonly ExplainerEntry[] = [
  { slug: "cpi-is-not-local", title: "Why is the CPI not your local cost of living?", caveatKeys: ["cpi-not-local"] },
  { slug: "five-year-estimates", title: "What is a 5-year estimate?", caveatKeys: ["five-year-estimate"] },
  { slug: "jobs-versus-employed", title: "Why do jobs and employed people differ?", caveatKeys: ["jobs-versus-employed"] },
  { slug: "margin-of-error", title: "Why does a survey estimate have a margin of error?", caveatKeys: ["margin-of-error"] },
  { slug: "missing-crime-reports", title: "Why is a missing crime report not zero crime?", caveatKeys: ["missing-crime-reports"] },
  { slug: "modeled-prevalence", title: "Why is a modeled prevalence not a case count?", caveatKeys: ["modeled-prevalence"] },
  { slug: "peer-percentile", title: "What does a percentile rank among peers mean?", caveatKeys: ["peer-percentile"] },
  { slug: "population-estimates-versus-survey", title: "Why do the population estimate and the survey disagree?", caveatKeys: ["pep-versus-acs"] },
  { slug: "revisions", title: "What is a revision, and why do numbers change?", caveatKeys: ["revisions"] },
  { slug: "suppressed-values", title: "What does a suppressed value mean?", caveatKeys: ["suppressed-cell"] },
  { slug: "unemployment-rate", title: "What does the unemployment rate count?", caveatKeys: ["unemployment-rate"] },
  { slug: "vintages", title: "What is a vintage?", caveatKeys: ["vintage"] },
];

/** The explainer that answers a caveat, or null: no link is better than a dead one. */
export function explainerForCaveat(key: string | null | undefined): ExplainerEntry | null {
  if (!key) return null;
  return EXPLAINER_INDEX.find((entry) => entry.caveatKeys.includes(key)) || null;
}

export function explainerHref(slug: string): string {
  return `/explain/${slug}`;
}
