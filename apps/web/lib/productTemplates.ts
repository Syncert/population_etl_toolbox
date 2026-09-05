// First-wave product templates.
//
// A product template is navigation and presentation over stable API catalog
// identities — a curated reading order for measures the warehouse already
// publishes. It is not a new semantic layer: it computes nothing, combines
// nothing, and defines no measure of its own.
//
// Two rules keep a template honest:
//
// 1. Every slot names candidate metric codes explicitly and resolves to the
//    first one the live catalog actually publishes. The resolved identity is
//    always displayed, so a reader can see exactly which measure answered
//    rather than trusting a label the template chose. A slot no candidate
//    satisfies reports what it looked for; it never falls back to a
//    "similar" measure, which would silently answer a different question.
// 2. A slot that cannot be filled leaves a gap, stated. Partial source
//    coverage is normal — CDC publishes no county series for every place,
//    NASS suppresses small-cell values — and a profile that quietly dropped
//    those slots would read as though the place had no such conditions.

import type { MetricSummary } from "./api/types";

export interface TemplateMeasure {
  /** Stable slot identity, for URL state and saved configurations. */
  id: string;
  /** What this slot is for, in the reader's terms. Never a measure name. */
  label: string;
  /**
   * Catalog identities that can fill this slot, in preference order. The
   * first one the catalog publishes wins, and its own published display
   * name and source travel with it.
   */
  candidates: string[];
  /** Why this slot belongs in the product, for the reader. */
  note?: string;
}

export interface TemplateSection {
  id: string;
  title: string;
  description: string;
  measures: TemplateMeasure[];
}

export interface ProductTemplate {
  id: string;
  title: string;
  summary: string;
  /** What the product deliberately does not claim. */
  limits: string;
  sections: TemplateSection[];
}

/**
 * The three first-wave products. Each is configuration: adding a measure or
 * a section is an edit here, not a new component.
 */
export const PRODUCT_TEMPLATES: ProductTemplate[] = [
  {
    id: "community-conditions",
    title: "Community conditions profile",
    summary:
      "Population, labor, health, safety, and rural context for one place, each measure shown with its own source, period, and caveats.",
    limits:
      "Every measure stands on its own. Nothing here is combined into a score, an index, or a ranking, and no measure explains another.",
    sections: [
      {
        id: "population",
        title: "Population",
        description: "Who lives here, as the Census Bureau publishes it.",
        measures: [
          {
            id: "total-population",
            label: "Total population",
            candidates: ["ACS:acs5:B01003_001", "ACS:acs1:B01003_001"],
            note: "American Community Survey estimate, with its own margin of error.",
          },
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: [
              "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
              "CENSUS_PEP:pep_nst_alldata:POPESTIMATE",
            ],
            note: "Population Estimates Program vintage, a different method from the ACS survey estimate above.",
          },
        ],
      },
      {
        id: "labor",
        title: "Labor market",
        description: "Work and earnings context from the Bureau of Labor Statistics.",
        measures: [
          {
            id: "unemployment-rate",
            label: "Unemployment rate",
            candidates: ["BLS:LAU:UNEMP_RATE"],
            note: "Household-survey based; not the same universe as payroll employment.",
          },
          {
            id: "median-household-income",
            label: "Median household income",
            candidates: ["ACS:acs5:B19013_001", "ACS:acs1:B19013_001"],
          },
        ],
      },
      {
        id: "health",
        title: "Health and illness",
        description: "Health context from the Centers for Disease Control and Prevention.",
        measures: [
          {
            id: "cdc-indicator",
            label: "Chronic disease indicator",
            candidates: ["CDC:cdi:ALC1_1:crude"],
            note: "CDC publishes stratified measures; the explorer shows every published stratum.",
          },
        ],
      },
      {
        id: "safety",
        title: "Safety",
        description:
          "Reported crime from the FBI Uniform Crime Reporting Program, bounded by which agencies reported.",
        measures: [
          {
            id: "violent-crime",
            label: "Violent crime",
            candidates: ["FBI_UCR:summarized_violent_crime:actual"],
            note: "A period no agency reported is not zero crime; the explorer shows the participation context.",
          },
        ],
      },
      {
        id: "rural",
        title: "Rural and agricultural",
        description: "Agricultural context from USDA NASS, subject to its disclosure suppression.",
        measures: [
          {
            id: "nass-commodity",
            label: "Crop survey measure",
            candidates: ["USDA_NASS:corn_survey_annual:41"],
            note: "NASS suppresses small-cell values; a suppressed value is not a zero harvest.",
          },
        ],
      },
    ],
  },
  {
    id: "population-growth",
    title: "Population growth and service demand",
    summary:
      "Population estimates and change alongside the household, housing, and demographic context a service-demand discussion needs.",
    limits:
      "Estimates and survey values come from different methods and vintages and are shown separately. No projection is made; nothing here forecasts future demand.",
    sections: [
      {
        id: "estimates",
        title: "Population estimates",
        description:
          "Population Estimates Program values, whose vintage and method differ from the survey estimates below.",
        measures: [
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: [
              "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
              "CENSUS_PEP:pep_nst_alldata:POPESTIMATE",
            ],
          },
        ],
      },
      {
        id: "survey",
        title: "Survey context",
        description: "American Community Survey estimates, each with its own margin of error.",
        measures: [
          {
            id: "total-population",
            label: "Total population",
            candidates: ["ACS:acs5:B01003_001", "ACS:acs1:B01003_001"],
          },
          {
            id: "households",
            label: "Households",
            candidates: ["ACS:acs5:B11001_001"],
          },
          {
            id: "housing-units",
            label: "Housing units",
            candidates: ["ACS:acs5:B25001_001"],
          },
        ],
      },
    ],
  },
  {
    id: "workforce",
    title: "Workforce availability and labor-market depth",
    summary:
      "Labor force, employment, education, and population measures kept in their own survey universes.",
    limits:
      "Household-survey and establishment-survey measures, jobs and employed people, and counts and rates are distinct and are never combined here.",
    sections: [
      {
        id: "labor-force",
        title: "Labor force",
        description: "Bureau of Labor Statistics measures, on their own published frequency.",
        measures: [
          {
            id: "unemployment-rate",
            label: "Unemployment rate",
            candidates: ["BLS:LAU:UNEMP_RATE"],
          },
          {
            id: "participation",
            label: "Labor force participation",
            candidates: ["FRED:CIVPART"],
            note: "A national series from FRED; it does not describe this place on its own.",
          },
        ],
      },
      {
        id: "population-base",
        title: "Population base",
        description: "The population context a labor-market read depends on.",
        measures: [
          {
            id: "total-population",
            label: "Total population",
            candidates: ["ACS:acs5:B01003_001"],
          },
          {
            id: "educational-attainment",
            label: "Educational attainment",
            candidates: ["ACS:acs5:B15003_001"],
          },
        ],
      },
    ],
  },
];

export const DEFAULT_TEMPLATE_ID = PRODUCT_TEMPLATES[0]!.id;

export function findTemplate(id: string | null | undefined): ProductTemplate | null {
  if (!id) {
    return null;
  }
  return PRODUCT_TEMPLATES.find((template) => template.id === id) || null;
}

export interface ResolvedMeasure {
  slot: TemplateMeasure;
  /** The published catalog row that filled the slot, or `null`. */
  metric: MetricSummary | null;
  /** The identity that answered, so the reader sees what they are reading. */
  metricCode: string;
  available: boolean;
  /** When unavailable, what the template looked for and did not find. */
  reason: string;
}

export interface ResolvedSection {
  section: TemplateSection;
  measures: ResolvedMeasure[];
}

/**
 * Fill each slot from the published catalog.
 *
 * The index is keyed by the catalog's own `metric_code`, so a slot is filled
 * only by an identity the API published. Nothing is matched by name,
 * because two sources can publish very different measures under similar
 * labels.
 */
export function resolveTemplate(
  template: ProductTemplate | null | undefined,
  metricsByCode: Map<string, MetricSummary> | null | undefined,
): ResolvedSection[] {
  if (!template) {
    return [];
  }
  const index = metricsByCode || new Map<string, MetricSummary>();

  return template.sections.map((section) => ({
    section,
    measures: section.measures.map((slot) => {
      const found = slot.candidates.find((code) => index.has(code));
      if (!found) {
        return {
          slot,
          metric: null,
          metricCode: "",
          available: false,
          reason: `no published measure for this slot (looked for ${slot.candidates.join(", ")})`,
        };
      }
      return {
        slot,
        metric: index.get(found) || null,
        metricCode: found,
        available: true,
        reason: "",
      };
    }),
  }));
}

export interface TemplateCoverage {
  requested: number;
  available: number;
  unavailable: number;
}

/**
 * How much of the product the catalog can actually fill.
 *
 * Reported rather than hidden: a profile that quietly dropped its empty
 * slots would read as though the place had no such conditions, when the
 * truth is that this warehouse publishes no such measure for it.
 */
export function templateCoverage(sections: ResolvedSection[] | null | undefined): TemplateCoverage {
  const measures = (sections || []).flatMap((entry) => entry.measures);
  const available = measures.filter((measure) => measure.available).length;
  return {
    requested: measures.length,
    available,
    unavailable: measures.length - available,
  };
}

/** Every candidate identity a template could ask the catalog for. */
export function templateMetricCodes(
  template: ProductTemplate | null | undefined,
): string[] {
  if (!template) {
    return [];
  }
  return [
    ...new Set(
      template.sections.flatMap((section) =>
        section.measures.flatMap((measure) => measure.candidates),
      ),
    ),
  ];
}
