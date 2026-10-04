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
import type { ObservationRow } from "./explorerViewModel";
import { observationUnit } from "./explorerViewModel";
import { displayMetricName } from "./format";
import {
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_FIELDS,
  observationCoverageValue,
  observationPeriodLabel,
  observationUncertaintyValue,
} from "./observationAccess";

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

// Identity is published by gold_cdc.metric_publisher; the county PLACES
// parser and replay fixtures verify this measure and its age-adjustment type.
const COUNTY_ARTHRITIS: TemplateMeasure = {
  id: "county-arthritis",
  label: "County arthritis prevalence (age-adjusted)",
  candidates: ["CDC:places_county:ARTHRITIS:AgeAdjPrv"],
  note: "CDC PLACES modeled adult prevalence, with its published interval. It is distinct from state CDI surveillance and from a clinical count.",
};

// The other thirty-nine county PLACES measures, enumerated from the live
// catalog on 2026-10-04: gold_cdc publishes every measure of the PLACES 2025
// county release, each as an age-adjusted and a crude prevalence. These slots
// name the age-adjusted measure and say so in the label; the crude companion
// remains its own published measure in the catalog, reachable through the
// explorer, and is never converted into the other here.
const placesCounty = (
  id: string,
  label: string,
  candidate: string,
): TemplateMeasure => ({
  id,
  label: `${label} (age-adjusted)`,
  candidates: [candidate],
});

const PLACES_CONDITIONS: TemplateMeasure[] = [
  placesCounty("places-bphigh", "High blood pressure", "CDC:places_county:BPHIGH:AgeAdjPrv"),
  placesCounty("places-cancer", "Cancer (non-skin) or melanoma", "CDC:places_county:CANCER:AgeAdjPrv"),
  placesCounty("places-casthma", "Current asthma", "CDC:places_county:CASTHMA:AgeAdjPrv"),
  placesCounty("places-chd", "Coronary heart disease", "CDC:places_county:CHD:AgeAdjPrv"),
  placesCounty("places-copd", "Chronic obstructive pulmonary disease", "CDC:places_county:COPD:AgeAdjPrv"),
  placesCounty("places-depression", "Depression", "CDC:places_county:DEPRESSION:AgeAdjPrv"),
  placesCounty("places-diabetes", "Diagnosed diabetes", "CDC:places_county:DIABETES:AgeAdjPrv"),
  placesCounty("places-highchol", "High cholesterol (screened adults)", "CDC:places_county:HIGHCHOL:AgeAdjPrv"),
  placesCounty("places-obesity", "Obesity", "CDC:places_county:OBESITY:AgeAdjPrv"),
  placesCounty("places-stroke", "Stroke", "CDC:places_county:STROKE:AgeAdjPrv"),
  placesCounty("places-teethlost", "All teeth lost (adults 65+)", "CDC:places_county:TEETHLOST:AgeAdjPrv"),
];

const PLACES_STATUS_BEHAVIORS: TemplateMeasure[] = [
  placesCounty("places-ghlth", "Fair or poor self-rated health", "CDC:places_county:GHLTH:AgeAdjPrv"),
  placesCounty("places-mhlth", "Frequent mental distress", "CDC:places_county:MHLTH:AgeAdjPrv"),
  placesCounty("places-phlth", "Frequent physical distress", "CDC:places_county:PHLTH:AgeAdjPrv"),
  placesCounty("places-sleep", "Short sleep duration", "CDC:places_county:SLEEP:AgeAdjPrv"),
  placesCounty("places-binge", "Binge drinking", "CDC:places_county:BINGE:AgeAdjPrv"),
  placesCounty("places-csmoking", "Current cigarette smoking", "CDC:places_county:CSMOKING:AgeAdjPrv"),
  placesCounty("places-lpa", "No leisure-time physical activity", "CDC:places_county:LPA:AgeAdjPrv"),
];

const PLACES_PREVENTION: TemplateMeasure[] = [
  placesCounty("places-access2", "No health insurance (adults 18-64)", "CDC:places_county:ACCESS2:AgeAdjPrv"),
  placesCounty("places-bpmed", "Blood pressure medicine use", "CDC:places_county:BPMED:AgeAdjPrv"),
  placesCounty("places-checkup", "Routine checkup in the past year", "CDC:places_county:CHECKUP:AgeAdjPrv"),
  placesCounty("places-cholscreen", "Cholesterol screening", "CDC:places_county:CHOLSCREEN:AgeAdjPrv"),
  placesCounty("places-colon-screen", "Colorectal cancer screening (ages 45-75)", "CDC:places_county:COLON_SCREEN:AgeAdjPrv"),
  placesCounty("places-dental", "Dental visit in the past year", "CDC:places_county:DENTAL:AgeAdjPrv"),
  placesCounty("places-mammouse", "Mammography use (women 50-74)", "CDC:places_county:MAMMOUSE:AgeAdjPrv"),
];

const PLACES_DISABILITY: TemplateMeasure[] = [
  placesCounty("places-cognition", "Cognitive disability", "CDC:places_county:COGNITION:AgeAdjPrv"),
  placesCounty("places-disability", "Any disability", "CDC:places_county:DISABILITY:AgeAdjPrv"),
  placesCounty("places-hearing", "Hearing disability", "CDC:places_county:HEARING:AgeAdjPrv"),
  placesCounty("places-indeplive", "Independent living disability", "CDC:places_county:INDEPLIVE:AgeAdjPrv"),
  placesCounty("places-mobility", "Mobility disability", "CDC:places_county:MOBILITY:AgeAdjPrv"),
  placesCounty("places-selfcare", "Self-care disability", "CDC:places_county:SELFCARE:AgeAdjPrv"),
  placesCounty("places-vision", "Vision disability", "CDC:places_county:VISION:AgeAdjPrv"),
];

const PLACES_SOCIAL_NEEDS: TemplateMeasure[] = [
  placesCounty("places-emotionspt", "Lacks social and emotional support", "CDC:places_county:EMOTIONSPT:AgeAdjPrv"),
  placesCounty("places-foodinsecu", "Food insecurity (past 12 months)", "CDC:places_county:FOODINSECU:AgeAdjPrv"),
  placesCounty("places-foodstamp", "Received food stamps (past 12 months)", "CDC:places_county:FOODSTAMP:AgeAdjPrv"),
  placesCounty("places-housinsecu", "Housing insecurity (past 12 months)", "CDC:places_county:HOUSINSECU:AgeAdjPrv"),
  placesCounty("places-lacktrpt", "Lacks reliable transportation", "CDC:places_county:LACKTRPT:AgeAdjPrv"),
  placesCounty("places-loneliness", "Loneliness", "CDC:places_county:LONELINESS:AgeAdjPrv"),
  placesCounty("places-shututility", "Utility shut-off threat (past 12 months)", "CDC:places_county:SHUTUTILITY:AgeAdjPrv"),
];

// The shared caveat each PLACES section states beside its measures.
const PLACES_SECTION_NOTE =
  "CDC PLACES model-based county estimates among adults, each with its published confidence interval. These are not clinical counts, and the crude-prevalence companion of each measure stays a separate published measure.";

/**
 * The eight packaged products, first wave then second. Each is configuration:
 * adding a measure or a section is an edit here, not a new component, and
 * `/profiles` lists whatever this array holds.
 *
 * Every candidate code below was resolved against a deployed warehouse's
 * publisher views before it was written down, because a candidate nobody
 * checked is a slot that reports a gap forever and reads as though the place
 * had no such measure. The codes for sources a given deployment does not carry
 * still appear -- a template is not rewritten per deployment -- and those slots
 * state their gap, which is the designed behaviour rather than a defect.
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
            candidates: [
              "CENSUS_ACS:acs5:B01003_001",
              "CENSUS_ACS:acs1:B01003_001",
            ],
            note: "American Community Survey estimate, with its own margin of error.",
          },
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: ["CENSUS_PEP:POPESTIMATE"],
            note: "Population Estimates Program vintage, a different method from the ACS survey estimate above.",
          },
        ],
      },
      {
        id: "labor",
        title: "Labor market",
        description:
          "Work and earnings context from the Bureau of Labor Statistics.",
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
            candidates: [
              "CENSUS_ACS:acs5:B19013_001",
              "CENSUS_ACS:acs1:B19013_001",
            ],
          },
        ],
      },
      {
        id: "health",
        title: "Health and illness",
        description:
          "Health context from the Centers for Disease Control and Prevention. " +
          PLACES_SECTION_NOTE +
          " The dedicated disease and illness page carries every published county measure.",
        measures: [
          COUNTY_ARTHRITIS,
          {
            id: "cdc-indicator",
            label: "State chronic disease indicator",
            candidates: ["CDC:cdi:ALC06:AGEADJPREV", "CDC:cdi:ALC06:CRDPREV"],
            note: "CDC publishes stratified measures; the explorer shows every published stratum.",
          },
          placesCounty("places-obesity", "Obesity", "CDC:places_county:OBESITY:AgeAdjPrv"),
          placesCounty("places-diabetes", "Diagnosed diabetes", "CDC:places_county:DIABETES:AgeAdjPrv"),
          placesCounty("places-depression", "Depression", "CDC:places_county:DEPRESSION:AgeAdjPrv"),
          placesCounty("places-bphigh", "High blood pressure", "CDC:places_county:BPHIGH:AgeAdjPrv"),
          placesCounty("places-csmoking", "Current cigarette smoking", "CDC:places_county:CSMOKING:AgeAdjPrv"),
          placesCounty("places-mhlth", "Frequent mental distress", "CDC:places_county:MHLTH:AgeAdjPrv"),
          placesCounty("places-access2", "No health insurance (adults 18-64)", "CDC:places_county:ACCESS2:AgeAdjPrv"),
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
            candidates: [
              "FBI_UCR:summarized_violent_crime:V:offense:absolute_total",
            ],
            note: "A period no agency reported is not zero crime; the explorer shows the participation context.",
          },
        ],
      },
      {
        id: "rural",
        title: "Rural and agricultural",
        description:
          "Agricultural context from USDA NASS, subject to its disclosure suppression.",
        measures: [
          {
            id: "nass-commodity",
            label: "Crop survey measure",
            candidates: [
              "USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11",
            ],
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
            candidates: ["CENSUS_PEP:POPESTIMATE"],
          },
        ],
      },
      {
        id: "survey",
        title: "Survey context",
        description:
          "American Community Survey estimates, each with its own margin of error.",
        measures: [
          {
            id: "total-population",
            label: "Total population",
            candidates: [
              "CENSUS_ACS:acs5:B01003_001",
              "CENSUS_ACS:acs1:B01003_001",
            ],
          },
          {
            id: "households",
            label: "Households",
            candidates: ["CENSUS_ACS:acs5:B11001_001"],
          },
          {
            id: "housing-units",
            label: "Housing units",
            candidates: ["CENSUS_ACS:acs5:B25001_001"],
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
        description:
          "Bureau of Labor Statistics measures, on their own published frequency.",
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
            candidates: ["CENSUS_ACS:acs5:B01003_001"],
          },
          {
            id: "educational-attainment",
            label: "Adults aged 25+ (education table total)",
            candidates: ["CENSUS_ACS:acs5:B15003_001"],
          },
        ],
      },
    ],
  },
  {
    id: "housing-affordability",
    title: "Housing affordability and household pressure",
    summary:
      "Local rent, home value, cost burden, tenure, and income from the ACS, shown beside the national financial series that describe the borrowing environment they sit in.",
    limits:
      "The FRED series here are national and are labelled as such; nothing on this page is a local mortgage rate or a local price index. No affordability score, ratio, or index is computed - the cost measures and the income measures are published separately and are shown separately. A national rate moving beside a local value is not that rate acting on that place.",
    sections: [
      {
        id: "cost",
        title: "What housing costs here",
        description:
          "American Community Survey estimates for this place. Each carries its own margin of error, shown with the value.",
        measures: [
          {
            id: "median-gross-rent",
            label: "Median gross rent",
            candidates: [
              "CENSUS_ACS:acs5:B25064_001",
              "CENSUS_ACS:acs1:B25064_001",
            ],
            note: "Rent plus the utilities the renter pays, so it is not comparable to a contract rent figure.",
          },
          {
            id: "median-home-value",
            label: "Median home value",
            candidates: [
              "CENSUS_ACS:acs5:B25077_001",
              "CENSUS_ACS:acs1:B25077_001",
            ],
            note: "The owner's own estimate of value, which is what the ACS asks; it is not an assessed or sale price.",
          },
          {
            id: "median-monthly-housing-cost",
            label: "Median monthly housing cost",
            candidates: [
              "CENSUS_ACS:acs5:B25105_001",
              "CENSUS_ACS:acs1:B25105_001",
            ],
          },
        ],
      },
      {
        id: "pressure",
        title: "How far income goes",
        description:
          "Cost burden as the ACS publishes it, beside the income it is measured against. Both are published measures; neither is derived here.",
        measures: [
          {
            id: "rent-share-of-income",
            label: "Renter households in the rent-burden table",
            candidates: [
              "CENSUS_ACS:acs5:B25070_001",
              "CENSUS_ACS:acs1:B25070_001",
            ],
            note: "This is the table's total renter-household count, not a rent-to-income ratio or the number with high burden. Open the source table for its published bands.",
          },
          {
            id: "owner-cost-share-of-income",
            label: "Owner-occupied housing units in the cost-burden table",
            candidates: [
              "CENSUS_ACS:acs5:B25091_001",
              "CENSUS_ACS:acs1:B25091_001",
            ],
            note: "This is the total owner-occupied housing-unit universe, not a cost-to-income ratio. Mortgage status and burden bands are separate published variables.",
          },
          {
            id: "median-household-income",
            label: "Median household income",
            candidates: [
              "CENSUS_ACS:acs5:B19013_001",
              "CENSUS_ACS:acs1:B19013_001",
            ],
          },
        ],
      },
      {
        id: "stock",
        title: "The housing that exists",
        description: "Tenure, occupancy, and age of the stock.",
        measures: [
          {
            id: "tenure",
            label: "Occupied housing units (tenure table total)",
            candidates: [
              "CENSUS_ACS:acs5:B25003_001",
              "CENSUS_ACS:acs1:B25003_001",
            ],
          },
          {
            id: "occupancy",
            label: "Housing units (occupancy table total)",
            candidates: [
              "CENSUS_ACS:acs5:B25002_001",
              "CENSUS_ACS:acs1:B25002_001",
            ],
            note: "A vacant unit is not necessarily available: the ACS counts seasonal and other vacancies here too.",
          },
          {
            id: "year-built",
            label: "Housing units (year-built table total)",
            candidates: [
              "CENSUS_ACS:acs5:B25034_001",
              "CENSUS_ACS:acs1:B25034_001",
            ],
          },
        ],
      },
      {
        id: "national-context",
        title: "National financial context",
        description:
          "Federal Reserve series for the United States as a whole. These are national and describe the environment, not this place.",
        measures: [
          {
            id: "mortgage-rate",
            label: "30-year fixed mortgage rate (national)",
            candidates: ["FRED:MORTGAGE30US"],
            note: "A national weekly average. No local rate is published here, and this one does not describe local lending.",
          },
          {
            id: "new-house-price",
            label: "Median sales price, new houses sold (national)",
            candidates: ["FRED:MSPUS"],
            note: "New construction nationally, a different universe from the ACS home value above.",
          },
          {
            id: "consumer-prices",
            label: "Consumer Price Index, all items (national)",
            candidates: ["FRED:CPIAUCSL"],
          },
        ],
      },
    ],
  },
  {
    id: "aging-population",
    title: "Aging population and health-service planning",
    summary:
      "The size and shape of the older population here, with the living arrangement, disability, insurance, and income measures a service-planning discussion needs.",
    limits:
      "Age bands come from the tables that publish them and are not re-cut here; a band in one measure may not match a band in another, and no measure is rebased onto another's bands. ACS margins of error travel with every estimate. Nothing here is a projection, a need estimate, or a service recommendation.",
    sections: [
      {
        id: "how-many",
        title: "How many older residents",
        description: "Counts and central tendency, as published.",
        measures: [
          {
            id: "population-65-plus",
            label: "Population aged 65 and over",
            candidates: [
              "CENSUS_ACS:acs5:B09020_001",
              "CENSUS_ACS:acs1:B09020_001",
            ],
          },
          {
            id: "median-age",
            label: "Median age",
            candidates: [
              "CENSUS_ACS:acs5:B01002_001",
              "CENSUS_ACS:acs1:B01002_001",
            ],
          },
          {
            id: "age-distribution",
            label: "Population (age-and-sex table total)",
            candidates: [
              "CENSUS_ACS:acs5:B01001_001",
              "CENSUS_ACS:acs1:B01001_001",
            ],
            note: "The full distribution, so a band can be read rather than inferred from the median.",
          },
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: ["CENSUS_PEP:POPESTIMATE"],
            note: "A different method and vintage from the survey estimates above, shown beside them rather than merged with them.",
          },
        ],
      },
      {
        id: "circumstances",
        title: "Living arrangement and support",
        description:
          "Who older residents live with, and the disability and insurance context published for this place.",
        measures: [
          {
            id: "older-adults-in-households",
            label: "Older adults by living arrangement",
            candidates: [
              "CENSUS_ACS:acs5:B09020_002",
              "CENSUS_ACS:acs1:B09020_002",
            ],
            note: "Living alone and living in a family household are different planning situations and are published apart.",
          },
          {
            id: "disability-by-age",
            label: "Population (disability table total)",
            candidates: [
              "CENSUS_ACS:acs5:C18108_001",
              "CENSUS_ACS:acs1:C18108_001",
            ],
            note: "The ACS counts disabilities reported, not diagnoses or service eligibility.",
          },
          {
            id: "health-insurance-by-age",
            label: "Population (health-insurance table total)",
            candidates: [
              "CENSUS_ACS:acs5:B27010_001",
              "CENSUS_ACS:acs1:B27010_001",
            ],
          },
        ],
      },
      {
        id: "means",
        title: "Income context",
        description:
          "Household and per-person income for the place as a whole.",
        measures: [
          {
            id: "median-household-income",
            label: "Median household income",
            candidates: [
              "CENSUS_ACS:acs5:B19013_001",
              "CENSUS_ACS:acs1:B19013_001",
            ],
            note: "For all households here, not only those with an older member.",
          },
          {
            id: "per-capita-income",
            label: "Per capita income",
            candidates: [
              "CENSUS_ACS:acs5:B19301_001",
              "CENSUS_ACS:acs1:B19301_001",
            ],
          },
        ],
      },
      {
        id: "health-measures",
        title: "Published health measures",
        description:
          "Chronic-condition indicators as the CDC publishes them, with their own stratification. " +
          PLACES_SECTION_NOTE,
        measures: [
          COUNTY_ARTHRITIS,
          {
            id: "cdc-chronic-indicator",
            label: "Chronic disease indicator",
            candidates: ["CDC:cdi:ALC06:AGEADJPREV", "CDC:cdi:ALC06:CRDPREV"],
            note: "Crude and age-adjusted are different measures; the published stratum is shown and is never converted to the other.",
          },
          placesCounty("places-teethlost", "All teeth lost (adults 65+)", "CDC:places_county:TEETHLOST:AgeAdjPrv"),
          placesCounty("places-mobility", "Mobility disability", "CDC:places_county:MOBILITY:AgeAdjPrv"),
          placesCounty("places-indeplive", "Independent living disability", "CDC:places_county:INDEPLIVE:AgeAdjPrv"),
          placesCounty("places-selfcare", "Self-care disability", "CDC:places_county:SELFCARE:AgeAdjPrv"),
          placesCounty("places-hearing", "Hearing disability", "CDC:places_county:HEARING:AgeAdjPrv"),
          placesCounty("places-vision", "Vision disability", "CDC:places_county:VISION:AgeAdjPrv"),
          placesCounty("places-cognition", "Cognitive disability", "CDC:places_county:COGNITION:AgeAdjPrv"),
        ],
      },
    ],
  },
  {
    id: "disease-illness-burden",
    title: "Community disease and illness burden",
    summary:
      "Published condition indicators for this place, beside the population and community measures a needs assessment reads them against.",
    limits:
      "The denominator is not computed here. A CDC indicator is shown as the CDC publishes it - crude or age-adjusted, with its own stratum - and is never divided by a population figure from another source to make a rate. Suppressed, not reported, and provisional stay distinct from one another and from zero. Surveillance coverage is a property of the indicator, not of this place.",
    sections: [
      {
        id: "indicators",
        title: "Published condition indicators",
        description:
          "CDC chronic disease indicators. Each carries its own case definition, stratification, and provisional status.",
        measures: [
          COUNTY_ARTHRITIS,
          {
            id: "cdc-chronic-indicator",
            label: "Chronic disease indicator",
            candidates: ["CDC:cdi:ALC06:AGEADJPREV", "CDC:cdi:ALC06:CRDPREV"],
            note: "A crude rate and an age-adjusted rate answer different questions; both are published separately and neither is derived from the other here.",
          },
        ],
      },
      {
        id: "conditions",
        title: "Chronic conditions",
        description: `Every remaining condition the PLACES county release publishes. ${PLACES_SECTION_NOTE}`,
        measures: PLACES_CONDITIONS,
      },
      {
        id: "health-status",
        title: "Health status and risk behaviors",
        description: `Self-rated health, distress, sleep, and behaviors, as published. ${PLACES_SECTION_NOTE}`,
        measures: PLACES_STATUS_BEHAVIORS,
      },
      {
        id: "prevention",
        title: "Prevention and access to care",
        description: `Screening, checkups, and insurance coverage, as published. ${PLACES_SECTION_NOTE}`,
        measures: PLACES_PREVENTION,
      },
      {
        id: "disability",
        title: "Disability",
        description: `Each published disability measure, shown separately. ${PLACES_SECTION_NOTE}`,
        measures: PLACES_DISABILITY,
      },
      {
        id: "social-needs",
        title: "Health-related social needs",
        description: `Social and material circumstances the PLACES release publishes beside its health measures. ${PLACES_SECTION_NOTE}`,
        measures: PLACES_SOCIAL_NEEDS,
      },
      {
        id: "denominator",
        title: "Population the indicators describe",
        description:
          "The published population measures for this place, shown so a reader can see the base rather than infer it.",
        measures: [
          {
            id: "total-population",
            label: "Total population",
            candidates: [
              "CENSUS_ACS:acs5:B01003_001",
              "CENSUS_ACS:acs1:B01003_001",
            ],
            note: "Shown as context. It is not the denominator of any indicator above, which carry their own.",
          },
          {
            id: "age-distribution",
            label: "Population (age-and-sex table total)",
            candidates: [
              "CENSUS_ACS:acs5:B01001_001",
              "CENSUS_ACS:acs1:B01001_001",
            ],
            note: "Age structure is what age adjustment exists to handle; it is shown rather than applied.",
          },
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: ["CENSUS_PEP:POPESTIMATE"],
          },
        ],
      },
      {
        id: "community",
        title: "Community characteristics",
        description:
          "Measures a needs assessment commonly reads beside condition burden. Association only; none of these explains an indicator above.",
        measures: [
          {
            id: "health-insurance",
            label: "Population (health-insurance table total)",
            candidates: [
              "CENSUS_ACS:acs5:B27010_001",
              "CENSUS_ACS:acs1:B27010_001",
            ],
          },
          {
            id: "poverty",
            label: "Population (poverty-status table total)",
            candidates: [
              "CENSUS_ACS:acs5:B17001_001",
              "CENSUS_ACS:acs1:B17001_001",
            ],
          },
          {
            id: "disability-by-age",
            label: "Population (disability table total)",
            candidates: [
              "CENSUS_ACS:acs5:C18108_001",
              "CENSUS_ACS:acs1:C18108_001",
            ],
          },
        ],
      },
    ],
  },
  {
    id: "public-safety-trend",
    title: "Public-safety trend with population context",
    summary:
      "Reported offence counts from the FBI UCR program, the population base published for the same place, and the reporting participation that bounds what the counts mean - each shown separately.",
    limits:
      "No rate is computed here. The rate shown is the one the FBI program published, on its own denominator; the count is not divided by the population estimate beside it, and those two numbers will not agree. Reporting participation is not published in this catalog, so the count is bounded by an unknown number of non-reporting agencies -- a period no agency reported is not zero crime, and not reported, suppressed and zero stay distinct. This program publishes at agency, state and national grain and not at county, so a county view will state that gap rather than aggregate agencies into one. Definition breaks between program years are visible in the series and are not smoothed. Nothing here describes cause.",
    sections: [
      {
        id: "reported",
        title: "Reported offences",
        description:
          "Counts as agencies reported them to the FBI. The count is the number of offences reported, not the number that occurred.",
        measures: [
          {
            id: "violent-crime",
            label: "Violent crime, reported",
            candidates: [
              "FBI_UCR:summarized_violent_crime:V:offense:absolute_total",
            ],
            note: "A missing period means no agency report, which is not a period without crime.",
          },
        ],
      },
      {
        id: "published-rate",
        title: "The rate the program published",
        description:
          "The FBI's own population-normalized rate for the same offence. It is shown because the program published it, not because anything here divided one measure by another.",
        measures: [
          {
            id: "violent-crime-rate",
            label: "Violent crime rate, as published",
            candidates: ["FBI_UCR:summarized_violent_crime:V:offense:rate"],
            note: "The program's own rate, on its own denominator. It is not the count above divided by the population below, and the two will not agree.",
          },
          {
            id: "violent-crime-clearance",
            label: "Offences cleared, reported",
            candidates: [
              "FBI_UCR:summarized_violent_crime:V:clearance:absolute_total",
            ],
            note: "Clearance is not reporting participation: it counts what was cleared, not which agencies reported at all.",
          },
        ],
      },
      {
        id: "population-base",
        title: "Population base",
        description:
          "The published population for the same place, shown beside the counts rather than divided into them.",
        measures: [
          {
            id: "population-estimate",
            label: "Resident population estimate",
            candidates: ["CENSUS_PEP:POPESTIMATE"],
            note: "The Population Estimates Program vintage for this place. It is a base a reader can apply, not one this product applies.",
          },
          {
            id: "total-population",
            label: "Total population (survey estimate)",
            candidates: [
              "CENSUS_ACS:acs5:B01003_001",
              "CENSUS_ACS:acs1:B01003_001",
            ],
            note: "A survey estimate with its own margin of error, and a different method from the estimate above.",
          },
        ],
      },
    ],
  },
  {
    id: "rural-agricultural-economy",
    title: "Rural and agricultural economy profile",
    summary:
      "Published agricultural measures for this place beside the workforce, income, and household measures that describe the economy around them.",
    limits:
      "Commodity measures keep the units, survey year, and geography the USDA NASS program published them in, and are not converted, rebased, or interpolated. A suppressed value is withheld for disclosure reasons and is not a zero harvest; it stays distinct from a value that was never surveyed. Agricultural geography is not always the same geography as the census measures beside it, and the two are not merged.",
    sections: [
      {
        id: "agriculture",
        title: "Agricultural production",
        description:
          "USDA NASS survey measures, in their own units and survey years, subject to disclosure suppression.",
        measures: [
          {
            id: "nass-commodity",
            label: "Crop survey measure",
            candidates: [
              "USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11",
            ],
            note: "Published per commodity, per survey year, in the program's own unit; a suppressed cell is withheld, not zero.",
          },
        ],
      },
      {
        id: "workforce",
        title: "Workforce",
        description:
          "Labor force measures from the American Community Survey, which publishes them for the same geographies as the household measures below.",
        measures: [
          {
            id: "labor-force",
            label: "Population in the labor force",
            candidates: [
              "CENSUS_ACS:acs5:B23025_002",
              "CENSUS_ACS:acs1:B23025_002",
            ],
            note: "The ACS labor-force universe, not the payroll or household survey universe a national series uses.",
          },
          {
            id: "unemployed",
            label: "Unemployed, civilian labor force",
            candidates: [
              "CENSUS_ACS:acs5:B23025_005",
              "CENSUS_ACS:acs1:B23025_005",
            ],
            note: "A published count for this place. It is not the BLS unemployment rate, which BLS publishes under a separate series identity for each area.",
          },
          {
            id: "industry",
            label: "Civilian employed residents (industry table total)",
            candidates: [
              "CENSUS_ACS:acs5:C24050_001",
              "CENSUS_ACS:acs1:C24050_001",
            ],
            note: "Agriculture appears here as an industry of employment, which counts people rather than farms.",
          },
        ],
      },
      {
        id: "households",
        title: "Household conditions",
        description: "Income and household measures for the same place.",
        measures: [
          {
            id: "median-household-income",
            label: "Median household income",
            candidates: [
              "CENSUS_ACS:acs5:B19013_001",
              "CENSUS_ACS:acs1:B19013_001",
            ],
          },
          {
            id: "poverty",
            label: "Population (poverty-status table total)",
            candidates: [
              "CENSUS_ACS:acs5:B17001_001",
              "CENSUS_ACS:acs1:B17001_001",
            ],
          },
          {
            id: "total-population",
            label: "Total population",
            candidates: [
              "CENSUS_ACS:acs5:B01003_001",
              "CENSUS_ACS:acs1:B01003_001",
            ],
          },
        ],
      },
    ],
  },
];

export const DEFAULT_TEMPLATE_ID = PRODUCT_TEMPLATES[0]!.id;

export function findTemplate(
  id: string | null | undefined,
): ProductTemplate | null {
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
export function templateCoverage(
  sections: ResolvedSection[] | null | undefined,
): TemplateCoverage {
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

/** One measure's published answer for the selected place. */
export interface MeasureAnswer {
  /** The published row for this place, or `null` when none was published. */
  row: ObservationRow | null;
  state: string;
  message: string;
}

export interface ProductExport {
  headings: string[];
  rows: string[][];
}

/**
 * The profile as the file a reader keeps (WEB-060).
 *
 * It carried one uncertainty column, `margin_of_error`, read straight off the
 * row -- so it saw only what `normalizeObservationRows` happens to lift, two
 * of seven fields and only for a neutral-shaped source -- and no coverage
 * column at all. WEB-051 and WEB-053 wrote down why that is wrong, for the
 * explorer's export: "a file that carried a subset would be this client
 * deciding which part of a source's participation basis a reader may have",
 * and the same for every field `ObservationUncertainty` publishes. The
 * profile product is a product screen, the polished surface a
 * non-specialist reads, and its template already configures a CDC slot --
 * CDC publishes confidence bounds, not a margin.
 *
 * Extracted from the component so the file's contents can be asserted:
 * neither export's rows were covered anywhere, which is how this survived
 * the screen that was fixed.
 */
export function profileExport(
  template: ProductTemplate | null | undefined,
  sections: ResolvedSection[] | null | undefined,
  answers: Record<string, MeasureAnswer> | null | undefined,
  place: { geoId: string; placeName: string },
): ProductExport {
  const headings = [
    "product",
    "section",
    "slot",
    "metric_code",
    "metric_name",
    "source",
    "geo_id",
    "geo_name",
    "period",
    "value",
    "value_status",
    "unit",
    ...OBSERVATION_UNCERTAINTY_FIELDS,
    ...OBSERVATION_COVERAGE_FIELDS,
    "availability",
  ];
  const byId = answers || {};
  const rows: string[][] = [];
  for (const entry of sections || []) {
    for (const measure of entry.measures) {
      const answer = byId[measure.slot.id];
      const row = answer?.row ?? null;
      rows.push([
        template?.title || "",
        entry.section.title,
        measure.slot.label,
        measure.metricCode,
        measure.metric ? displayMetricName(measure.metric) : "",
        String(measure.metric?.source_code ?? ""),
        place.geoId,
        place.placeName,
        row ? observationPeriodLabel(row) : "",
        row?.value == null ? "" : String(row.value),
        String(row?.value_status ?? ""),
        row ? observationUnit(row) : "",
        ...OBSERVATION_UNCERTAINTY_FIELDS.map((field) =>
          observationUncertaintyValue(row, field),
        ),
        ...OBSERVATION_COVERAGE_FIELDS.map((field) =>
          observationCoverageValue(row, field),
        ),
        measure.available ? answer?.message || "not requested" : measure.reason,
      ]);
    }
  }
  return { headings, rows };
}
