import { PRODUCT_TEMPLATES } from "./productTemplates";
import type { ProductTemplate, TemplateSection } from "./productTemplates";

export interface UseCasePage extends ProductTemplate {
  rank: number;
  href: string;
  group: string;
  groupTitle?: string;
  audience: string;
  products: string;
  guardrail: string;
  question: string;
  steps: string[];
  catalogQuery: string;
  tool: "profile" | "trend" | "comparison" | "evidence" | "quality";
}

// Presentation bundles over existing verified templates, never new metric
// definitions. The catalog still resolves every identity at runtime.
function sections(...references: string[]): TemplateSection[] {
  const seen = new Set<string>();
  return references.map((reference) => {
    const [templateId, sectionId] = reference.split("/");
    const section = PRODUCT_TEMPLATES.find((entry) => entry.id === templateId)?.sections.find((entry) => entry.id === sectionId);
    if (!section) throw new Error(`Unknown verified section: ${reference}`);
    return {
      ...section,
      measures: section.measures.filter((slot) => {
        if (seen.has(slot.id)) return false;
        seen.add(slot.id);
        return true;
      }),
    };
  }).filter((section) => section.measures.length > 0);
}

export const USE_CASE_GROUPS = [
  { id: "place", title: "Place profiles", description: "Understand a community and how it is changing." },
  { id: "economy", title: "Economy & workforce", description: "Put jobs, housing, prices, and market conditions in context." },
  { id: "health", title: "Health & community", description: "Read health signals with their population and reporting basis." },
  { id: "safety", title: "Public safety", description: "Explore reported crime with coverage and context." },
  { id: "agriculture", title: "Agriculture & rural economy", description: "Connect production, livelihoods, and rural conditions." },
  { id: "publishing", title: "Publishing & evidence", description: "Turn transparent analysis into reusable, traceable evidence." },
];

type Definition = Omit<UseCasePage, "href" | "limits">;

const definitions: Definition[] = [
  {
    "id": "community-conditions",
    "rank": 1,
    "title": "Community conditions profile",
    "group": "place",
    "tool": "profile",
    "audience": "Local government, journalists, residents, grant writers",
    "products": "ACS, PEP, BLS, CDC, FBI, AG",
    "summary": "One reusable place profile combining population, demographics, labor, health, safety, and rural/agricultural context with direct paths into each source explorer",
    "guardrail": "Display each measure's period, denominator, geography, coverage, and source independently; do not collapse unlike measures into an unexplained score",
    "question": "What do the published measures tell us about this community?",
    "catalogQuery": "community",
    "steps": [
      "Choose a place and review the independently sourced indicators.",
      "Inspect a trend or open its map with the same metric and geography.",
      "Export the profile or save it for a recurring community briefing."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "community-conditions/health", "community-conditions/safety", "community-conditions/rural")
  },
  {
    "id": "population-growth",
    "rank": 2,
    "title": "Population growth and service-demand planning",
    "group": "place",
    "tool": "trend",
    "audience": "Planners, utilities, schools, health systems",
    "products": "PEP, ACS",
    "summary": "Map recent population change with age, household, housing, and socioeconomic characteristics to identify where service demand may be shifting",
    "guardrail": "Distinguish PEP estimates from ACS survey estimates, vintages, margins of error, and boundary changes",
    "question": "Where might changing population put pressure on local services?",
    "catalogQuery": "population",
    "steps": [
      "Read population history alongside households and housing units.",
      "Keep PEP vintages and ACS survey periods separate.",
      "Save a chart and document the service-planning question it supports."
    ],
    "sections": sections("population-growth/estimates", "population-growth/survey")
  },
  {
    "id": "workforce",
    "rank": 3,
    "title": "Workforce availability and labor-market depth",
    "group": "economy",
    "tool": "comparison",
    "audience": "Economic development teams, employers, workforce boards",
    "products": "BLS, ACS, PEP",
    "summary": "Combine employment, unemployment, labor-force participation, occupation/industry context, commuting, education, and population change into a workforce briefing",
    "guardrail": "Keep household-survey, establishment-survey, and ACS concepts separate; never sum rates or mix jobs with employed people",
    "question": "How deep is the available workforce, and how is it changing?",
    "catalogQuery": "labor",
    "steps": [
      "Choose a place and inspect employment and labor-force context.",
      "Compare published measures only after the API checks compatibility.",
      "Build a workforce briefing with the survey universes stated."
    ],
    "sections": sections("workforce/labor-force", "workforce/population-base", "rural-agricultural-economy/workforce")
  },
  {
    "id": "housing-affordability",
    "rank": 4,
    "title": "Housing affordability and household pressure",
    "group": "economy",
    "tool": "profile",
    "audience": "Housing agencies, lenders, community organizations",
    "products": "ACS, FRED, BLS",
    "summary": "Relate household income, rent, home value, tenure, labor earnings, mortgage rates, and inflation in an affordability dashboard",
    "guardrail": "Label national FRED rates versus local ACS measures; preserve ACS uncertainty and avoid implying mortgage rates alone cause local outcomes",
    "question": "What do local housing costs and incomes say about household pressure?",
    "catalogQuery": "housing",
    "steps": [
      "Read local rent, home value, and income on their own terms.",
      "Switch the chart to national geography for mortgage and price context.",
      "Export the evidence with uncertainty and geography differences visible."
    ],
    "sections": sections("housing-affordability/cost", "housing-affordability/pressure", "housing-affordability/stock", "housing-affordability/national-context")
  },
  {
    "id": "cost-of-living",
    "rank": 5,
    "title": "Local cost-of-living context",
    "group": "economy",
    "tool": "trend",
    "audience": "Residents, employers, journalists, policy analysts",
    "products": "BLS, FRED, ACS",
    "summary": "Explain national/regional price movement alongside local incomes, housing costs, and earnings using an article-ready indicator bundle",
    "guardrail": "Do not present a national price index as a precise local cost-of-living index; expose geography and index-base limitations",
    "question": "How do wider price movements sit alongside local household resources?",
    "catalogQuery": "price",
    "steps": [
      "Inspect national price history and its published index base.",
      "Read local housing costs and household income separately.",
      "Compose an indicator bundle that states the geographic limits."
    ],
    "sections": sections("housing-affordability/national-context", "housing-affordability/cost", "rural-agricultural-economy/households")
  },
  {
    "id": "disease-illness-burden",
    "rank": 6,
    "title": "Community disease and illness burden",
    "group": "health",
    "tool": "profile",
    "audience": "Public-health departments, hospitals, researchers, journalists",
    "products": "CDC, ACS, PEP",
    "summary": "Explore condition incidence, prevalence, hospitalization, or mortality alongside population and community characteristics to support needs assessment",
    "guardrail": "Use the correct denominator and age adjustment; show suppression, provisional status, case definitions, and surveillance coverage",
    "question": "Which published health indicators can inform a community needs assessment?",
    "catalogQuery": "disease",
    "steps": [
      "Choose a health measure from the published catalog.",
      "Inspect its strata, denominator, uncertainty, and surveillance coverage.",
      "Save the evidence alongside independently published community context."
    ],
    "sections": sections("disease-illness-burden/indicators", "disease-illness-burden/conditions", "disease-illness-burden/health-status", "disease-illness-burden/prevention", "disease-illness-burden/disability", "disease-illness-burden/social-needs", "disease-illness-burden/denominator", "disease-illness-burden/community")
  },
  {
    "id": "disease-capacity-watch",
    "rank": 7,
    "title": "Disease trend and capacity watch",
    "group": "health",
    "tool": "trend",
    "audience": "Public-health operations, emergency planners, health systems",
    "products": "CDC, PEP",
    "summary": "Track available disease/illness trends and population-normalized burden with freshness and provisional-data indicators",
    "guardrail": "This is situational awareness, not diagnosis or prediction; reporting delays and changing case definitions must remain visible",
    "question": "What do reported health trends show, and how current are those signals?",
    "catalogQuery": "disease",
    "steps": [
      "Find the disease or illness series relevant to your watch.",
      "Inspect published trends without collapsing strata or estimating missing periods.",
      "Check freshness and provisional status before exporting a watch briefing."
    ],
    "sections": sections("disease-illness-burden/indicators", "disease-illness-burden/conditions", "disease-illness-burden/health-status", "population-growth/estimates")
  },
  {
    "id": "public-safety-trend",
    "rank": 8,
    "title": "Public-safety trend normalized by population",
    "group": "safety",
    "tool": "trend",
    "audience": "Local government, journalists, researchers",
    "products": "FBI, PEP",
    "summary": "Present reported offense or arrest counts beside population-based rates and reporting participation over time",
    "guardrail": "Never treat missing agency reports as zero crime; show program, coverage, denominator, and definition breaks",
    "question": "How do reported offenses and published rates change with reporting coverage?",
    "catalogQuery": "crime",
    "steps": [
      "Choose a state or agency geography supported by the measure.",
      "Read reported counts, published rates, and participation separately.",
      "Export the trend with the program definitions and denominator."
    ],
    "sections": sections("public-safety-trend/reported", "public-safety-trend/published-rate", "public-safety-trend/population-base")
  },
  {
    "id": "crime-economic-context",
    "rank": 9,
    "title": "Crime and economic-context explorer",
    "group": "safety",
    "tool": "comparison",
    "audience": "Researchers, community organizations, journalists",
    "products": "FBI, BLS, ACS, PEP",
    "summary": "Compare public-safety trends with labor-market and community conditions through linked charts and maps",
    "guardrail": "Describe association only; prevent causal language and avoid ecological conclusions about individuals or demographic groups",
    "question": "How do public-safety signals sit beside local economic conditions?",
    "catalogQuery": "crime",
    "steps": [
      "Read the crime series at its published geography and coverage.",
      "Inspect labor and income trends independently; respect declined comparisons.",
      "Record associations with period and coverage differences, without causal claims."
    ],
    "sections": sections("public-safety-trend/reported", "public-safety-trend/published-rate", "community-conditions/labor", "population-growth/estimates")
  },
  {
    "id": "rural-agricultural-economy",
    "rank": 10,
    "title": "Rural and agricultural economy profile",
    "group": "agriculture",
    "tool": "profile",
    "audience": "Counties, cooperatives, lenders, extension programs",
    "products": "AG, ACS, PEP, BLS",
    "summary": "Combine farms, commodities, production, acreage, yield, employment, population, and household conditions into a rural-economy profile",
    "guardrail": "Preserve commodity units, survey years, suppression, and agriculture-specific geography; do not interpolate suppressed values",
    "question": "What do agricultural activity and household conditions reveal about a rural economy?",
    "catalogQuery": "agriculture",
    "steps": [
      "Choose the commodity and geography the program actually publishes.",
      "Read production, workforce, income, and population separately.",
      "Save the profile with units, survey years, and disclosure limits."
    ],
    "sections": sections("rural-agricultural-economy/agriculture", "rural-agricultural-economy/workforce", "rural-agricultural-economy/households", "population-growth/estimates")
  },
  {
    "id": "agricultural-production-prices",
    "rank": 11,
    "title": "Agricultural production and price context",
    "group": "agriculture",
    "tool": "trend",
    "audience": "Producers, analysts, food businesses, journalists",
    "products": "AG, FRED, BLS",
    "summary": "Relate production, yield, inventories, and commodity measures to broader producer/consumer price and labor indicators",
    "guardrail": "Separate physical quantities from prices and indexes; disclose seasonal, revision, and geographic differences",
    "question": "How does agricultural production sit alongside broader price movements?",
    "catalogQuery": "production",
    "steps": [
      "Find a production, yield, or inventory measure in the catalog.",
      "Inspect its history and compare price context on a separate scale.",
      "Keep physical quantities, prices, indexes, and revisions distinct in a chart."
    ],
    "sections": sections("rural-agricultural-economy/agriculture", "housing-affordability/national-context", "rural-agricultural-economy/workforce")
  },
  {
    "id": "agricultural-workforce",
    "rank": 12,
    "title": "Agricultural workforce monitor",
    "group": "agriculture",
    "tool": "comparison",
    "audience": "Workforce boards, producers, rural planners",
    "products": "AG, BLS, ACS, PEP",
    "summary": "Explore agricultural activity alongside employment, wages, commuting, demographic change, and available labor-force measures",
    "guardrail": "Agricultural program definitions and seasonal work do not map perfectly to general BLS/ACS industries; label the mismatch",
    "question": "What workforce context accompanies agricultural activity in this area?",
    "catalogQuery": "agriculture",
    "steps": [
      "Read the crop activity and labor-force measures for the same area.",
      "Inspect published histories and identify seasonal or industry-definition differences.",
      "Build a briefing with agriculture and general labor concepts stated separately."
    ],
    "sections": sections("rural-agricultural-economy/agriculture", "rural-agricultural-economy/workforce", "workforce/labor-force", "population-growth/estimates")
  },
  {
    "id": "aging-population",
    "rank": 13,
    "title": "Aging population and health-service planning",
    "group": "place",
    "tool": "profile",
    "audience": "Health systems, aging agencies, local government",
    "products": "ACS, PEP, CDC",
    "summary": "Show growth in older populations with disability, living arrangement, income, and available illness/mortality measures",
    "guardrail": "Use compatible age bands and rates; retain ACS margins of error and CDC age-adjustment status",
    "question": "What population and health context should inform planning for older residents?",
    "catalogQuery": "age",
    "steps": [
      "Read the published age bands and living arrangements.",
      "Inspect disability, income, and health context without recutting age bands.",
      "Save the planning evidence with survey uncertainty and adjustment status."
    ],
    "sections": sections("aging-population/how-many", "aging-population/circumstances", "aging-population/means", "aging-population/health-measures")
  },
  {
    "id": "economic-shock-recovery",
    "rank": 14,
    "title": "Economic shock and recovery monitor",
    "group": "economy",
    "tool": "trend",
    "audience": "State/local leaders, analysts, journalists",
    "products": "BLS, FRED, PEP, CDC",
    "summary": "Track labor, macroeconomic, population, and relevant health signals through a disruption and recovery period",
    "guardrail": "Align release dates and frequencies; distinguish revised data from what was known at the time",
    "question": "How have labor, economic, population, and health signals moved through a disruption?",
    "catalogQuery": "employment",
    "steps": [
      "Choose the disruption period and inspect each published history.",
      "Read frequencies and releases separately; a latest history can include revisions.",
      "Use the workbench to pin releases when reporting what was known at the time."
    ],
    "sections": sections("community-conditions/labor", "housing-affordability/national-context", "population-growth/estimates", "disease-illness-burden/indicators")
  },
  {
    "id": "grant-needs-assessment",
    "rank": 15,
    "title": "Evidence-backed grant needs assessment",
    "group": "publishing",
    "tool": "evidence",
    "audience": "Nonprofits, local agencies, grant writers",
    "products": "ACS, PEP, CDC, FBI, BLS, AG",
    "summary": "Produce a traceable evidence packet with maps, trends, source notes, downloadable tables, and frozen/live chart choices",
    "guardrail": "Selection must be transparent; avoid cherry-picking and include uncertainty, missingness, coverage, and comparison rationale",
    "question": "Which traceable measures support the needs described in a grant proposal?",
    "catalogQuery": "poverty",
    "steps": [
      "Select indicators and explain the comparison and selection rationale.",
      "Export tables and save charts with missingness and uncertainty included.",
      "Build an evidence packet with methodology, caveats, and live or pinned blocks."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "disease-illness-burden/community", "community-conditions/health", "community-conditions/safety", "community-conditions/rural")
  },
  {
    "id": "business-location",
    "rank": 16,
    "title": "Business location and market context",
    "group": "economy",
    "tool": "comparison",
    "audience": "Site selectors, entrepreneurs, economic developers",
    "products": "ACS, PEP, BLS, FRED, FBI",
    "summary": "Package workforce, population, income, commuting, macro-financial, and reported public-safety context for candidate geographies",
    "guardrail": "Avoid opaque rankings; allow users to inspect weights, periods, reporting coverage, and every underlying measure",
    "question": "What transparent evidence helps compare candidate business locations?",
    "catalogQuery": "income",
    "steps": [
      "Choose candidate places and inspect workforce and market context.",
      "Use peer charts and compatible comparisons with every period visible.",
      "Save the underlying measures and decision rationale without an opaque score."
    ],
    "sections": sections("workforce/population-base", "rural-agricultural-economy/workforce", "community-conditions/labor", "housing-affordability/national-context", "public-safety-trend/reported", "population-growth/estimates")
  },
  {
    "id": "peer-benchmarking",
    "rank": 17,
    "title": "Peer county or state benchmarking",
    "group": "place",
    "tool": "comparison",
    "audience": "Public administrators, researchers, journalists",
    "products": "ACS, PEP, BLS, CDC, FBI, AG",
    "summary": "Let users create explainable peer groups and compare distributions, ranks, and trends across several domains",
    "guardrail": "Peer criteria must be explicit; ranks need uncertainty/coverage warnings and should not combine incomparable periods silently",
    "question": "How does this place compare with explicitly selected peers?",
    "catalogQuery": "population",
    "steps": [
      "Choose a published measure and a geography grain shared by your peers.",
      "Open its map or ranking in the workbench and state the peer criteria.",
      "Export the comparison with periods, coverage, and uncertainty visible."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "community-conditions/health", "community-conditions/safety", "community-conditions/rural")
  },
  {
    "id": "data-journalism",
    "rank": 18,
    "title": "Local data journalism and story production",
    "group": "publishing",
    "tool": "evidence",
    "audience": "Newsrooms, independent journalists, students",
    "products": "All products",
    "summary": "Search a topic, build a chart/map, inspect methodology, save it, combine it with narrative, and publish a reproducible story",
    "guardrail": "Every published block retains source, metric, geography, period, transform, refresh/vintage, caveats, and live/frozen status",
    "question": "How can a public-data question become a reproducible local story?",
    "catalogQuery": "community",
    "steps": [
      "Search the catalog for a story question and inspect the source evidence.",
      "Build and save charts or maps, then add narrative and methodology in Builder.",
      "Preview the composed article and retain the query, period, and live or pinned status."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "community-conditions/health", "community-conditions/safety", "community-conditions/rural", "housing-affordability/national-context")
  },
  {
    "id": "program-evidence-library",
    "rank": 19,
    "title": "Public-program planning evidence library",
    "group": "publishing",
    "tool": "evidence",
    "audience": "Public agencies, nonprofits, regional partnerships",
    "products": "ACS, PEP, BLS, CDC, FBI, AG",
    "summary": "Save approved indicator collections for recurring plans covering housing, workforce, health, safety, population, or rural development",
    "guardrail": "The library provides context, not program-effect attribution; definitions and approved uses live in reviewed semantic documentation",
    "question": "Which indicator collections can be reused in recurring public-program plans?",
    "catalogQuery": "community",
    "steps": [
      "Select indicators and record their definitions and approved uses.",
      "Save profiles and chart configurations with explicit program context.",
      "Reopen the saved library and check freshness before each planning cycle."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "community-conditions/health", "community-conditions/safety", "community-conditions/rural")
  },
  {
    "id": "source-data-quality",
    "rank": 20,
    "title": "Source coverage and data-quality explorer",
    "group": "publishing",
    "tool": "quality",
    "audience": "Data stewards, analysts, advanced users",
    "products": "All products",
    "summary": "Visualize freshness, revisions, suppressed values, missing periods, geography coverage, reporting participation, and definition changes before analysis begins",
    "guardrail": "Quality states must come from source evidence and pipeline observations; never convert unknown, suppressed, or unreported values to zero",
    "question": "What coverage, freshness, and reporting limits should be checked before analysis?",
    "catalogQuery": "coverage",
    "steps": [
      "Review published source freshness and catalog coverage.",
      "Inspect the quality table and open source measures to check missingness and revisions.",
      "Record the observed limitations before saving or publishing an analysis."
    ],
    "sections": sections("community-conditions/population", "community-conditions/labor", "community-conditions/health", "community-conditions/safety", "community-conditions/rural", "housing-affordability/national-context")
  }
];

export const useCasePages: UseCasePage[] = definitions.map((entry) => ({
  ...entry,
  groupTitle: USE_CASE_GROUPS.find((group) => group.id === entry.group)?.title,
  limits: entry.guardrail,
  href: `/use-cases/${entry.id}`,
}));

export const useCaseGroups = USE_CASE_GROUPS.map((group) => ({
  ...group,
  pages: useCasePages.filter((entry) => entry.group === group.id),
}));

export function findUseCasePage(id: string): UseCasePage | null {
  return useCasePages.find((entry) => entry.id === id) || null;
}
