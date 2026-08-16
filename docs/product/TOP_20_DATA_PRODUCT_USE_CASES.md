# Top 20 Packaged Public-Data Use Cases

## Purpose

These use cases show how the packaged Census ACS, Census Population Estimates Program (PEP), FRED, BLS, CDC disease and illness, FBI crime, and agricultural data products can create value beyond seven disconnected source explorers.

They are product opportunities, not precomputed warehouse outputs. Each should be assembled from source-transparent metrics, reviewed business definitions, reusable chart configurations, and explicit analytical guardrails. Cross-source association must never be presented as causation.

Product abbreviations used below:

- **ACS:** Census American Community Survey
- **PEP:** Census Population Estimates Program
- **FRED:** Federal Reserve Economic Data
- **BLS:** Bureau of Labor Statistics
- **CDC:** CDC disease and illness data
- **FBI:** FBI crime data
- **AG:** Agricultural data, principally source-native USDA programs as onboarded

## Prioritized Use Cases

| Rank | Use case | Primary users | Packaged products | User value and recommended output | Essential guardrail |
| ---: | --- | --- | --- | --- | --- |
| 1 | Community conditions profile | Local government, journalists, residents, grant writers | ACS, PEP, BLS, CDC, FBI, AG | One reusable place profile combining population, demographics, labor, health, safety, and rural/agricultural context with direct paths into each source explorer | Display each measure's period, denominator, geography, coverage, and source independently; do not collapse unlike measures into an unexplained score |
| 2 | Population growth and service-demand planning | Planners, utilities, schools, health systems | PEP, ACS | Map recent population change with age, household, housing, and socioeconomic characteristics to identify where service demand may be shifting | Distinguish PEP estimates from ACS survey estimates, vintages, margins of error, and boundary changes |
| 3 | Workforce availability and labor-market depth | Economic development teams, employers, workforce boards | BLS, ACS, PEP | Combine employment, unemployment, labor-force participation, occupation/industry context, commuting, education, and population change into a workforce briefing | Keep household-survey, establishment-survey, and ACS concepts separate; never sum rates or mix jobs with employed people |
| 4 | Housing affordability and household pressure | Housing agencies, lenders, community organizations | ACS, FRED, BLS | Relate household income, rent, home value, tenure, labor earnings, mortgage rates, and inflation in an affordability dashboard | Label national FRED rates versus local ACS measures; preserve ACS uncertainty and avoid implying mortgage rates alone cause local outcomes |
| 5 | Local cost-of-living context | Residents, employers, journalists, policy analysts | BLS, FRED, ACS | Explain national/regional price movement alongside local incomes, housing costs, and earnings using an article-ready indicator bundle | Do not present a national price index as a precise local cost-of-living index; expose geography and index-base limitations |
| 6 | Community disease and illness burden | Public-health departments, hospitals, researchers, journalists | CDC, ACS, PEP | Explore condition incidence, prevalence, hospitalization, or mortality alongside population and community characteristics to support needs assessment | Use the correct denominator and age adjustment; show suppression, provisional status, case definitions, and surveillance coverage |
| 7 | Disease trend and capacity watch | Public-health operations, emergency planners, health systems | CDC, PEP | Track available disease/illness trends and population-normalized burden with freshness and provisional-data indicators | This is situational awareness, not diagnosis or prediction; reporting delays and changing case definitions must remain visible |
| 8 | Public-safety trend normalized by population | Local government, journalists, researchers | FBI, PEP | Present reported offense or arrest counts beside population-based rates and reporting participation over time | Never treat missing agency reports as zero crime; show program, coverage, denominator, and definition breaks |
| 9 | Crime and economic-context explorer | Researchers, community organizations, journalists | FBI, BLS, ACS, PEP | Compare public-safety trends with labor-market and community conditions through linked charts and maps | Describe association only; prevent causal language and avoid ecological conclusions about individuals or demographic groups |
| 10 | Rural and agricultural economy profile | Counties, cooperatives, lenders, extension programs | AG, ACS, PEP, BLS | Combine farms, commodities, production, acreage, yield, employment, population, and household conditions into a rural-economy profile | Preserve commodity units, survey years, suppression, and agriculture-specific geography; do not interpolate suppressed values |
| 11 | Agricultural production and price context | Producers, analysts, food businesses, journalists | AG, FRED, BLS | Relate production, yield, inventories, and commodity measures to broader producer/consumer price and labor indicators | Separate physical quantities from prices and indexes; disclose seasonal, revision, and geographic differences |
| 12 | Agricultural workforce monitor | Workforce boards, producers, rural planners | AG, BLS, ACS, PEP | Explore agricultural activity alongside employment, wages, commuting, demographic change, and available labor-force measures | Agricultural program definitions and seasonal work do not map perfectly to general BLS/ACS industries; label the mismatch |
| 13 | Aging population and health-service planning | Health systems, aging agencies, local government | ACS, PEP, CDC | Show growth in older populations with disability, living arrangement, income, and available illness/mortality measures | Use compatible age bands and rates; retain ACS margins of error and CDC age-adjustment status |
| 14 | Economic shock and recovery monitor | State/local leaders, analysts, journalists | BLS, FRED, PEP, CDC | Track labor, macroeconomic, population, and relevant health signals through a disruption and recovery period | Align release dates and frequencies; distinguish revised data from what was known at the time |
| 15 | Evidence-backed grant needs assessment | Nonprofits, local agencies, grant writers | ACS, PEP, CDC, FBI, BLS, AG | Produce a traceable evidence packet with maps, trends, source notes, downloadable tables, and frozen/live chart choices | Selection must be transparent; avoid cherry-picking and include uncertainty, missingness, coverage, and comparison rationale |
| 16 | Business location and market context | Site selectors, entrepreneurs, economic developers | ACS, PEP, BLS, FRED, FBI | Package workforce, population, income, commuting, macro-financial, and reported public-safety context for candidate geographies | Avoid opaque rankings; allow users to inspect weights, periods, reporting coverage, and every underlying measure |
| 17 | Peer county or state benchmarking | Public administrators, researchers, journalists | ACS, PEP, BLS, CDC, FBI, AG | Let users create explainable peer groups and compare distributions, ranks, and trends across several domains | Peer criteria must be explicit; ranks need uncertainty/coverage warnings and should not combine incomparable periods silently |
| 18 | Local data journalism and story production | Newsrooms, independent journalists, students | All products | Search a topic, build a chart/map, inspect methodology, save it, combine it with narrative, and publish a reproducible story | Every published block retains source, metric, geography, period, transform, refresh/vintage, caveats, and live/frozen status |
| 19 | Public-program planning evidence library | Public agencies, nonprofits, regional partnerships | ACS, PEP, BLS, CDC, FBI, AG | Save approved indicator collections for recurring plans covering housing, workforce, health, safety, population, or rural development | The library provides context, not program-effect attribution; definitions and approved uses live in reviewed semantic documentation |
| 20 | Source coverage and data-quality explorer | Data stewards, analysts, advanced users | All products | Visualize freshness, revisions, suppressed values, missing periods, geography coverage, reporting participation, and definition changes before analysis begins | Quality states must come from source evidence and pipeline observations; never convert unknown, suppressed, or unreported values to zero |

## Recommended Product Bundles

The use cases become easier to discover when packaged into a small number of cross-source entry points:

1. **Place Profile:** use cases 1, 2, 13, and 17.
2. **Economy and Workforce:** use cases 3, 4, 5, 12, 14, and 16.
3. **Health and Community:** use cases 6, 7, and 13.
4. **Public Safety:** use cases 8 and 9.
5. **Agriculture and Rural Economy:** use cases 10, 11, and 12.
6. **Publishing and Evidence:** use cases 15, 18, 19, and 20.

These bundles are navigation and saved-configuration templates. They are not additional warehouse schemas and should not create hard dependencies between source pipelines.

## Suggested Delivery Sequence

### First wave: prove reusable cross-source composition

- Community conditions profile
- Population growth and service-demand planning
- Workforce availability and labor-market depth
- Evidence-backed grant needs assessment
- Source coverage and data-quality explorer

These establish the reusable profile, comparison, documentation, quality, and publishing patterns required by most later use cases.

### Second wave: specialized domain packages

- Community disease and illness burden
- Public-safety trend normalized by population
- Rural and agricultural economy profile
- Housing affordability and household pressure
- Aging population and health-service planning

### Third wave: advanced comparison and publishing

- Peer benchmarking
- Business location and market context
- Economic shock and recovery monitor
- Crime and economic-context explorer
- Agricultural production and price context
- Durable public-program evidence libraries and publishing workflows

## Product-Wide Definition of Done

A use case is ready only when:

- users can reach it without knowing warehouse schema or source API details;
- every metric resolves to a stable harvested catalog identifier;
- business definitions and analytical guidance come from reviewed semantic documentation outside the warehouse;
- source-native limitations, denominators, uncertainty, suppression, reporting coverage, revisions, and vintage are visible where applicable;
- charts and maps can be saved, exported, reopened in the source explorer, and embedded in a page;
- missing optional definitions never block source-derived data access;
- the experience avoids unexplained composite scores and unsupported causal claims;
- automated tests cover source attribution, query reproducibility, empty/partial states, and critical analytical warnings.

