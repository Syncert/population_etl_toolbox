# Economic Data Studio Interface Manifesto

This is a product-design reference rather than an implementation plan. The MVP
routes, live catalog/explorer/profile/article/builder surfaces, browser-local
saved views, CSV export, source notes, and core responsive/error states are
implemented. Remaining product work includes durable server-side persistence,
accounts and permissions, reviewed publishing workflows, cross-product search,
advanced analytical visualizations and transformations, collaboration, version
history, embeds, and scheduled notifications.

The packaged Census PEP, CDC disease and illness, FBI crime, and agricultural
products described below are product intent and are not yet implemented data
sources. Census ACS, FRED, and BLS are the currently implemented source families.

**A build manifesto for a source-transparent public-data analytics platform**

---

## 1. Product Thesis

This product is not merely a dashboard website. It is a **data publishing platform** for packaged public demographic, economic, labor, health, public-safety, and agricultural data.

The platform should combine:

- The credibility and traceability of source-native public datasets.
- The flexibility of a self-service chart explorer.
- The narrative power of data journalism.
- The repeatability of saved chart configurations.
- The composability of custom article and webpage building.

The guiding product idea is:

> Build a web platform where users can explore public economic datasets, save chart configurations, and publish data-rich analytical articles or dashboards with live embedded charts.

The system should serve analysts, local economic development users, public-sector researchers, journalists, students, and technically curious users who want to move from raw public data to understandable insight.

### 1.1 Packaged Data Product Portfolio

The platform should present sources as understandable, source-native data products rather than as warehouse tables or API endpoints. The intended portfolio is:

| Packaged product | Primary value | Natural organization and required context |
| --- | --- | --- |
| Census ACS | Detailed demographic, social, housing, and economic characteristics | Survey product/vintage → table/group → variable → geography; estimates, margins of error, universe, and ACS 1-year/5-year coverage remain visible |
| Census Population Estimates Program (PEP) | Annual population levels, change, and demographic components between decennial censuses | Estimate vintage → population/component measure → geography → year; estimate basis, revision vintage, and geographic-boundary context remain visible |
| FRED | Discoverable macroeconomic and financial time series, including republished series | Category/release → series → observation/vintage; original publisher, units, frequency, seasonal adjustment, transformation, and revision context remain visible |
| BLS | Labor-market, employment, wage, price, and workplace series | Program/survey → series → geography/industry → period; survey basis, seasonal adjustment, units, and revisions remain visible |
| CDC Disease and Illness | Public disease, illness, mortality, hospitalization, and surveillance measures where source data permits | Dataset/surveillance system → condition/outcome → population/geography → period; case definition, denominator, age adjustment, suppression, provisional status, and reporting coverage remain visible |
| FBI Crime Data | Reported crime, offense, arrest, and agency-participation measures | Program → offense/measure → agency/geography → period; counts versus rates, reporting coverage, definition changes, and known non-reporting remain visible |
| Agricultural Data | Agricultural production, acreage, yield, inventories, prices, farm characteristics, and related rural measures | USDA program/dataset → commodity/measure → geography → period; unit, estimate/survey basis, suppression, revision, and coverage remain visible |

“Packaged” does not mean erasing source identity or manufacturing a universal score. Each product supplies curated navigation, stable identifiers, consistent provenance, useful defaults, source documentation, and reusable visualization entry points while preserving the source's natural structure and limitations.

All packages should expose a common product envelope—source, dataset/program, stable metric identifier, geography, period, value, unit, release/vintage, refresh time, lineage, and data-quality state—without forcing their internal entities into one physical warehouse model.

Automatically harvested facts come from the independent glossary process. Reviewed business definitions and analytical guidance come from the separate semantic-documentation workflow. User preferences remain outside the data warehouse. The application combines these failure-isolated concerns so users can work without querying database schemas directly.

The cross-source value propositions and recommended delivery sequence are detailed in [Top 20 Packaged Public-Data Use Cases](TOP_20_DATA_PRODUCT_USE_CASES.md).

---

## 2. Core Philosophy

The platform must be designed around four user motions:

```text
Catalog → Explore → Compose → Publish
```

Each motion must be supported cleanly.

| Motion | User Intent | Product Responsibility |
|---|---|---|
| Catalog | Find available datasets, indicators, geographies, and source metadata | Make data discoverable and trustworthy |
| Explore | Analyze indicators, compare places, transform time series, inspect tables | Make analysis flexible and fast |
| Compose | Combine text, charts, maps, tables, and stat cards into reusable pages | Make insight building modular |
| Publish | Share articles, dashboards, source pages, and saved chart views | Make outputs polished, credible, and traceable |

The product should not imitate Tableau, Superset, or Databricks dashboards directly. It should instead combine a **modern data newsroom** with an **analytical workbench**.

---

## 3. Non-Negotiable Design Principles

### 3.1 Source Transparency Comes First

Every chart, map, table, stat card, and published article must be traceable back to its source.

Every visualization should expose:

- Source and publisher: Census, FRED and any original publisher, BLS, CDC, FBI, USDA, or derived/internal.
- Dataset or program name.
- Metric or variable code.
- Geography level.
- Date range.
- Last refreshed timestamp.
- Transformation applied.
- Known limitations or caveats.

A user should never wonder, “Where did this number come from?”

### 3.2 Saved Chart Configurations Are First-Class Objects

Charts must not be disposable UI elements. A chart should be a persisted configuration object.

A saved chart configuration should allow:

- Reuse inside articles.
- Reuse inside dashboards.
- Cloning and modification.
- Source inspection.
- CSV export.
- API query inspection.
- Version tracking.
- Optional refresh behavior.

This is the foundation of the product.

### 3.3 Articles and Dashboards Share the Same Building Blocks

The platform should not maintain separate visualization logic for article embeds and dashboard widgets.

The same reusable components should power:

- Article chart embeds.
- Dashboard widgets.
- Geography profile cards.
- Indicator pages.
- Data catalog previews.
- Custom pages.

A chart created once should be usable everywhere.

### 3.4 Natural Source Shape Matters

BLS, FRED, Census, CDC, FBI, and agricultural data should not be forced into one generic interface.

Each source has a natural data shape:

| Source | Natural Shape | Interface Treatment |
|---|---|---|
| BLS | Program → Series → Geography → Time | Program/series explorer |
| FRED | Series-first, metadata-heavy, macro indicators | Searchable series catalog with transformations |
| Census ACS | Survey product/vintage → Table → Variable → Geography | Survey/table/variable browser with uncertainty context |
| Census PEP | Estimate vintage → Measure/component → Geography → Year | Population-change and components explorer with revision context |
| CDC | Surveillance dataset → Condition/outcome → Population/geography → Period | Health-measure explorer with denominator, suppression, and provisional-status context |
| FBI | Program → Offense/measure → Agency/geography → Period | Crime explorer with reporting-participation and definition context |
| Agricultural | Program/dataset → Commodity/measure → Geography → Period | Commodity and rural-data explorer with units, suppression, and survey context |

The site should normalize data enough to compare across sources, but preserve source-native browsing for trust and transparency.

### 3.5 The Interface Should Encourage Explanation, Not Just Exploration

Raw dashboards often leave interpretation to the user. This product should encourage explanation.

The interface should support:

- Narrative articles.
- Key takeaway callouts.
- Source notes.
- Methodology sections.
- Embedded chart explanations.
- Reusable data story templates.

The goal is not simply to show data. The goal is to help users explain what changed, where it changed, and why it matters.

---

## 4. Primary Navigation Model

The top-level navigation should be simple and durable.

```text
Home | Articles | Dashboards | Data Catalog | Explore | Builder | About
```

### 4.1 Home

The front door to the product. It should highlight key economic signals, featured analysis, search, and entry points into the catalog and builder.

### 4.2 Articles

A library of curated data stories that embed live or versioned chart configurations.

### 4.3 Dashboards

Reusable analytical pages for broad views such as national snapshots, state profiles, labor-market monitors, inflation trackers, and geography comparisons.

### 4.4 Data Catalog

The authoritative index of datasets, sources, indicators, variables, geographies, update cadence, and metadata.

### 4.5 Explore

The analytical workspace where users build charts, maps, comparisons, and transformations.

### 4.6 Builder

The composition interface where users assemble custom articles, dashboards, or webpages from saved blocks.

### 4.7 About

Explains source methodology, data refresh processes, limitations, project purpose, and contact/context.

---

## 5. Home Page Manifesto

The home page should feel like an economic intelligence portal, not a random dashboard collection.

### 5.1 Homepage Goals

The homepage must answer:

1. What changed recently?
2. What are the most important current signals?
3. Where can I explore deeper?
4. What data sources are available?
5. How do I start building my own view?

### 5.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Economic Data Studio                                      🔍  │
│ BLS + FRED + Census analytics for labor, population, inflation│
├──────────────────────────────────────────────────────────────┤
│ [Search indicators, geographies, articles, datasets...]       │
├──────────────────────────────────────────────────────────────┤
│  Key Signals                                                  │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│ │ US Unemp.   │ │ CPI Shelter │ │ WI Pop.     │ │ Labor Force│ │
│ │ 4.1% ↑      │ │ +5.2% YoY   │ │ 5.9M        │ │ 168.2M     │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
│                                                              │
│ ┌─────────────────────────────┐ ┌──────────────────────────┐ │
│ │ Featured Article            │ │ National Snapshot Map     │ │
│ │ “Where population growth is │ │ [County/state choropleth] │ │
│ │ outpacing labor growth”     │ │                          │ │
│ └─────────────────────────────┘ └──────────────────────────┘ │
│                                                              │
│ Recent Analysis                                               │
│ - Midwest labor markets are cooling unevenly                  │
│ - Shelter inflation remains sticky in Region 2                │
│ - County population growth favors large metros                │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 Design Highlights

The homepage should include:

- A prominent universal search box.
- Key economic signal cards.
- A featured article or data story.
- A featured map or national snapshot.
- Recently updated datasets.
- Popular indicators.
- A clear “Build your own page” action.

### 5.4 Accountability Standard

The homepage is successful when a new user can understand the product in less than one minute and can choose one of three paths:

- Read analysis.
- Explore data.
- Build a custom page.

---

## 6. Article Page Manifesto

Articles are a core differentiator. They should behave like data journalism pages with embedded live analytics.

### 6.1 Article Page Goals

An article page should:

- Present a clear thesis.
- Use narrative text to explain patterns.
- Embed saved chart configurations.
- Allow readers to inspect or clone embedded charts.
- Provide transparent source notes.
- Preserve analytical credibility.

### 6.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ ARTICLE                                                       │
│ Population Growth Is Concentrating in Fewer Counties          │
│ By Nicholas · Updated Jun 2026 · Sources: Census ACS, BLS     │
├──────────────────────────────────────────────────────────────┤
│ Summary                                                       │
│ Population growth has become increasingly concentrated in...  │
│                                                              │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ Embedded Chart: County Population Growth, 2015–2024       │ │
│ │ [Map / Line / Bar toggle]                                │ │
│ │ Filters: State [All]  Metric [Population Growth %]        │ │
│ └──────────────────────────────────────────────────────────┘ │
│                                                              │
│ Narrative Section                                             │
│ The strongest growth appears in counties surrounding major... │
│                                                              │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ Embedded Chart: Top 25 Counties by Growth                 │ │
│ │ [Horizontal bar chart]                                   │ │
│ └──────────────────────────────────────────────────────────┘ │
│                                                              │
│ Data Notes                                                    │
│ - Census ACS 5-year used for county-level stability           │
│ - BLS LAUS used for unemployment/labor-force comparisons      │
└──────────────────────────────────────────────────────────────┘
```

### 6.3 Required Embedded Chart Actions

Every embedded chart should support:

```text
[Open in Explorer] [Copy Embed] [View Source Data] [Download CSV] [Clone Chart]
```

### 6.4 Article Block Types

| Block Type | Purpose |
|---|---|
| Text block | Narrative body content |
| Heading block | Article structure |
| Chart embed | Saved chart configuration |
| Map embed | Saved map configuration |
| Table embed | Filtered source or transformed table |
| Stat card | Single metric highlight |
| Callout | Key takeaway or warning |
| Source note | Dataset and methodology explanation |
| Comparison block | Side-by-side region or indicator comparison |
| Divider | Visual structure |

### 6.5 Live vs Frozen Chart Behavior

The platform should support two publication modes:

| Mode | Behavior | Use Case |
|---|---|---|
| Live | Chart refreshes as source data updates | Current dashboard-like articles |
| Frozen | Chart remains locked to a specific data version | Historical analysis and reproducible writing |

The default for serious analysis should be explicit. The user must know whether a published article is live-updating or frozen.

### 6.6 Accountability Standard

An article is complete only when:

- Every embedded chart has source metadata.
- Every major claim is supported by visible data.
- The reader can open each chart in Explorer.
- Data caveats are visible.
- The article has a clear thesis, not just charts stacked vertically.

---

## 7. Chart Explorer Manifesto

The Chart Explorer is the analytical workhorse of the platform.

### 7.1 Explorer Goals

The Explorer should allow users to:

- Select a source.
- Select a dataset or series.
- Select a metric.
- Select geography.
- Select date range.
- Apply transformations.
- Compare geographies or indicators.
- Save the chart configuration.
- Add the chart to an article or dashboard.

### 7.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Chart Explorer                                                │
├───────────────┬──────────────────────────────────────────────┤
│ Data Source   │                                              │
│ ○ BLS         │  Chart: Unemployment Rate by State            │
│ ○ FRED        │  ┌────────────────────────────────────────┐  │
│ ○ Census      │  │                                        │  │
│               │  │          [Interactive Line Chart]       │  │
│ Dataset       │  │                                        │  │
│ [LAUS ▼]      │  └────────────────────────────────────────┘  │
│               │                                              │
│ Metric        │  Tabs: Chart | Table | Metadata | API Query   │
│ [Unemp Rate]  │                                              │
│               │  Chart Controls                              │
│ Geography     │  - Chart type: Line / Bar / Map / Scatter     │
│ [Wisconsin]   │  - Transformation: Raw / YoY / Indexed / MA   │
│               │  - Frequency: Monthly / Quarterly / Annual    │
│ Date Range    │  - Compare: US / State / County / Region      │
│ [2010–2026]   │                                              │
│               │  [Save Chart] [Add to Article] [Export]       │
└───────────────┴──────────────────────────────────────────────┘
```

### 7.3 Required Explorer Tabs

| Tab | Purpose |
|---|---|
| Chart | Primary visualization |
| Table | Underlying observations |
| Metadata | Source, metric, frequency, geography, update information |
| API Query | Reproducible endpoint/query view |
| Notes | Source caveats and transformation explanations |

### 7.4 Required Transformations

| Transformation | Purpose |
|---|---|
| Raw value | Show the original metric |
| Year-over-year change | Show annual growth or inflation |
| Month-over-month change | Show short-term movement |
| Indexed to 100 | Compare unlike units on a common baseline |
| Rolling average | Smooth noisy monthly data |
| Percent rank | Compare geographies across a distribution |
| Difference from national average | Benchmark local performance |
| Contribution to total | Show share of broader totals |
| Log scale | Make skewed county population data usable |

### 7.5 Chart Types

The Explorer should support:

- Line chart.
- Bar chart.
- Horizontal ranked bar chart.
- Choropleth map.
- Bubble map.
- Scatter plot.
- Histogram.
- Box plot.
- Small multiples.
- Data table.
- Stat card.

### 7.6 Accountability Standard

The Explorer is complete only when a user can go from raw source selection to saved reusable chart without leaving the page.

---

## 8. Data Catalog Manifesto

The Data Catalog is the trust layer of the product.

### 8.1 Catalog Goals

The catalog should let users browse and understand available data by:

- Source.
- Dataset.
- Topic.
- Geography.
- Frequency.
- Metric.
- Variable.
- Release cadence.
- Last update.

### 8.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Data Catalog                                                  │
├─────────────┬────────────────────────────────────────────────┤
│ Filters     │ Datasets                                       │
│             │                                                │
│ Source      │ ┌────────────────────────────────────────────┐ │
│ [BLS]       │ │ BLS LAUS - Local Area Unemployment Stats    │ │
│ [FRED]      │ │ Monthly labor force, unemployment, rates    │ │
│ [Census]    │ │ Geography: US, State, County                │ │
│             │ │ Updated: Monthly                            │ │
│ Frequency   │ │ [Open Dataset] [View Series] [API Details]  │ │
│ Monthly     │ └────────────────────────────────────────────┘ │
│ Annual      │                                                │
│             │ ┌────────────────────────────────────────────┐ │
│ Geography   │ │ Census ACS 5-Year Population Estimates      │ │
│ US          │ │ Demographic and population observations     │ │
│ State       │ │ Geography: State, County                    │ │
│ County      │ │ [Open Dataset] [Explore Variables]          │ │
└─────────────┴────────────────────────────────────────────────┘
```

### 8.3 Catalog Dimensions

| Dimension | Examples |
|---|---|
| Source | Census ACS, Census PEP, FRED, BLS, CDC, FBI, USDA |
| Topic | Demographics, population, labor, inflation, housing, income, health, crime, agriculture |
| Geography | US, region, state, county, metro |
| Frequency | Daily, weekly, monthly, quarterly, annual, periodic survey |
| Dataset | ACS 1-year/5-year, PEP, FRED releases, LAUS/CPS/CES, CDC surveillance datasets, FBI programs, USDA programs |
| Metric type | Estimate, rate, count, index, percent change, incidence, mortality, acreage, yield, price |
| Last updated | Recently refreshed data |

### 8.4 Dataset Card Requirements

Each dataset card should show:

- Dataset name.
- Source.
- Short description.
- Geography coverage.
- Time coverage.
- Frequency.
- Last updated.
- Available metrics or variables.
- Links to open, explore, and inspect API/source details.

### 8.5 Accountability Standard

The catalog is complete only when a user can discover what data exists, understand its shape, and launch directly into exploration.

---

## 9. Source-Native Page Manifesto

Each individual source must have a page that respects its natural structure.

### 9.1 Portfolio Expansion Requirements

In addition to the detailed BLS, FRED, and Census ACS examples below, the product requires source-native entry points for Census PEP, CDC disease and illness, FBI crime, and agricultural data. These pages must not be generic skins over a metric dropdown.

- Census PEP should foreground population change, estimate vintages, components of change, and revision history.
- CDC should foreground condition/outcome definitions, denominators, age adjustment, suppression, provisional data, and surveillance coverage.
- FBI should foreground program and offense definitions, agency participation/reporting coverage, counts versus rates, and breaks in comparability.
- Agricultural pages should foreground program, commodity, unit, production/acreage/yield/price distinctions, suppression, and survey or estimate basis.

Every packaged-product page must lead into the common Explorer, allow source inspection, and emit reusable saved configurations without discarding its specialized context.

---

## 10. BLS Source Page

BLS data should be organized around programs, series, geographies, and time.

### 10.1 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ BLS Data                                                      │
│ Bureau of Labor Statistics                                    │
├──────────────────────────────────────────────────────────────┤
│ Programs                                                      │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│ │ LAUS        │ │ CPS         │ │ CES         │              │
│ │ Local Area  │ │ Household   │ │ Payroll Jobs│              │
│ └─────────────┘ └─────────────┘ └─────────────┘              │
│                                                              │
│ Featured Series                                               │
│ - Unemployment Rate                                           │
│ - Labor Force                                                 │
│ - Employment                                                  │
│ - Unemployment Level                                          │
│                                                              │
│ Browse by Geography                                           │
│ [United States] [State] [County] [Metro]                      │
└──────────────────────────────────────────────────────────────┘
```

### 10.2 BLS Series Page

```text
BLS Series: Wisconsin Unemployment Rate
Source: BLS LAUS
Frequency: Monthly
Seasonality: Seasonally adjusted / not adjusted
Geography: Wisconsin
Series ID: ...

[Line Chart]

Tabs:
Chart | Observations | Metadata | Related Series | API Query
```

### 10.3 BLS Design Highlights

The BLS experience should emphasize:

- Labor-market programs.
- Employment, unemployment, and labor-force series.
- Geography browsing.
- Seasonality indicators.
- Monthly update behavior.
- Related series discovery.

### 10.4 Accountability Standard

A BLS page is complete only when the user can understand the program, series, geography, time coverage, and whether values are seasonally adjusted.

---

## 11. FRED Source Page

FRED data should be organized as a searchable series-first experience.

### 11.1 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ FRED Data                                                     │
│ Federal Reserve Economic Data                                 │
├──────────────────────────────────────────────────────────────┤
│ Search FRED Series                                            │
│ [CPI shelter, unemployment, mortgage rate...]                  │
│                                                              │
│ Popular Series                                                │
│ ┌───────────────────────┐ ┌───────────────────────────────┐  │
│ │ CPI Shelter           │ │ Federal Funds Rate             │  │
│ │ Monthly index         │ │ Daily / monthly rate           │  │
│ └───────────────────────┘ └───────────────────────────────┘  │
│                                                              │
│ Categories                                                    │
│ Inflation | Labor | Housing | Interest Rates | GDP            │
└──────────────────────────────────────────────────────────────┘
```

### 11.2 FRED Series Page

```text
FRED Series: CPI: Shelter
Series ID: CUSR0000SAH1
Frequency: Monthly
Units: Index
Seasonally Adjusted: Yes
Release: Consumer Price Index

[Line chart with recession shading]

Controls:
Raw | YoY % | MoM % | Indexed | Rolling Avg

Tabs:
Chart | Observations | Metadata | Transformations | Source Notes
```

### 11.3 FRED Design Highlights

The FRED experience should emphasize:

- Series search.
- Series IDs.
- Units.
- Frequency.
- Seasonal adjustment.
- Releases.
- Transformations.
- Recession shading where applicable.

### 11.4 Accountability Standard

A FRED page is complete only when a user can search, inspect, transform, and reuse a time series while understanding its units and release metadata.

---

## 12. Census Source Page

Census data should be organized around datasets, tables, variables, years, and geographies.

### 12.1 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Census Data                                                   │
├──────────────────────────────────────────────────────────────┤
│ Products                                                      │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│ │ ACS 1-Year  │ │ ACS 5-Year  │ │ Population  │              │
│ │ Larger geos │ │ County-safe │ │ Estimates   │              │
│ └─────────────┘ └─────────────┘ └─────────────┘              │
│                                                              │
│ Explore                                                       │
│ [Dataset] [Year] [Table] [Variable] [Geography]               │
│                                                              │
│ Example Tables                                                │
│ - B01003 Total Population                                     │
│ - B19013 Median Household Income                              │
│ - B25077 Median Home Value                                    │
└──────────────────────────────────────────────────────────────┘
```

### 12.2 Census Variable Page

```text
Census Variable: B01003_001E
Label: Total Population
Dataset: ACS 5-Year
Year: 2024
Geography: County

[Map]
[Distribution histogram]
[Top/bottom ranked counties]
[Raw table]

Tabs:
Overview | Geography Coverage | Raw Observations | Margin of Error | API Query
```

### 12.3 Census Design Highlights

The Census experience should emphasize:

- Dataset selection.
- Year selection.
- Table and variable browsing.
- Geography coverage.
- Estimate fields.
- Margin of error fields.
- ACS 1-year vs ACS 5-year distinctions.
- Map and distribution views.

### 12.4 Margin of Error Requirement

For ACS data, margin of error support is mandatory.

The interface should expose:

- Estimate value.
- Margin of error.
- Margin of error percentage where available.
- Visual indication when uncertainty is high.
- Source notes explaining ACS sampling uncertainty.

If ACS margin of error is ignored, the product loses credibility.

### 12.5 Accountability Standard

A Census page is complete only when a user can understand the dataset, variable, geography, year, estimate, and margin of error context.

---

## 13. Geography Profile Page Manifesto

Geography profile pages should provide an instant analytical summary of a place.

### 13.1 Geography Profile Goals

A profile page should answer:

- What is this place?
- What is its population trend?
- What is its labor-market condition?
- How does it compare to the state, nation, or peers?
- What source data supports the summary?

### 13.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Geography Profile: Dane County, Wisconsin                     │
├──────────────────────────────────────────────────────────────┤
│ Snapshot                                                      │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐ │
│ │ Population  │ │ Unemp. Rate │ │ Labor Force │ │ CPI Region│ │
│ │ 575,000     │ │ 3.2%        │ │ 330,000     │ │ Midwest   │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘ │
│                                                              │
│ ┌─────────────────────────────┐ ┌──────────────────────────┐ │
│ │ Population Trend             │ │ Labor Market Trend       │ │
│ │ [Line chart]                 │ │ [Line chart]             │ │
│ └─────────────────────────────┘ └──────────────────────────┘ │
│                                                              │
│ Benchmark Against                                             │
│ [Wisconsin] [United States] [Peer Counties]                   │
│                                                              │
│ Related Data                                                  │
│ Census ACS | BLS LAUS | FRED Regional CPI                     │
└──────────────────────────────────────────────────────────────┘
```

### 13.3 Required Components

| Component | Purpose |
|---|---|
| Snapshot cards | Quick facts |
| Population trend | Census anchor |
| Labor-force trend | BLS anchor |
| Unemployment trend | BLS anchor |
| Inflation region | FRED context where available |
| Peer comparison | Analytical benchmarking |
| Related articles | Connects data to narrative |
| Source coverage | Shows data availability and caveats |

### 13.4 Benchmarking Controls

A geography profile should support comparison against:

- Parent geography.
- United States.
- Neighboring counties or states.
- Peer geographies by population size.
- User-selected custom comparison group.

### 13.5 Accountability Standard

A geography profile is complete only when the user can understand a place’s economic and population context without manually assembling multiple charts.

---

## 14. Custom Page Builder Manifesto

The Builder is what turns the product from a dashboard into a platform.

### 14.1 Builder Goals

The Builder should let users create:

- Articles.
- Dashboards.
- Custom reports.
- Geography briefings.
- Indicator monitors.
- Public or private analytical pages.

### 14.2 Draft Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Page Builder: “Midwest Labor Market Monitor”                  │
├───────────────┬──────────────────────────────┬───────────────┤
│ Blocks        │ Canvas                       │ Settings      │
│               │                              │               │
│ + Text        │ ┌──────────────────────────┐ │ Page Title    │
│ + Chart       │ │ Header: Midwest Labor... │ │ Slug          │
│ + Map         │ └──────────────────────────┘ │ Visibility    │
│ + Table       │                              │ Theme         │
│ + Stat Card   │ ┌──────────┐ ┌──────────┐   │ Data Refresh  │
│ + Callout     │ │ WI Unemp │ │ IL Unemp │   │               │
│ + Divider     │ └──────────┘ └──────────┘   │ Selected Block│
│               │                              │ Chart Type    │
│ Saved Charts  │ ┌──────────────────────────┐ │ Filters       │
│ - CPI Shelter │ │ Multi-state line chart    │ │ Color Scale   │
│ - WI LAUS     │ └──────────────────────────┘ │ Source Notes  │
│ - County Pop  │                              │               │
│               │ [Preview] [Publish] [Save]   │               │
└───────────────┴──────────────────────────────┴───────────────┘
```

### 14.3 Builder Content Types

| Page Type | Description |
|---|---|
| Article | Narrative-heavy page with embedded charts |
| Dashboard | Grid-heavy, interactive analytical page |
| Report | Structured briefing with sections and exports |
| Profile | Geography or indicator-focused page |
| Landing page | Curated topic overview |

### 14.4 Builder Blocks

The Builder should support:

- Text.
- Heading.
- Markdown/rich text.
- Chart.
- Map.
- Table.
- Stat card.
- Callout.
- Source note.
- Methodology block.
- Divider.
- Section container.
- Filter controls.
- Comparison selector.
- Saved chart library.

### 14.5 Builder Settings

Each page should support:

- Title.
- Slug.
- Description.
- Visibility.
- Theme.
- Refresh mode.
- Live/frozen data behavior.
- SEO metadata where relevant.
- Source summary.
- Export/share options.

### 14.6 Accountability Standard

The Builder is complete only when a user can assemble a coherent page from reusable components without needing to code or manually duplicate chart logic.

---

## 15. Saved Chart Configuration Manifesto

Saved chart configurations are the backbone of the system.

### 15.1 Conceptual Configuration Example

```json
{
  "title": "Wisconsin Unemployment Rate",
  "source": "BLS",
  "dataset": "LAUS",
  "metric": "unemployment_rate",
  "geography": ["state:55"],
  "date_range": {
    "start": "2010-01-01",
    "end": "latest"
  },
  "chart_type": "line",
  "transform": "raw",
  "comparison": ["US"],
  "display": {
    "show_recession_bands": true,
    "show_source_note": true,
    "y_axis_format": "percent"
  }
}
```

### 15.2 Required Chart Config Fields

A chart configuration should include:

- Chart ID.
- Title.
- Description.
- Source.
- Dataset.
- Metric or variable.
- Geography selection.
- Date range.
- Chart type.
- Transformations.
- Comparison settings.
- Display options.
- Source metadata.
- Created by.
- Created timestamp.
- Updated timestamp.
- Version.
- Refresh behavior.

### 15.3 Chart Config Benefits

| Feature | Benefit |
|---|---|
| Embed in articles | Reuse the same chart across content |
| Clone chart | Modify without damaging original |
| Refresh automatically | Latest data updates everywhere |
| Track provenance | Every chart knows its source |
| Version changes | Published content can remain reproducible |
| Export | CSV, PNG, SVG, iframe, API query |

### 15.4 Accountability Standard

No chart should be embedded in a published page unless it exists as a saved or serializable configuration with source metadata.

---

## 16. Visualization Manifesto

Visualization should be purposeful, not decorative.

---

## 17. Time Series Visualizations

Time series charts are essential for BLS and FRED data.

### 17.1 Draft Example

```text
Unemployment Rate
%
8 ┤           ╭╮
7 ┤          ╯ ╰╮
6 ┤         ╯   ╰╮
5 ┤   ╭────╯     ╰──
4 ┤───╯
3 ┤
  └────────────────────
   2010   2015   2020   2025
```

### 17.2 Time Series Chart Types

| Chart | Best For |
|---|---|
| Line chart | Most economic indicators |
| Indexed line | Comparing unlike units |
| YoY bar chart | Inflation, population growth |
| Rolling average | Noisy monthly labor metrics |
| Recession-shaded chart | Macro context |
| Small multiples | Comparing all states or regions |

### 17.3 Accountability Standard

A time series chart is complete only when units, frequency, date range, source, and transformation are visible or inspectable.

---

## 18. Map Visualizations

Maps are strongest for Census and geography-level BLS data.

### 18.1 Draft Example

```text
┌────────────────────────────────────────────┐
│ County Population Growth                   │
│ [ Metric ▼ ] [ Year ▼ ] [ State ▼ ]         │
│                                            │
│          [ Interactive Choropleth Map ]     │
│                                            │
│ Hover: Dane County, WI                     │
│ Population: 575,000                        │
│ Growth: +8.4%                              │
└────────────────────────────────────────────┘
```

### 18.2 Map Types

| Map | Best For |
|---|---|
| Choropleth | County/state rates, growth, population |
| Bubble map | Absolute counts like population or labor force |
| Bivariate choropleth | Population growth plus unemployment |
| Ranked map | Top/bottom geographies |
| Difference map | Local metric minus national average |

### 18.3 Raw Population Warning

Raw population choropleths can mislead because high-population counties dominate the color scale.

For county maps, prefer:

- Growth rate.
- Percentile rank.
- Log population.
- Difference from benchmark.
- Per-capita rates.
- Binned categories.

### 18.4 Accountability Standard

A map is complete only when the color scale, geography level, metric, and tooltip are clear.

---

## 19. Distribution Visualizations

Distribution views are critical for county-level data because many public datasets are skewed.

### 19.1 Draft Example

```text
County Population Distribution
┌────────────────────────────────────────────┐
│ █████████████                              │
│ ███████                                    │
│ ████                                       │
│ ██                                         │
│ █                                          │
└────────────────────────────────────────────┘
 Small counties                    Large counties
```

### 19.2 Distribution Chart Types

| Chart | Purpose |
|---|---|
| Histogram | Show skew in county populations |
| Box plot | Compare states or regions |
| Percentile rank | Show where a county stands |
| Top/bottom table | Provide ranked insight |
| Scatter plot | Show relationships between indicators |

### 19.3 Accountability Standard

A distribution chart is complete only when the user can understand where a geography or observation sits relative to peers.

---

## 20. Comparative Scatter Visualizations

Scatter plots should reveal relationships across geographies or indicators.

### 20.1 Recommended Pairings

| X-Axis | Y-Axis |
|---|---|
| Population growth | Unemployment rate |
| Labor force growth | Employment growth |
| CPI shelter growth | Population growth |
| Median income | Population growth |
| Rent CPI | Labor-force participation |

### 20.2 Recommended Encoding

- Each dot should represent a geography or observation.
- Color should represent state, region, or category.
- Size should represent population or labor force where appropriate.
- Tooltip should expose source, geography, metric values, and year.

### 20.3 Accountability Standard

A scatter plot is complete only when axes, units, encoded dimensions, and tooltips are explicit.

---

## 21. Tooltip Manifesto

Tooltips are not decorative. They are miniature analytical summaries.

A strong tooltip should include:

- Geography or series name.
- Source.
- Metric name.
- Value.
- Date or year.
- Comparison value where applicable.
- Transformation if applied.
- Margin of error where applicable.

### Example Map Tooltip

```text
Dane County, Wisconsin
Population: 575,000
Growth: +8.4% since 2015
Percentile Rank: 91st
Source: Census ACS 5-Year
Year: 2024
MOE: ±2,100
```

### Accountability Standard

A chart is not complete until its tooltip helps the user interpret the data without needing to inspect a raw table.

---

## 22. Page Templates to Build First

The MVP should focus on reusable templates, not one-off pages.

### 22.1 Priority Templates

| Priority | Template | Reason |
|---|---|---|
| 1 | Home page | Sets product context |
| 2 | Data Catalog | Builds trust and discoverability |
| 3 | Indicator Explorer | Core analytical workflow |
| 4 | Geography Profile | High-value reusable public page |
| 5 | Article Viewer | Differentiates product from BI dashboards |
| 6 | Article/Page Builder | Platform-level capability |
| 7 | Source Pages | Makes BLS/FRED/Census transparent |

### 22.2 MVP Page Set

A strong MVP should include:

```text
1. Home
2. Data Catalog
3. Indicator Explorer
4. Geography Profile
5. Article Viewer
6. Article/Page Builder
```

### Accountability Standard

Do not overbuild exotic visualizations before these core page templates are usable.

---

## 23. Visual Design Direction

The visual style should be credible, calm, and modern.

### 23.1 Target Feel

The site should feel like:

```text
Modern data newsroom + analytical workbench
```

It should not feel like:

```text
Generic enterprise BI grid
```

### 23.2 Design Language

| Element | Direction |
|---|---|
| Background | Off-white or dark slate option |
| Cards | Clean, spacious, rounded, low clutter |
| Charts | Minimal axes, strong tooltips, clear legends |
| Typography | Editorial for articles, compact for catalog/explorer |
| Colors | Reserved, semantic, accessible |
| Maps | Muted basemap, clear hover states |
| Metadata | Visible but collapsible |
| Source notes | Treated as a product feature |

### 23.3 Color Philosophy

Use color to communicate meaning, not decoration.

Examples:

- Growth vs decline.
- Above vs below benchmark.
- Positive vs negative change.
- Selected vs unselected geography.
- Source identity only when useful.

### 23.4 Accountability Standard

The interface is successful when it feels trustworthy before it feels flashy.

---

## 24. Source Notes and Methodology Requirements

Every serious page should include source and methodology context.

### 24.1 Required Source Note Fields

- Source name.
- Dataset/program name.
- Metric/variable name.
- Time coverage.
- Geography coverage.
- Update cadence.
- Last refresh timestamp.
- Transformation description.
- Data limitations.

### 24.2 Methodology Examples

The product should explain issues like:

- ACS 1-year vs ACS 5-year usage.
- Margin of error.
- Census ACS survey estimates versus Census PEP population estimates and vintages.
- Seasonally adjusted vs not seasonally adjusted BLS data.
- FRED series units and transformations.
- CDC case definitions, denominators, age adjustment, provisional values, and suppression.
- FBI counts versus rates, agency participation, non-reporting, and definition changes.
- Agricultural production versus acreage, yield, inventory, and price measures; survey coverage and suppression.
- County-level availability.
- Missing data.
- Suppressed data.
- Source revisions.

### Accountability Standard

No published analysis should exist without enough methodology context for a user to judge whether the chart is appropriate.

---

## 25. Search Experience Manifesto

Search should cut across the entire product.

Users should be able to search for:

- Indicators.
- Datasets.
- FRED series.
- BLS programs.
- Census variables.
- Census PEP estimates and components of change.
- CDC conditions, outcomes, and surveillance datasets.
- FBI offenses, measures, programs, and reporting geographies.
- Agricultural commodities, measures, and programs.
- Geographies.
- Articles.
- Saved charts.
- Dashboards.

### 25.1 Search Result Types

| Result Type | Example |
|---|---|
| Indicator | Unemployment Rate |
| Geography | Dane County, Wisconsin |
| Dataset | BLS LAUS |
| Series | FRED CPI Shelter |
| Variable | Census B01003 Total Population |
| Population estimate | County annual population change from Census PEP |
| Health measure | CDC condition incidence or mortality measure |
| Crime measure | FBI reported violent-crime rate and reporting coverage |
| Agricultural measure | USDA county crop yield or production estimate |
| Article | Population Growth Is Concentrating |
| Saved Chart | Wisconsin Unemployment Rate Trend |

### Accountability Standard

Search is complete only when it routes users to action, not just discovery. A result should open, explore, compare, or add to a page.

---

## 26. Data Quality and Trust Manifesto

The product must be honest about data quality.

### 26.1 Data Quality Indicators

The UI should expose:

- Missing values.
- Partial coverage.
- Last update date.
- Source revision risk.
- Margin of error.
- Confidence limitations.
- Suppressed values.
- Unknown or derived values.

### 26.2 Data Status Labels

Recommended labels:

| Label | Meaning |
|---|---|
| Current | Refreshed successfully and up to date |
| Stale | Data has not updated within expected cadence |
| Partial | Some geographies or periods are missing |
| Derived | Value was transformed or calculated internally |
| Estimated | Estimate includes uncertainty or margin of error |
| Unknown | Source status unavailable |

### Accountability Standard

The system should never hide uncertainty or missingness to make the interface look cleaner.

---

## 27. User Workflow Manifesto

The target user workflow should feel continuous.

### 27.1 Ideal Workflow

```text
User searches “Wisconsin unemployment”
→ Opens BLS LAUS series page
→ Adjusts date range and comparison to US
→ Applies rolling average
→ Saves chart
→ Adds chart to article
→ Adds narrative text and source note
→ Publishes article
→ Reader opens chart in Explorer and clones it
```

### 27.2 Workflow Requirements

The product should make it easy to move from:

- Source page to Explorer.
- Explorer to saved chart.
- Saved chart to article/dashboard.
- Article/dashboard back to source data.
- Published page back to Explorer.

### Accountability Standard

If users hit dead ends between catalog, explorer, article, and builder, the product has failed its core workflow.

---

## 28. Recommended Feature Phasing

### 28.1 Phase 1: Foundation

Build:

- Data catalog.
- Basic source pages.
- Basic chart explorer.
- Saved chart config model.
- Line charts, bar charts, maps, tables.
- Source metadata panels.

### 28.2 Phase 2: Publishing

Build:

- Article viewer.
- Article builder.
- Chart embed blocks.
- Source note blocks.
- Live/frozen chart behavior.
- Public page routing.

### 28.3 Phase 3: Profiles and Dashboards

Build:

- Geography profiles.
- Indicator pages.
- National snapshot dashboard.
- State profile dashboard.
- County profile dashboard.
- Benchmarking controls.

### 28.4 Phase 4: Advanced Analytics

Build:

- Scatter plots.
- Small multiples.
- Bivariate maps.
- Percentile rankings.
- Automated insight summaries.
- Peer geography grouping.

### 28.5 Phase 5: Collaboration and Scale

Build:

- User accounts.
- Draft/published states.
- Version history.
- Sharing permissions.
- Embeddable iframes.
- Scheduled refresh notifications.

### Accountability Standard

Do not jump to advanced analytics before the catalog, explorer, saved chart configuration, and publishing workflows are stable.

---

## 29. Definition of Done for the Buildout

The buildout is not done when the UI displays charts. It is done when the platform supports a full analytical publishing workflow.

### 29.1 Product-Level Definition of Done

The platform is done when a user can:

1. Browse the packaged Census ACS, Census PEP, FRED, BLS, CDC disease and illness, FBI crime, and agricultural data products.
2. Inspect datasets in their natural source structure.
3. Create charts from source data.
4. Transform those charts meaningfully.
5. Save chart configurations.
6. Embed charts in articles or custom pages.
7. Build a geography profile or dashboard.
8. Inspect metadata and source notes.
9. Export or clone charts.
10. Publish a credible analytical page.

### 29.2 Interface-Level Definition of Done

Each interface is done when it has:

- Clear purpose.
- Clear navigation.
- Source traceability.
- Responsive behavior.
- Empty states.
- Loading states.
- Error states.
- Metadata visibility.
- Export or reuse actions where appropriate.
- Accessibility-conscious design.

### 29.3 Analytical Integrity Definition of Done

Each analytical object is done when it has:

- Source.
- Metric.
- Geography.
- Date range.
- Unit.
- Transformation.
- Refresh timestamp.
- Caveat handling.
- Tooltip clarity.
- Raw data inspectability.

---

## 30. Agent Accountability Checklist

An implementation agent should use this checklist throughout the buildout.

### 30.1 Before Building a Feature

Ask:

- What user motion does this support: catalog, explore, compose, or publish?
- Is this feature reusable across articles, dashboards, and profiles?
- Does it preserve source metadata?
- Does it respect the natural shape of its packaged Census, FRED, BLS, CDC, FBI, or agricultural source?
- Is this necessary for the MVP, or is it advanced polish?

### 30.2 While Building a Feature

Verify:

- The UI exposes loading, empty, and error states.
- The component has source metadata support.
- The component can be reused outside its first page.
- The component does not hard-code one dataset unnecessarily.
- The data model supports future expansion.
- The chart config is serializable.

### 30.3 Before Marking a Feature Complete

Confirm:

- User can understand what they are looking at.
- User can inspect where the data came from.
- User can reuse or export the result where appropriate.
- User can navigate to a deeper source or explorer view.
- The design does not hide uncertainty.
- The page works for realistic data volume and missing data.

### 30.4 Product Guardrails

Do not:

- Build one-off dashboard pages that cannot reuse chart configs.
- Hide source information.
- Treat ACS estimates as exact values without uncertainty context.
- Force unlike Census, FRED, BLS, CDC, FBI, and agricultural products into one generic data browser.
- Prioritize flashy visuals over trust.
- Build the page builder before saved chart configs are stable.
- Treat custom articles as static markdown with screenshots.

Do:

- Make every chart reusable.
- Make every page source-aware.
- Make every article analytically inspectable.
- Make every source browseable in a source-native way.
- Build toward publishing, not just visualizing.

---

## 31. Final Product North Star

The north star is not “a dashboard with public data.”

The north star is:

> A credible public-data studio where users can discover packaged Census ACS, Census PEP, FRED, BLS, CDC disease and illness, FBI crime, and agricultural data; explore it analytically; save reusable visualizations; and publish polished data stories or dashboards that remain source-transparent from end to end.

Every design, data model, and interface decision should be judged against that sentence.

If a feature does not strengthen cataloging, exploration, composition, publishing, or source trust, it should be questioned.

If a feature helps users move from public data to credible explanation, it belongs.

