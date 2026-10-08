# Place Almanac Website Plan

Status: brainstorm, not an approved implementation plan. Drafted 2026-10-06.
Wireframes for every page are in
[`place_almanac_wireframes.html`](place_almanac_wireframes.html) (open it in a
browser). Numbers in the wireframes are placeholders, never data.

This document answers one question: what kind of public website should sit on
top of the warehouse so that an individual learns about their county, their
state, and the nation. It complements
[`TOP_20_DATA_PRODUCT_USE_CASES.md`](TOP_20_DATA_PRODUCT_USE_CASES.md), which
is written for professional users, and inherits every guardrail in it.

## Where the project is, and why the website feels stuck

The warehouse is further along than the website framing. Seven sources are
captured, conformed, and published with provenance. A versioned API serves
them. The Next.js app has catalog, explorer, compare, workbench, quality,
builder, article, and twenty use-case pages.

What exists is organized the way an analyst thinks: by tool (explore, compare,
workbench) and by professional use case (grant needs assessment, workforce
briefing). That is the right design for the people in the top-20 document. It
is the wrong front door for an individual. A resident does not arrive with a
use case. They arrive with a place and a question: "What is going on in my
county, and is that normal?"

**The pivot.** Organize the public site by place, not by tool or use case.
Give every county, state, and the nation one page with the same chapters in
the same order. Draw every number three times, county beside state beside
nation, so "is that normal" is answered by the picture itself. The existing
tools do not go away; they become the "dig deeper" layer one click below every
chart.

## What kind of website

Three patterns were considered.

- A **dashboard portal** (filters, dropdowns, a map) is what the app already is
  and what residents bounce off.
- A **news site** (articles first) depends on a publishing cadence that is not
  built yet and buries the evergreen value.
- An **almanac** wins: a reference page per place that is always true as of
  its stated periods, with an editorial layer on top for what changed.

Think of it as an encyclopedia article for every county, written entirely from
numbers that can be traced, with a monthly briefing attached. Three properties
the data supports and a YouTube channel needs:

1. **Nested levels on every chart.** County, state, and nation are the three
   grains every source except FRED publishes, and the geography master data
   already holds the containment. One visual grammar serves all 3,200
   counties. Fixed colours: County, State, United States.
2. **Same chapters everywhere.** People, Work and Money, Housing, Health,
   Safety, Land and Farms, Change. Each maps to a source bundle already
   published. A fixed chapter order is also a fixed episode rundown.
3. **Every number carries its period, source, and caveat.** The repository's
   guardrails (no composite scores, suppressed is not zero, association is not
   causation) become the site's voice rather than fine print.

## What the data can say about a place today

Read from the source configs and registries.

| Source | What it lets you say about a place | Grains published | Cadence | Caveat the page must show |
| --- | --- | --- | --- | --- |
| Census ACS | Age, households, race and ethnicity, nativity, mobility, commute, education, veterans, poverty, employment status, income distribution and medians, Gini, earnings, industry and occupation, housing stock, vacancy, tenure, rent and rent burden, home values and owner cost burden, health insurance, broadband | Nation, state, county (5-year for all counties; 1-year for populous ones) | Annual | Survey estimate with a margin of error; 5-year is a period, not a year |
| Census PEP | Population each July, births, deaths, natural change, international and domestic migration, net migration; vintages back to 2000 | Nation, state, county (places in some releases) | Annual, by vintage | Estimates are revised by vintage; the decennial count is April, not July |
| BLS LAUS | Unemployment rate and level, employment, labor force, participation, employment-to-population | State, county (national from CPS) | Monthly | Household concept; counties are not seasonally adjusted |
| BLS CES, CPI, JOLTS | Payroll jobs by industry, hours and earnings, consumer prices overall and for food, energy, shelter, rent, medical care; openings and turnover | Nation only | Monthly | National context; a national price index is not a local cost of living |
| FRED | Mortgage rate, housing starts and permits, median sale price, inflation measures, policy rate and yields, GDP, saving rate, retail sales, jobless claims | Nation only | Daily to quarterly | Macro backdrop; never implies a local cause |
| CDC PLACES | Model-based prevalence of chronic conditions, risk behaviors, prevention, and health status | Nation, county | Annual release | Modeled small-area estimates, not surveillance counts; confidence intervals |
| CDC CDI | Chronic disease indicators from state surveillance | Nation, state | Irregular | Indicator-specific populations and age adjustment |
| FBI UCR | Reported violent and property crime and eight component offenses | Nation, state, reviewed agencies (county roll-up is a to-do plan) | Monthly summarized, annual | Missing agency reports are not zero crime; participation and definition breaks |
| USDA NASS | Corn, soybeans, wheat, hay acreage, yield, production; census-of-agriculture county corn | Nation, state, county | Annual and in-season | Suppressed cells stay suppressed; combined counties are not counties |

Two gaps shape the plan. Safety at county grain depends on
`docs/plans/needs_review/COUNTY_CRIME_ROLLUP_FROM_AGENCY_REPORTS_PLAN.md`; until then
the county Safety chapter shows the state with a label, as the use-case pages
already do. Cities and towns have identities in the geography master data, but
ACS is ingested at county and above, so places are a later phase.

## Sitemap

| Route | Page | Purpose |
| --- | --- | --- |
| `/` | Find your place | Search, map, "use my location", three featured places |
| `/us/wi/dane`, `/us/wi`, `/us` | **Place page** | One per nation, state, county. Seven chapters, fixed order. The product. |
| `/us/wi/dane/vs/us/wi/brown` | Compare two places | Same chapters side by side, state and nation as reference lines |
| `/explain/<concept>` | Explainer | One concept per page, shared by every place page |
| `/map/<measure>` | One measure, every county | Map plus ranked table, one measure at a time, coverage shown |
| `/briefing/<yyyy-mm>` | Monthly briefing | What changed, published from an approved evidence packet |
| `/data` | Where the numbers come from | Public face of the quality explorer |
| `/studio` | Studio (operator only) | Render any card or chart as a 16:9 or 9:16 frame with its caveat and script notes |
| `/tools/*` | Dig deeper | The existing explore, compare, workbench, builder surfaces, unchanged |

## The pages and why each exists

### 1. Find your place (`/`)

An individual's first question is "where is mine". The home page does one
thing: get them to their county page in one action. The map is navigation, not
analysis. Draws on geography master data for search and the Martin county
tiles. YouTube: the trailer and every episode's call to action.

### 2. Place page (`/us/<state>/<county>`, `/us/<state>`, `/us`)

The product. One permanent, linkable page per place that reads top to bottom
like an almanac entry. Each chapter opens with two or three headline cards
drawn three times (county, state, nation), then one trend chart with state and
nation as reference lines, then a footnote naming period, source, and caveat,
and a "dig deeper" link into the explorer with the exact query.

Composition: the eight reviewed profile templates and the use-case sections
already assemble these measures; this page re-arranges them by place. ACS and
PEP fill People, Housing, and Change. LAUS and ACS fill Work and Money, with
CPI and FRED as a labeled national backdrop. PLACES fills Health at county
grain and CDI at state. FBI fills Safety at state until the county roll-up
lands. NASS fills Land and Farms where the county has published cells; the
chapter is omitted rather than shown empty.

YouTube: the page is the episode. Chapter order is the rundown, each headline
card is a scene, the trend chart is the b-roll.

### 3. Compare two places (`/us/wi/dane/vs/us/wi/brown`)

"Is that normal" has a second answer: "compared to where". Same chapter order
as the place page, folded in half. The comparison workspace's preflight rules
decide which measures are comparable; missing pairs are listed, not hidden.
YouTube: a "two counties" format.

### 4. Explainer (`/explain/<concept>`)

Every caveat the warehouse preserves gets one plain-language home, written
once and linked from everywhere. First dozen: what the unemployment rate
counts, why a margin of error exists, PEP estimates versus ACS surveys, why
missing crime reports are not zero, why the CPI is not a local cost of living,
what a suppressed cell means, why modeled health prevalence is not a case
count. Draws on the reviewed semantic definitions directory. YouTube:
evergreen "one number explained" videos.

### 5. One measure, every county (`/map/<measure>`)

Rankings are where public-data sites lose their integrity. This page allows
exactly one measure at a time, states period and denominator, shows uncertainty
where the source has it, and counts the counties with no published value
instead of painting them. No composite index. Draws on the explorer's map
wiring and the uncoloured-is-not-zero rule. YouTube: "where is it highest"
episodes.

### 6. Monthly briefing (`/briefing/<yyyy-mm>`)

The almanac is evergreen; the briefing is the reason to come back. Which
releases landed, what moved, which places it touched, written from evidence
blocks so every claim is reproducible. Depends on
`docs/plans/to_do/THE_PUBLISHING_APPROVAL_PATH_PLAN.md`; the briefing is that
path's first consumer. YouTube: the monthly episode, published the same day.

### 7. Where the numbers come from (`/data`)

A public, plain-language version of the quality explorer: last refresh per
source, geography covered, revisions, and the site's rules. The page a
skeptical commenter gets linked to.

### 8. Studio (`/studio`, operator only)

Pick any place, chapter, and card. Renders it at 1920x1080 and 1080x1920 with
large type, the three level colours, and the period, source, and caveat line
baked into the frame. A script-notes panel assembles the reviewed definition,
caveat, and exact request. Behind the existing bearer credential. Every video
frame comes from here.

## Making county and place pages deeper than state and nation

Readers will spend most of their time at county and place grain, so those
pages should be the richest. Four ways to get there, in rising order of cost.
The first two need no new source.

### 1. Use what is already ingested and not yet shown

The ACS configuration pulls roughly fifty tables at county grain; the current
templates use a handful. The unused tables are what residents ask about:
commute mode and time, broadband, health insurance, veterans, who moved in
during the last year, place of birth, housing stock age and type, vehicles per
household, industry and occupation mix, the full income distribution,
inequality, marital and living-alone status. PEP's components of change
(births, deaths, domestic and international migration) answer "why is the
population changing". All released PLACES measures are already offered for
counties. This is a chapter-depth change, not a pipeline change.

### 2. Use the geography master data as content

The versioned geography layer holds nation, state, county, and place
identities, boundaries, and the relationship bridge between them. Render it:
the cities and towns that overlap a county (the contract models places as
siblings of counties, not children, because places cross county lines);
neighbouring counties; metro or micro area membership; land area; and the
rural-urban classification once USDA ERS codes are added as a small reference
table. "Nearby and related places" turns a page into a browsable almanac and
gives Compare a sensible default pair.

### 3. Add county and place sources that fit the adapter contract

Each follows `docs/reference/ADDING_A_DATA_SOURCE.md`. Ranked by what it adds
for a resident per adapter built. All are government-published, county grain or
finer, openly licensed.

| Source | Adds to a county or place page | Grain | Chapter | Why it ranks here |
| --- | --- | --- | --- | --- |
| ACS at place grain | Everything the county page shows, for cities and towns; subject and data-profile tables give provider-computed percentages with margins | Place (5-year all; 1-year 65,000+) | All | Unlocks the place page; same adapter, one new geography level |
| BLS QCEW | Jobs and average weekly wages by industry, counted where the job is | County, quarterly | Work and Money | Most-asked unanswered question; BLS config already anticipates it |
| BEA regional accounts | Personal income, per-capita income, earnings by industry, transfers, county GDP | County, annual | Work and Money | The county as an economy, not a survey of residents |
| Census SAIPE and SAHIE | Annual county poverty, median income, uninsured, with intervals | County, annual | Work and Money, Health | Fresher than 5-year ACS, every county every year |
| Census Building Permits Survey | Housing units authorized by structure type | County and permit-issuing place, monthly | Housing | The one forward-looking local housing signal |
| IRS county-to-county migration | Where in-movers came from and out-movers went | County pairs, annual | People, Change | Turns PEP net migration into a story with named origins |
| Census LEHD LODES | Resident-to-workplace commuting flows | County and finer, annual | Work and Money | Explains the gap between residents and jobs |
| Census County Business Patterns | Establishments, employment, payroll by detailed industry | County and place, annual | Work and Money | The business mix of a town |
| CDC WONDER mortality and natality | Deaths by cause, births, suppression below ten | County, annual | Health, People | Behind every life-expectancy headline; suppression stays visible |
| NCES Common Core of Data | Schools, districts, enrollment, staffing, lunch eligibility | District and school, mapped to county and place | New chapter: Schools | Families ask about schools first |
| FHFA House Price Index | Repeat-sales price index | County and ZIP, annual | Housing | Local price change beside ACS values and FRED |
| HUD Fair Market Rents and income limits | Reference rent and income thresholds | County and metro, annual | Housing | A reference point readers recognize |
| FEMA National Risk Index and declarations | Expected annual loss by hazard, declared disasters | County and tract | New chapter: Land and Environment | Floods, tornadoes, wildfire are place facts |
| EPA AQS and NOAA climate normals | Air quality summaries; thirty-year normals | Monitor and station, mapped to county | Land and Environment | Weather is the most-searched local fact |
| FCC Broadband Data Collection | Availability by speed tier | Location, aggregated to county and place | Housing or People | Pairs ACS "has a subscription" with "could get one" |
| USDA ERS county codes and atlases | Rural-urban continuum, typology, food environment, SNAP | County | Land and Farms; peer grouping | Makes honest peer groups possible |

Keep off the ingest list: third-party composite rankings (county health
rankings, livability indexes, commercial home-value estimates) can be linked as
context but not ingested as facts, because they are the unexplained scores the
guardrails forbid. Any derived number (density from land area, a rate from a
count and denominator, a deviation from the state) belongs in a reviewed
derived product like the existing time aggregates (ADR-0007), labeled as
derived, never computed in the page.

### 4. Go below the county

ACS publishes 5-year tables for every tract and block group, and ZIP Code
Tabulation Areas sit beside them. With tracts in the geography master data, a
county page gains a "within the county" map for any measure, and a place page
can show its neighbourhoods. Roughly twenty-five times the county row count
per table, so this is its own phase, after place grain.

### What this does to the page

Two chapters are added where sources exist, Schools and Land and Environment,
so a county page's chapters become People, Work and Money, Housing, Schools,
Health, Safety, Land and Environment (absorbing Land and Farms), and Change. A
county page also gains a Nearby section from the geography data and a "What
makes this place distinctive" section listing the handful of measures where
the county differs most from its state, each shown separately with period and
uncertainty, never summed. State and national pages keep the same chapters
with fewer cards; the higher levels exist mostly as the reference lines on the
county's charts.

## The YouTube production kit

The site and the channel share one structure, so preparing a video is choosing
a page, not building a deck.

| Format | Source page | Rundown | Length | Cadence |
| --- | --- | --- | --- | --- |
| Your county in seven charts | Place page | Map cold open, seven chapters in page order, close on the data rules | 6 to 9 min | Weekly, any county |
| One number explained | Explainer | Question, short answer, diagram, what it is not, worked example | 3 to 4 min | Evergreen, a dozen to start |
| Where is it highest | Measure map | Map reveal, top ten, bottom ten, coverage note, "find yours" | 4 to 6 min | Biweekly, one measure |
| Two counties | Compare | Why these two, chapters side by side, what the data cannot say about why | 6 to 8 min | Monthly |
| Monthly briefing | Briefing | Releases, three evidence blocks, places that moved most | 5 to 7 min | Monthly, same day as the page |

Production rules: scene order equals chapter order; every frame comes from the
studio with the caveat line baked in; script notes come from the reviewed
definitions, so the voice-over says what the semantic documentation says and
nothing more.

## Build order against the repository

Most of the plan is rearrangement of what exists. The genuinely new work is
place routing, explainer content, the measure-map page, the studio renderer,
and the new county sources.

1. **Place pages for counties, states, and the nation.** Route by geography
   identifier, compose chapters from existing profile and use-case sections,
   add the three-level card and reference-line trend, render the unused ACS
   tables and PEP components, and add the Nearby section from geography data.
2. **Explainers and the studio.** First twelve explainers from the guardrails
   and semantic definitions. Studio as an authenticated page over the same
   chart components. The channel can start here.
3. **Briefings.** Implement the publishing approval path plan, then the
   briefing page as its first consumer.
4. **Measure maps and compare.** Map page over the explorer's wiring; two-place
   compare over the preflight rules.
5. **County depth sources.** ACS at place grain first, then QCEW, BEA, SAIPE,
   building permits, IRS migration, in the ranked order above, each as its own
   plan under `docs/plans/to_do/`.
6. **Sub-county geography.** Tracts and ZCTAs, after place grain.

## Implementation plans

Each piece of this plan is an approved, unclaimed plan under
`docs/plans/to_do/`, with dispatcher frontmatter and acceptance criteria.
Dependencies are declared in the plans themselves; the groupings below are
the build order above.

Web, over the published API:

- [`docs/plans/needs_review/PLACE_PAGES_FOR_COUNTIES_STATES_AND_THE_NATION_PLAN.md`](../plans/needs_review/PLACE_PAGES_FOR_COUNTIES_STATES_AND_THE_NATION_PLAN.md) — the product; every other web plan depends on its route and chapter contract.
- [`docs/plans/needs_review/NEARBY_AND_RELATED_PLACES_PLAN.md`](../plans/needs_review/NEARBY_AND_RELATED_PLACES_PLAN.md) — geography relationships served and shown.
- [`docs/plans/needs_review/WHAT_MAKES_THIS_PLACE_DISTINCTIVE_PLAN.md`](../plans/needs_review/WHAT_MAKES_THIS_PLACE_DISTINCTIVE_PLAN.md) — per-measure percentile rank among peers, API-derived, never summed.
- [`docs/plans/needs_review/EXPLAINER_PAGES_PLAN.md`](../plans/needs_review/EXPLAINER_PAGES_PLAN.md) — the first twelve explainers and their linking contract.
- [`docs/plans/needs_review/THE_STUDIO_RENDERS_VIDEO_FRAMES_PLAN.md`](../plans/needs_review/THE_STUDIO_RENDERS_VIDEO_FRAMES_PLAN.md) — operator-only frame renderer and script notes.
- [`docs/plans/needs_review/FIND_YOUR_PLACE_HOME_AND_DATA_PAGES_PLAN.md`](../plans/needs_review/FIND_YOUR_PLACE_HOME_AND_DATA_PAGES_PLAN.md) — the home page and the public data page.
- [`docs/plans/needs_review/ONE_MEASURE_EVERY_COUNTY_MAP_PAGE_PLAN.md`](../plans/needs_review/ONE_MEASURE_EVERY_COUNTY_MAP_PAGE_PLAN.md) — the measure map and ranked table.
- [`docs/plans/needs_review/COMPARE_TWO_PLACES_PLAN.md`](../plans/needs_review/COMPARE_TWO_PLACES_PLAN.md) — the place page folded in half.
- [`docs/plans/to_do/MONTHLY_BRIEFINGS_PLAN.md`](../plans/to_do/MONTHLY_BRIEFINGS_PLAN.md) — first consumer of the publishing approval path.

Warehouse, county and place depth:

- [`docs/plans/needs_review/ACS_AT_PLACE_GRAIN_PLAN.md`](../plans/needs_review/ACS_AT_PLACE_GRAIN_PLAN.md) — unlocks city and town pages.
- [`docs/plans/needs_review/BLS_QCEW_COUNTY_EMPLOYMENT_AND_WAGES_PLAN.md`](../plans/needs_review/BLS_QCEW_COUNTY_EMPLOYMENT_AND_WAGES_PLAN.md) — jobs and wages by industry where the job is.
- [`docs/plans/needs_review/BEA_REGIONAL_ACCOUNTS_PLAN.md`](../plans/needs_review/BEA_REGIONAL_ACCOUNTS_PLAN.md) — county personal income and GDP.
- [`docs/plans/to_do/CENSUS_SAIPE_AND_SAHIE_PLAN.md`](../plans/to_do/CENSUS_SAIPE_AND_SAHIE_PLAN.md) — annual every-county poverty, income, and uninsured estimates.
- [`docs/plans/in_progress/CENSUS_BUILDING_PERMITS_PLAN.md`](../plans/in_progress/CENSUS_BUILDING_PERMITS_PLAN.md) — housing units authorized.
- [`docs/plans/to_do/IRS_COUNTY_TO_COUNTY_MIGRATION_PLAN.md`](../plans/to_do/IRS_COUNTY_TO_COUNTY_MIGRATION_PLAN.md) — where people came from and went.
- [`docs/plans/to_do/SCOUT_THE_SECOND_TIER_COUNTY_SOURCES_PLAN.md`](../plans/to_do/SCOUT_THE_SECOND_TIER_COUNTY_SOURCES_PLAN.md) — research that produces one plan per remaining source.
- [`docs/plans/needs_review/SUB_COUNTY_GEOGRAPHY_PLAN.md`](../plans/needs_review/SUB_COUNTY_GEOGRAPHY_PLAN.md) — tracts and ZCTAs, after place grain.

## Decisions only the owner can make

- **A name.** The almanac framing wants a name a neighbour would say out loud.
- **Tone.** Reference-grade and calm, or curious and conversational.
- **Which county first.** Wisconsin is already the reviewed FBI agency sample,
  which makes a Wisconsin county a practical start.
- **Whether to promise cities at launch**, or add place-grain ACS to the
  backlog first and launch with counties.
