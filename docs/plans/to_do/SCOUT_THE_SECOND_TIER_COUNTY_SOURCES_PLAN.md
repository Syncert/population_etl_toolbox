---
id: second-tier-county-source-scouting
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/tooling -q
  - ruff check .
---

# Scout the second-tier county sources

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

The almanac plan ranks sixteen government sources that would deepen county
and place pages. Six have their own plans (`acs-place-grain`,
`bls-qcew-county-wages`, `bea-regional-accounts`, `census-saipe-sahie`,
`census-building-permits`, `irs-county-migration`). The remaining ten are
valuable but each needs research before an implementation plan can be
honest about endpoints, layouts, licensing, geography, and suppression. This
plan's deliverable is that research, recorded as one implementation plan per
source in `docs/plans/to_do/`, each following the adapter checklist, so the
backlog grows with plans an agent can execute rather than a wish list.

## Sources to scout

| Source | Expected chapter | Key questions |
| --- | --- | --- |
| Census LEHD LODES | Work and Money | File layout per state and year; origin-destination versus residence and workplace area characteristics; block-level volume and the county aggregation the files publish |
| Census County Business Patterns | Work and Money | API dataset and variables; noise infusion and suppression flags; county and place coverage; NAICS level |
| CDC WONDER mortality and natality | Health, People | Query API terms of use (WONDER forbids some automated use at sub-national grain; decide whether only the downloadable files are permissible); suppression below ten; age adjustment |
| NCES Common Core of Data | New chapter: Schools | Files per year; district and school identifiers; mapping districts to counties and places through the geography layer; lunch-eligibility definitions |
| FHFA House Price Index | Housing | County and ZIP annual files; index base and revision policy; coverage gaps for thin markets |
| HUD Fair Market Rents and income limits | Housing | API and key; metro versus county geography; fiscal-year periods |
| FEMA National Risk Index and disaster declarations | New chapter: Land and Environment | Download formats; county and tract grain; expected annual loss units; declarations API |
| EPA Air Quality System and NOAA climate normals | Land and Environment | AQS API and key; monitor-to-county mapping; NOAA normals station mapping and the 1991–2020 base |
| FCC Broadband Data Collection | Housing or People | Public data downloads and terms; location-level grain and the published county and place aggregates; speed tiers |
| USDA ERS county codes and atlases | Land and Farms; peer grouping | Rural-urban continuum and typology code files and their vintages; Food Environment Atlas and SNAP layouts |

## Deliverables

1. For each source above, a plan file in `docs/plans/to_do/` with
   dispatcher frontmatter, a Why, the verified endpoint or file contract
   with the official documentation cited, the geography grain and how it
   resolves to the geography layer, the suppression and missing semantics,
   licensing or terms of use, the proposed adapter package name, the chapter
   it feeds, acceptance criteria per the adapter checklist, and open items.
   A source whose terms forbid the intended use gets a short record of that
   finding instead of a plan, filed in the product document.
2. `docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md` updated with links to the
   new plans and any sources declined.
3. `docs/plans/EXECUTION_ENVIRONMENTS.md` regenerated so every new plan is
   classified.

## Acceptance criteria

- Ten plan files (or nine plus a recorded decline) exist in
  `docs/plans/to_do/` with valid dispatcher frontmatter; the plan inventory
  test passes.
- Every plan cites the official documentation it verified and states grain,
  suppression semantics, and terms of use; none proposes ingesting a
  third-party composite ranking as fact.
- The product document links to each new plan; the documentation link test
  passes.
- `docs/plans/EXECUTION_ENVIRONMENTS.md` matches the plans; the plan
  environments test passes.

## Open items to resolve during implementation

- Whether Schools and Land and Environment become chapters in the chapter
  contract at this point or when their first source lands; record the
  decision in `place-pages` or its successor.

## Checkpoint

Next pickup: start with Census County Business Patterns and USDA ERS codes,
whose contracts are smallest, then CDC WONDER, whose terms of use decide the
most.
