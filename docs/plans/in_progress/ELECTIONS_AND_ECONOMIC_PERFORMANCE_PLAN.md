---
id: elections-and-economic-performance
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/elections tests/unit/api tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "elections" -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place shows who held its seats, how it voted, and how its economy fared against the nation

## Status

In progress: E-1 merged (#281), E-2 open on `claude/project-thread-moxcyd`
(2026-11-04). Written 2026-11-04 after Nick's discovery request ("understanding
better the economic performance of areas that voted republican ... maps of
voter histories that go back the same timespan, and a means to assemble net
economic performance and history of republican/democrat house/senate/governor
seats"). He chose **option B with state legislatures added** the same day.
The rules for how party and economics are shown are
[ADR-0014](../../decisions/0014-economic-performance-by-party-and-seat.md)
(proposed).

Discovery proposal with sources, options and mockups (invented values):
<https://claude.ai/artifact/NJW5bJZ8RXiURwkZuGSBPH> (private to Nick). Mockup
images: `/mnt/project-files/elections/01-04*.png`.

This lands after the dev-to-main release (#166). It is not part of v1.

The tax and policy proposal
(<https://claude.ai/artifact/BJ5y3sdnDkRPhDVvickfUx>) adds fiscal measures to
the governor-term table, the lean-group page and the paired maps. Its items
plug into E-4 and E-5 below without changing their shape.

## Scope (option B plus state legislatures)

In: seat terms for governor, U.S. Senate, U.S. House and party control of
each state legislative chamber; county presidential returns 2000 to 2024;
state presidential, Senate and House results 1976 onward; the places-by-lean
view; the term table; paired maps.

Out, recorded for later: county-to-House-district population weights and
district maps per Congress (option C); county returns before 2000 and county
governor and Senate votes (option D); district-grain economics from ACS.
Until option C lands, a county page lists its state's House delegation and
names the districts that touch it, without residents' shares.

## Sources

| Source | Holds | Grain | Years | Licence | Status |
| --- | --- | --- | --- | --- | --- |
| MEDSL County Presidential Returns (doi:10.7910/DVN/VOQCHQ) | Votes by candidate and party | County | 2000–2024 | Harvard Dataverse; believed CC0 | Confirm licence by hand (Dataverse refuses automated reads) |
| MEDSL U.S. House (doi:10.7910/DVN/IG0UN2) | Votes by candidate and party | District | 1976–2024 | Same | Same |
| MEDSL U.S. Senate (doi:10.7910/DVN/PEJ5QU) | Votes by candidate and party | State | 1976–2020 | Same | 2022–2024 from FEC or MEDSL precinct files |
| MEDSL U.S. President (doi:10.7910/DVN/42MVDX) | Votes by candidate and party | State | 1976–2024 | Same | Published by MEDSL (`1976-2024-president`, confirmed 2026-11-04) |
| [unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators) | Terms: chamber, state, district, class, party, start, end | Seat | 1789–now | CC0 1.0 (checked) | Ready |
| Governors file (built here) | Governor, party, start, end, source link | State | 1969–now, about 600 rows | Facts from NGA and state blue books, each row cited | To build |
| State legislative control | Party holding a majority of each chamber, per session | State chamber | 1969–now | Candidates: Klarner's partisan composition data (Harvard Dataverse), NCSL partisan composition (2009 onward), state blue books | Decision L-1 |
| [FEC Federal Elections](https://www.fec.gov/introduction-campaign-finance/election-results-and-voting-information/) | Certified federal results | State, district | 1982–2022, 2024 president | Public domain | DQ cross-check |

## Decisions

- **L-1. State legislature source.** Recommended: load Klarner's data for
  the years it covers if its licence permits redistribution, and extend to
  the present from NCSL's yearly tables; where neither permits it, build a
  cited file like the governors file. Nebraska is one nonpartisan chamber and
  is shown as such. Mixed control (a tie or a coalition) is its own value,
  never assigned to a party.
- **L-2. Party codes.** One dimension: Republican, Democratic, independent
  (with caucus), other, nonpartisan. A senator's caucus is shown beside the
  party when they differ.
- **L-3. Two-party margin.** Maps use the Republican minus Democratic share
  of the two-party vote, in points. The full result with every candidate is
  in the tooltip and the table.

## Deliverables

Each item is one PR, merged in order through the merge queue.

- **E-1. ADR-0014 accepted.** Docs only. Move ADR-0014 to Accepted once Nick
  approves the wording, with any fiscal section the tax thread adds.
- **E-2. Elections source.** `src/data_ingestion_toolbox/elections/`
  following the source-adapter template: registry (the MEDSL datasets, the
  congress-legislators files, `governors.yaml`, the legislature file),
  capture, silver (`silver_elections`), gold (`gold_elections.fact_seat_term`,
  `fact_election_result`, `dim_party`, `dim_office`), DAG `elections_ingest`
  (monthly, and on the loader rule: any registered election year or new
  Congress not yet loaded is loaded on a scheduled run), catalog ids, a
  `warehouse_inventory.py` entry, a user guide under `docs/user-guides/`.
  DQ rules: state totals within 0.5 percent of FEC; no overlapping terms for
  one seat; every day since 1969 has exactly one governor per state (acting
  governors included); two-party shares between 0 and 100; county totals sum
  to the state's within 0.5 percent.
- **E-3. Lean groups.** A derived gold relation
  `gold_elections.lean_group_change`: for each window, measure and peer
  type, each county's lean group at the window's start and its change net of
  the nation (ADR-0014 rules 1–5). Rebuilt when elections or any input
  measure publishes. Inputs already loaded: BEA CAINC1 line 3 (deflated),
  LAUS, QCEW, SAIPE, BEA CAGDP1 line 1; peer types from USDA ERS rural-urban
  codes and BEA CAINC5N industry mix.
- **E-4. API.** `apps/api/routers/elections.py`:
  `/places/{geo}/seats` (seat terms covering a place, with the term's net
  change per measure), election results served as map measures,
  `/elections/lean-groups` (window, measure, peer type). Reads scoped to one
  place or one grain-year; OpenAPI snapshot updated; EXPLAIN on OFFICE_LAB
  before merge.
- **E-5. Web.** A seat-control timeline in each place's Change chapter,
  sharing the year axis with the trend chart (lazy chunk: place pages have
  17 bytes of bundle budget spare); election chips and a paired voting map on
  `/map`; a places-by-lean page; a governor and legislature term table on
  state pages. ADR-0014 rule 7 wording on each.

## Sizing

| Item | Size |
| --- | --- |
| E-1 | Half a day |
| E-2 | 4–5 days (includes the governors and legislature files) |
| E-3 | 2 days |
| E-4 | 2–3 days |
| E-5 | 4–5 days |

Data size is small: about 30,000 county result rows per presidential
election and under 50,000 seat terms, well under 1 GB.

## Performance (prevention list applied)

- Seat reads scope to one state and its districts; `(state_fips, office,
  start_date)` index on `fact_seat_term`.
- The lean view reads the pre-computed relation, never joins raw economic
  facts per request.
- Paired maps read one grain-year of results; county tiles per on-screen
  state as today.

## Acceptance criteria

- Dane County, Wisconsin and the nation show seat timelines whose holders and
  dates match the sources for 1989 onward.
- The 2024 county presidential map matches MEDSL's county totals, and state
  sums match FEC within 0.5 percent.
- The lean page for 2000 to 2024, real income per person, with and without
  the rural-urban peer group, shows the group named, counts per group, and
  the ADR-0014 wording.
- No view shows a combined score or rank of measures.

## Tests

Unit tests for the registry, parsers, term-overlap logic, lean grouping and
net-of-nation arithmetic; database integration for the gold relations;
API contract tests; browser tests for the timeline, paired map and lean page
with fixtures under `tests/frontend/support/`.

## Open questions

- MEDSL licences and the Algara and Amlani pre-2000 county dataset need one
  manual check on Harvard Dataverse.
- Whether to add option C (House district weights and maps) straight after E-5.

## Checkpoint

2026-11-04, E-2 (elections source) built and held for go:

- `src/data_ingestion_toolbox/elections/`: six streams (congress-legislators
  current and historical, one Wikidata governor query, four MEDSL Dataverse
  files), capture-first, replayed into `silver_elections.seat_term` and
  `silver_elections.candidate_result`, served by `gold_elections` (seat terms,
  contest results, and three presidential measures at state and county in the
  neutral observation shape). DAG `elections_ingest`, monthly on the 3rd;
  every stream is re-read whole, which satisfies the loader rule.
  Operations: `docs/user-guides/ELECTIONS_PIPELINE_OPERATIONS.md`. Catalog
  ETL-111, EXT-031.
- Changed from the deliverable text, on purpose:
  - Governors come from Wikidata (CC0) rather than a hand-kept
    `governors.yaml`, so a new governor loads on the next run. EXT-031 must
    confirm the query on a machine that can reach Wikidata before the first
    deploy; the fixture's identifiers are invented.
  - Gold relations are `seat_term`, `contest_result`, `candidate_total`,
    `office_definition`, `party_definition` (views over the current read)
    rather than `fact_*`/`dim_*` tables.
  - DQ rules shipped: DQ-ELEC-001 keys, -002 reconciliation, -003 no
    overlapping spans for one seat, -004 county sums against the state
    (warn). Not shipped: the FEC comparison (no FEC source yet), "one
    governor every day since 1969" (Wikidata leaves gaps for acting
    governors; measure it on the first live load before making it a rule),
    and the 0-100 share bound (a CHECK-free view; the arithmetic cannot
    leave it).
- Not yet: the legislature file (decision L-1), the MEDSL licence check on
  each Dataverse page.
- First live check (OFFICE_LAB, 2026-11-04, EXT-031): legislators and state
  president passed; fixed on the PR: Wikidata unknown-value dates on one
  Alabama governor aborted the parse; the Senate file is now
  `1976-2024-senate-state.tab`; the House DOI is `10.7910/DVN/IG0UN2`; the
  county and House files need a Dataverse guestbook answer (operator
  identity in `stack.env`; whose identity is Nick's call). Rerun at c8a26241:
  8 of 10 pass (512 governor terms, none rejected; state president is now
  `1976-2024-president`); county and House wait on the guestbook identity.
- Next: E-3 lean groups, after E-2 merges and one live load on OFFICE_LAB.

2026-11-04, E-3 (lean groups) built on top of E-2 and held for go:

- `gold_elections/DDL/lean_groups.sql`: tables `lean_place_change` (county
  per window and measure) and `lean_group_change` (lean band within peer
  group: count, median, quartiles of change net of the nation), the
  definitions of measures, bands and peer types, and the rebuild functions.
  PL/pgSQL so the file does not depend on the other sources' gold DDL order.
  DAG `elections_lean_groups`, daily, rebuilds only when an input's
  fingerprint changed (`lean_refresh`). Catalog ETL-112; rule DQ-ELEC-005.
- Decisions applied: four bands from the window-start presidential margin
  (an exact tie is in no band); income deflated with the annual mean of the
  twelve monthly PCEPI values, internal only; change against the nation, one
  measure at a time, no combined score.
- Changed from the deliverable text, on purpose: the industry-mix peer group
  uses ERS's 2025 industry dependence code (already loaded, one code per
  county) instead of a CAINC5N classification; the nation's unemployment rate
  is the LAUS states' annual unemployed over labor force (LAUS has no national
  series and CPS's seasonally adjusted series has no annual average). RUCC and
  the typology are the newest editions loaded and serve every window.
- Known gaps: counties named differently across inputs drop out
  (Connecticut planning regions, Alaska districts, BEA's combined Virginia
  areas); windows end at 2024 until 2028's vote loads.
- Before merge: EXPLAIN ANALYZE of `gold_elections.refresh_lean_groups(TRUE)`
  on OFFICE_LAB (it scans BEA, QCEW and LAUS once per rebuild), then one
  live rebuild and a read of the 2000 to 2024 groups.
- Next: E-4 API (`/elections/lean-groups`), reading `lean_group_change` by
  window, measure and peer type.
