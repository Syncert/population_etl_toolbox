---
id: election-results-back-to-1990
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/elections -q
  - python -m pytest tests/integration/database/test_elections_capture_replay.py -q -m database
  - ruff check .
---

# Election results back to 1990: president, Senate and House by county

## Status

To do. Drafted 2026-11-07 from Nick's ask in the project thread: one source
that shows Senate, House and presidential results back to at least 1990,
replacing MIT Election Lab (MEDSL) entirely if one source can cover it all,
rather than stitching several sources together.

**Waiting on decision S-1 (below).** The loader shape, geography mapping and
pages are the same under either answer, so build work can start on the
provider-neutral parts once S-1 is answered.

## What the site has today

From `src/data_ingestion_toolbox/elections/registry.py` (live since #287):

| Office | Grain | Years | Source |
| --- | --- | --- | --- |
| President | county | 2000–2024 | MEDSL `doi:10.7910/DVN/VOQCHQ` |
| President | state | 1976–2024 | MEDSL `doi:10.7910/DVN/42MVDX` |
| U.S. Senate | state | 1976–2024 | MEDSL `doi:10.7910/DVN/PEJ5QU` |
| U.S. House | district | 1976–2024 | MEDSL `doi:10.7910/DVN/IG0UN2` |
| Seat holders (House, Senate) | seat | 1969 → | unitedstates/congress-legislators (CC0) |
| Governors | state | 1969 → | Wikidata (CC0) |

So state-level and district-level results already reach 1976, and who held
every seat already reaches 1969. **The real gap is the county grain:**

- county president for 1992 and 1996;
- county Senate for every year (none is loaded);
- county House for every year (House is loaded only by district, and a
  district is not a shared geography; districts split counties).

## Candidates (researched 2026-11-07)

Ranked by Nick's order: one source for all three offices at county level
from 1990, free and licence-compatible with a public site, machine-readable,
stable between revisions.

| # | Source | Offices at county level | Years | Format | Cost and licence | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | **Dave Leip's Atlas** (uselectionatlas.org data store) | President, Senate, House (county **and** district), governor | President 1912–2020 (quadrennial; 2024 expected), Senate 1990–2022, House 1990–2022 (1990–1998 marked preliminary, Pennsylvania incomplete), governor 1990–2023; American University's copy carries Senate to 2024 | One national CSV per office and year with state, county and town FIPS codes, plus an Excel workbook | Paid per office-year. Individual licence ≈ $3,900 for president 1992–2024 + Senate 1990–2022 + House 1990–2022 (store prices, 2026-11-07); site licence ≈ 4×. Store terms: "Redistribution – to republish data for access by third parties – requires contract – please contact Dave". A public website needs that contract; its price is not published. Updates are free after purchase. | **Only single source that covers all three offices by county from 1990.** Needs a redistribution contract. |
| 2 | CQ Voting and Elections Collection (CQ Press / Sage) | President, Senate, House, governor | 1824 → | Web tables, CSV export | Institutional subscription; terms forbid redistribution. | Rejected: no public-site licence. |
| 3 | Algara & Amlani, *Partisanship & Nationalization in American Elections … U.S. Counties, 1872–2020* (Harvard Dataverse) | President, Senate, governor (no House) | 1872–2020 | Tabular, county FIPS | Free; Dataverse licence not yet read (Dataverse refuses automated reads from the cloud; OFFICE_LAB can read it). Academic replication data: may never be revised again. | Best free source for county president and Senate; still needs MEDSL for 2022–2024 and has no House. |
| 4 | MEDSL county Senate / House | Senate, House by county | precinct-derived only, 2016/2018 → | CSV on Dataverse | Free, guestbook. | Too recent. |
| 5 | ICPSR 13, *General Election Data for the U.S., 1950–1990* | President, Senate, House, governor | ends 1990 | Fixed-width | ICPSR terms: login, no redistribution. | Covers only the 1990 year; rejected. |
| 6 | ICPSR 8611, Clubb–Flanigan–Zingale county returns | President, Congress | 1840–1972 | Fixed-width | ICPSR terms. | Ends 1972; rejected. |
| 7 | OpenElections | Varies by state | mostly 2000 → | CSV per state per election | Free; coverage uneven, many states missing before 2004. | Rejected: not national before 2000. |
| 8 | Wikipedia / Wikidata | Some county tables | uneven | HTML tables | CC BY-SA (Wikipedia); county results not structured in Wikidata. | Rejected: not machine-readable at county grain, and share-alike. |
| 9 | NHGIS county boundaries | (geography, not results) | 1790 → | Shapefiles | Free with registration and citation. | Not needed for 1990 → (see Geography below); the Census Bureau's public-domain county-change list is enough. |

What the table settles:

- **No free source covers county House before about 2016.** House results
  are counted by district; only Leip has re-cut them by county for 1990 →.
- **Only Leip is "one source".** Under Nick's rule it is the only way to
  replace MEDSL entirely. The question is whether a redistribution
  contract is affordable.
- **The free path is two sources, not one:** Algara–Amlani for 1990–2020
  county president and Senate, MEDSL kept for 2022–2024 and for House
  (district only).

## Decision S-1: which source (Nick)

- **A. Leip, replace MEDSL results (recommended).** Nick (or the
  coordinator for him) emails Dave Leip for a redistribution quote covering
  county president 1992 →, Senate 1990 →, House 1990 → and future years on
  a public website, with attribution. No money is spent until Nick accepts
  a quote. On acceptance, MEDSL's four result streams are retired; seat
  holders (congress-legislators) and governors (Wikidata) stay, because
  they are free and already cover 1969 →.
- **B. Free: Algara–Amlani + MEDSL.** County president and Senate from
  1990; county House is not shown (House stays at district grain, as
  today). Two result sources, against Nick's preference.
- **C. Leave as is.** State and district results from 1976 stay; county
  maps start in 2000.

If A's quote is refused or too high, fall back to B without a new plan: the
loader below is provider-neutral.

## Deliverables (one PR, after S-1)

### R-1. A second result provider in the elections package

`registry.py` gains a `ResultProvider` (code, label, licence, citation) and
each `ResultDataset` names its provider. Streams added under A:

| Stream | File pattern (Leip names files by office and year) | First year |
| --- | --- | --- |
| `county_president_leip` | `President_General_County_<year>.csv` (confirmed on the first purchased file) | 1992 |
| `county_senate_leip` | `Senate_General_County_<year>.csv` | 1990 |
| `county_house_leip` | `House_General_County_<year>.csv` | 1990 |
| `district_house_leip` | `House_General_CD_<year>.csv` | 1990 |

Leip files are bought, not fetched: the operator drops each purchased file
into `ELECTIONS_LEIP_INBOX` (a folder set in `stack.env`, never in the
repository). The monthly `elections_ingest` run captures any inbox file it
has not captured before, keyed by SHA-256, exactly as the other streams
capture a download. Under B, the stream is `county_results_algara_amlani`
read from Dataverse with the existing guestbook client.

### R-2. Loader rule: never-loaded registry periods load on the scheduled run

`registry.py` lists every expected election year per stream (even years
from 1990 for Senate and House, presidential years from 1992). The run
compares the list with `control.elections_run`; a registered year with no
published capture is reported as `awaiting_file` (Leip, not yet bought or
dropped) or loaded if its file is present. There is no history run.

### R-3. Churn rule: unchanged rows are never rewritten

- A file whose bytes match its last published capture is recorded
  `unchanged` and replays nothing (as today).
- A revised file (Leip's free updates, a new Algara–Amlani version) replays
  into silver with `INSERT … ON CONFLICT … DO UPDATE … WHERE` the counted
  columns differ, so only the rows that changed get a new revision row.
- Gold rebuild for a year touches only that office and year's partition.
- Lean groups rebuild only when an input fingerprint changes (unchanged
  behaviour of `refresh_lean_groups`).

### R-4. Repeats and conflicts

Same rules as IRS and BPS: two byte-identical rows keep one and set the
repeat aside as `repeated_row`; two rows for one county, contest and
candidate that disagree are both quarantined as `conflicting_row`, never
summed or guessed between. A row with no county FIPS (overseas ballots,
Maine UOCAVA, "statewide writein") is quarantined `no_county_code`.

### R-5. Choosing between providers for one contest

Under A, MEDSL result streams are paused and then removed in the same PR, so
each contest has one provider. During the switch, a contest present in both
is served from the provider named in `registry.SERVING_PROVIDER` and
DQ-ELEC-007 warns where their county two-party margins differ by more than
0.5 points (a check on the purchase, not a served measure).

### R-6. Geography for changed counties (1990 →)

The warehouse's counties are current FIPS codes. Election files use the
code in force on election day. A reviewed crosswalk
`silver_elections.county_code_change` (from the Census Bureau's
"Substantial changes to counties" list, public domain) maps each retired
code forward; no votes are split by area.

| Change | Years affected | Rule |
| --- | --- | --- |
| Dade, FL 12025 → Miami-Dade 12086 (1997) | 1990–1996 | rename: map 1:1 |
| Shannon, SD 46113 → Oglala Lakota 46102 (2015) | 1990–2014 | rename: map 1:1 |
| Wade Hampton, AK 02270 → Kusilvak 02158 (2015) | — | Alaska reports by election district, see below |
| South Boston city, VA 51780 → Halifax 51083 (1995) | 1990–1994 | merge: add into Halifax |
| Clifton Forge city, VA 51560 → Alleghany 51005 (2001) | 1990–2000 | merge: add into Alleghany |
| Bedford city, VA 51515 → Bedford 51019 (2013) | 1990–2012 | merge: add into Bedford |
| Yellowstone NP, MT 30113 (1997) | 1990–1996 | votes ≈ 0: drop to `no_county_code` if present |
| Broomfield, CO 08014 created 2001 from four counties | 1990–2000 | no area split: Broomfield is "not counted that year", and its four parent counties are served as published |
| Connecticut planning regions (2022) | 2022 → | towns are published by Leip; regions are summed from towns where the town list is reviewed, else served at state grain only |
| Alaska | all | results are by state house district, not borough: served at state grain only, the county map shows "not published by county" |

Merges are summed before the margin is computed, never averaged. A county
created after a year (Broomfield before 2001) shows "not published that
year", never zero.

### R-7. What pages and cards gain

| Where | Today | After |
| --- | --- | --- |
| Paired vote/economy maps | county president 2000 → | county president 1992 →; new office switch Senate (1990 →) and, under A, House (1990 →) |
| Seat timeline (place Change chapter) | seat holders 1969 → | each Senate and House term marked with the county's own vote in that race (A; B Senate only) |
| Lean groups | windows from 2000 | windows from 1992 (LAUS county starts 1990; SAIPE county gaps 1990–92, 1994, 1996 are recorded gaps, so the poverty measure starts at 1997) |
| County election card | latest presidential margin | latest presidential, Senate and House margins side by side, each labelled with its election year |
| State page | state results 1976 → | unchanged |

House margins by county are labelled "votes cast in this county for the
House races covering it" because a county can sit in several districts.

### R-8. Catalog, quality and tests

- Catalog rows for each new stream (ids assigned at merge time).
- DQ-ELEC-004 extended: county sums per state per contest vs the state
  total.
- DQ-ELEC-008: every registered year either published or `awaiting_file`.
- Unit fixtures for one Leip CSV per office (built from the purchased
  layout, 3 counties), the crosswalk merges, repeats and conflicts.
- EXT-031 extended to read the Algara–Amlani dataset page from OFFICE_LAB
  under B.

## Sizing

About 2 days of build after S-1, plus the purchase lead time. Storage is
small: about 3,150 counties × 17 Senate/House years × ~3 offices × ~5
candidate lines ≈ 800,000 rows, well under 1 GB including revisions.

## Performance

County result reads are by `(office, election_year, geo_id)`; the new
tables carry that index. Maps read one office-year at a time (one index
range scan). Speed gate script before merge: EXPLAIN ANALYZE of the county
map read and of `refresh_lean_groups(TRUE)` with 1992 windows, in a
ROLLBACK transaction with `lock_timeout = 10s`; user-path reads under
500 ms.

## Acceptance criteria

- Dane County, WI shows county president margins for 1992 and 1996, and a
  Senate margin for 1990 onward.
- Miami-Dade shows 1992 and 1996 results under 12086.
- Halifax, VA 1992 includes South Boston city's votes.
- Broomfield, CO 1996 reads "not published that year".
- A rerun with unchanged files writes zero silver or gold rows.
- Under A, MEDSL result streams are removed from the registry and the
  catalog, and seat holders and governors still load.

## Open questions

- Leip's exact file names and columns: confirmed on the first purchased
  file (placeholder names above).
- Algara–Amlani licence and whether a 2024 edition exists: to be read on
  OFFICE_LAB if S-1 is B.

## Checkpoint

Not started.
