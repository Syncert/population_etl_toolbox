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
replacing MIT Election Lab (MEDSL) entirely if one source can cover it all.

**Decision S-1 answered 2026-11-07 14:28Z: free sources only.** Nick passed
on Dave Leip's Atlas (the only single source, but paid and needing a
redistribution contract). The route is Algara & Amlani plus MEDSL, with
House shown per county through the districts that cover it (R-2), not by
scraping (see "Scraping, assessed").

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
| 1 | **Dave Leip's Atlas** (uselectionatlas.org data store) | President, Senate, House (county **and** district), governor | President 1912–2020 (quadrennial; 2024 expected), Senate 1990–2022, House 1990–2022 (1990–1998 marked preliminary, Pennsylvania incomplete), governor 1990–2023; American University's copy carries Senate to 2024 | One national CSV per office and year with state, county and town FIPS codes, plus an Excel workbook | Paid per office-year. Individual licence ≈ $3,900 for president 1992–2024 + Senate 1990–2022 + House 1990–2022 (store prices, 2026-11-07); site licence ≈ 4×. Store terms: "Redistribution – to republish data for access by third parties – requires contract – please contact Dave". A public website needs that contract; its price is not published. Updates are free after purchase. | Only single source for all three offices by county from 1990. **Declined by Nick 2026-11-07** (paid; free sources only). |
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
  replace MEDSL entirely; Nick declined it (paid).
- **The free path (chosen) is two sources, not one:** Algara & Amlani for
  1990–2020 county president and Senate, MEDSL kept for everything it
  serves today plus county Senate 2022 →, and House per county through
  its covering districts (R-1b).

## Scraping, assessed (Nick asked 2026-11-07)

The one gap no free file fills is **House votes counted by county before
2016**. Candidates for scraping it:

| Candidate | What it has | County House? | Licence | Brittleness | Verdict |
| --- | --- | --- | --- | --- | --- |
| State election boards (50 canvass archives) | Official county-by-county statements of vote | Yes, in most states | Public records | Worst: 50 layouts, many 1990s years only as scanned PDFs needing OCR, URLs move each redesign; months of work and a per-state parser to keep alive | Rejected for v1; a single state could be added later if Nick wants one |
| Clerk of the House, *Statistics of the Presidential and Congressional Election* PDFs (1920 →) | Candidate totals | **No**, district totals only | Public domain | Medium (PDF tables) | Rejected: MEDSL already has these district totals |
| OurCampaigns | User-entered results, some with county breakdowns | Patchy | No republishing licence; user-contributed | High | Rejected |
| Wikipedia district articles | District results, rarely by county | Rarely | CC BY-SA (share-alike would bind the site's data) | High (free-form tables) | Rejected |
| Harvard Election Data Archive (precinct, about 2002–2010) | Precinct returns for some states | Partly, by summing precincts | Unclear; project page dated 2011 | Medium | Not v1; recheck from OFFICE_LAB if wanted |
| MEDSL precinct files (2016 →) | Precinct returns, all offices | Yes, by summing precincts | Free, Dataverse | Low | Possible later; not needed for the 1990 goal |

**Recommendation: no scraping.** A House member represents a district, not
a county, so the honest county view is "the House district(s) covering this
county, who won each, and by how much", built from MEDSL's district results
(1976 →, already loaded) and the Census Bureau's congressional district to
county relationship files (public domain). That covers 1990 → for every
county with no brittle parser. Its limit, said on the card: the margin
shown is the district's, not the county's own vote.

## Deliverables (one PR, after S-1)

### R-1. County president and Senate from Algara & Amlani; MEDSL kept

MEDSL stays the source for everything it already serves. Added streams:

| Stream | Source | Years served |
| --- | --- | --- |
| `county_results_algara_amlani` | Algara & Amlani, Harvard Dataverse (county president, Senate, governor, 1872–2020) | president 1992 and 1996 (MEDSL serves 2000 →); Senate 1990–2020; governor kept in silver only, not served in v1 |
| `county_senate_medsl` | MEDSL county-level Senate files for 2022 and 2024 (e.g. `doi:10.7910/DVN/YB60EJ` for 2022) | Senate 2022 → |

`registry.py` gains a provider field (`MEDSL`, `ALGARA_AMLANI`) and a
`SERVED_YEARS` rule per office so one contest has exactly one serving
provider: county president from Algara & Amlani only for 1992 and 1996,
county Senate from Algara & Amlani to 2020 and MEDSL after. The Algara &
Amlani download goes through the existing Dataverse client (and guestbook
handling if that dataset has one), from OFFICE_LAB only.

### R-1b. House by county through the districts that cover it

**As built (2026-11-07, second PR).** Census's relationship files give land
area only, and its API serves county pieces with population only for the
110th, 111th, 113th, 115th, 116th, 118th and 119th Congresses (probe on
OFFICE_LAB, `/mnt/project-files/probe_cd_county.out`). Stream
`house_county_share` reads those; `gold_elections.county_house_district`
serves House elections from 2006, a Congress without its own tabulation
using the one before it except where a state redrew (North Carolina 2020).
1990 to 2004 is House by district only. **Decided by Nick 2026-11-08:** no
county view from the UCLA district maps weighted by land area, because
spreading a district's votes by area is an estimate, not a count. County
pages say "House results before 2006 are shown by district only" for those
years, and the district results themselves stay on the state page.

*Superseded design (kept for the record): Census does not publish
population shares for the 103rd–109th Congresses or the 1980s districts,
so the stream below was not built.* A new reference stream
`cd_county_relationship` captures the Census
Bureau's congressional district to county relationship files, one per
Congress from the 103rd (1993) on, each giving the county's population
share in each district. `gold_elections.county_house_district` holds, per
county and Congress, each covering district, its population share, the
winner and the district two-party margin from the MEDSL House file. For
the 1990 election (102nd Congress, 1980s districts) the 1980s relationship
file is used if Census still serves it; if not, 1990 House is shown at
district grain only and the card says so. Nothing sums a district margin
into a county margin.

### R-2. Loader rule: never-loaded registry periods load on the scheduled run

`registry.py` lists every expected election year per stream (even years
from 1990 for Senate and House, presidential years from 1992). The run
compares the list with `control.elections_run`; a registered year with no
published capture is reported as `awaiting_publication` (the publisher has not
published it yet, e.g. a 2026 county Senate file) or loaded if its file is
present. There is no history run.

### R-3. Churn rule: unchanged rows are never rewritten

- A file whose bytes match its last published capture is recorded
  `unchanged` and replays nothing (as today).
- A revised file (a new MEDSL or Algara & Amlani version) replays
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

### R-5. Overlap between providers

Algara & Amlani and MEDSL overlap for county president 2000–2020. The overlap
is not served (R-1 names the serving provider) but is checked: DQ-ELEC-007
warns where a county's two-party margin differs by more than 0.5 points
between them. A large count of warnings means one file is wrong and is a
reason to stop and look before the 1992/1996 years are trusted.

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
| Connecticut planning regions (2022) | 2022 → | served at the grain the file publishes; where it still uses the old eight counties, the county map shows "not published for planning regions" |
| Alaska | all | results are by state house district, not borough: served at state grain only, the county map shows "not published by county" |

Merges are summed before the margin is computed, never averaged. A county
created after a year (Broomfield before 2001) shows "not published that
year", never zero.

### R-7. What pages and cards gain

| Where | Today | After |
| --- | --- | --- |
| Paired vote/economy maps | county president 2000 → | county president 1992 →; new office switch Senate (1990 →); House is not a county map (districts are not counties) |
| Seat timeline (place Change chapter) | seat holders 1969 → | each Senate term marked with the county's own Senate vote; each House term marked with the covering district's margin and the county's population share in it |
| Lean groups | windows from 2000 | windows from 1992 (LAUS county starts 1990; SAIPE county gaps 1990–92, 1994, 1996 are recorded gaps, so the poverty measure starts at 1997) |
| County election card | latest presidential margin | latest presidential, Senate and House margins side by side, each labelled with its election year; before 2006 the House line reads "House results before 2006 are shown by district only" |
| State page | state results 1976 → | unchanged |

House figures on a county are labelled "district result" with the
district's code, because a county can sit in several districts.

### R-8. Catalog, quality and tests

- Catalog rows for each new stream (ids assigned at merge time).
- DQ-ELEC-004 extended: county sums per state per contest vs the state
  total.
- DQ-ELEC-008: every registered year either published or `awaiting_file`.
- Unit fixtures for the Algara & Amlani layout (3 counties, built from the
  OFFICE_LAB sample), one CD relationship file, the crosswalk merges,
  repeats and conflicts.
- EXT-031 extended to read the Algara & Amlani dataset and the Census
  relationship file listing from OFFICE_LAB.

## Sizing

About 2–3 days of build, after one OFFICE_LAB sample download of the
Algara & Amlani file and one CD relationship file. Storage is
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
- Dane County's seat timeline names WI-02 for each Congress from the 103rd
  with the district margin and Dane's population share in it.
- Miami-Dade shows 1992 and 1996 results under 12086.
- Halifax, VA 1992 includes South Boston city's votes.
- Broomfield, CO 1996 reads "not published that year".
- A rerun with unchanged files writes zero silver or gold rows.
- DQ-ELEC-007 overlap warnings for 2000–2020 are counted and reviewed
  before 1992/1996 county maps go live.

## Open questions

- Algara & Amlani: licence, guestbook, file names, columns, and whether a
  2022/2024 edition exists. Dataverse refuses the cloud, so OFFICE_LAB reads
  the dataset page and downloads a sample first.
- ~~Whether Census still serves the 1980s district to county file for the
  1990 House election.~~ It does not serve population shares for it;
  1990 to 2004 House stays at district level (Nick, 2026-11-08).

## Checkpoint

**2026-11-07, first build PR (county president and Senate).** OFFICE_LAB read
the Algara & Amlani dataset: version 1.1 (2022-09-04), CC0 1.0, no guestbook,
no edition after 2020; county files are R `.Rdata` (gzip, `RDX3`, R 4.0.2).
MEDSL's YB60EJ is county Senate 2022 only; 2024 is precinct-only
(`doi:10.7910/DVN/ZCM3BN`, 166 MB).

Done in this PR:

- R-1: streams `county_president_history`, `county_senate_history` (one
  Dataverse dataset, one file each) and `county_senate` (MEDSL 2022, whose
  `totalvotes` is the statewide total and is ignored). The `.Rdata` files are
  read by `data_ingestion_toolbox.utility.rdata`, a standard-library reader
  (pyreadr is AGPL-3.0 and needs pandas; not added).
- R-2/R-3: every stream is re-read monthly; an unchanged file replays
  nothing (existing capture ledger).
- R-4: repeats, conflicts and rows with neither party's votes quarantined.
- R-5: Algara & Amlani presidential rows from 2000 are not served;
  DQ-ELEC-007 compares them with MEDSL's.
- R-6: `registry.COUNTY_SUCCESSOR` (renames and merged Virginia cities),
  applied to both providers.
- R-7 (gold only): Senate measures at state and county; presidential county
  measures for 1992 and 1996. No API or web change yet.
- Speed gate: the first OFFICE_LAB run timed out at 10 minutes (and so did
  dev's views at 90 s): `candidate_total` joined two scans of a CTE the
  planner estimated at one or two rows. Contest rows are now stored once per
  published read (`gold_elections.contest_result_store`, written by
  `sync_contest_result()` inside `publish_run`; an unchanged read stores
  nothing), and the totalling views partition by `run_id` so one read is
  totalled from the silver index.

Left:

- R-1b: House per county through the covering districts (Census
  congressional district to county relationship files), its own PR.
- ~~County Senate 2024 from MEDSL's precinct file (aggregate to county).~~
  Built 2026-11-08 as stream `county_senate_precinct`, served per state only
  where county sums match the statewide count (`DQ-ELEC-009`); see the ops
  guide.
- ~~Lean-group windows from 1992 (they still start in 2000).~~ Built: windows start in 1992.
- API and web: the Senate office switch on the paired maps; the seat
  timeline's vote per term.
