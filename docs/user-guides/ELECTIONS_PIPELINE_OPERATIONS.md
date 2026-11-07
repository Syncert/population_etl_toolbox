# Operating the elections pipeline

The `elections_ingest` DAG captures and publishes who held each U.S. House,
U.S. Senate and governor seat, and when, and the MIT Election Data and
Science Lab's (MEDSL) certified results for president, Senate and House. It
is the source step (E-2) of
[the elections plan](../plans/in_progress/ELECTIONS_AND_ECONOMIC_PERFORMANCE_PLAN.md)
and follows [ADR-0014](../decisions/0014-economic-performance-by-party-and-seat.md):
seats are facts with dates and sources, a two-party margin is the Republican
share minus the Democratic share of their combined vote, and nothing here
scores, ranks or adds measures together.

## What is read

`src/data_ingestion_toolbox/elections/registry.py` names every stream. None
needs a credential.

| Stream | Provider | What |
| --- | --- | --- |
| `legislators` | `raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages` | `legislators-current.json` and `legislators-historical.json` (CC0 1.0) |
| `governors` | `query.wikidata.org/sparql` | one SPARQL query: holders of each state's governor position (P39), with start, end and party (CC0 1.0) |
| `county_president` | Harvard Dataverse `doi:10.7910/DVN/VOQCHQ` | the file matching `countypres_2000-20NN` |
| `state_president` | `doi:10.7910/DVN/42MVDX` | `1976-20NN-president` |
| `senate` | `doi:10.7910/DVN/PEJ5QU` | `1976-20NN-senate-state` (or `1976-20NN-senate`) |
| `house` | `doi:10.7910/DVN/IG0UN2` | `1976-20NN-house` |
| `county_president_history` | Algara & Amlani, `doi:10.7910/DVN/DGUMFI` (CC0 1.0, no guestbook) | `dataverse_shareable_presidential_county_returns_1868_20NN.Rdata`, data frame `pres_elections_release` |
| `county_senate_history` | the same dataset | `dataverse_shareable_us_senate_county_returns_1908_20NN.Rdata`, data frame `senate_elections_release` |
| `county_senate` | MEDSL `doi:10.7910/DVN/YB60EJ` | `senate_20NN` (the 2022 county file) |

For each MEDSL stream the run first captures the dataset's file list, then
the one file whose name matches the registered pattern, in its original
format (`?format=original`). A second matching file (a new edition beside the
old) is refused rather than guessed between; update the pattern deliberately.
Every capture is committed before parsing, so a replay never needs the
network.

### County returns before 2000 (Algara & Amlani)

MEDSL's county presidential file starts in 2000 and MEDSL publishes county
Senate results only from 2022 (2024 only by precinct). Algara & Amlani's
county dataset (version 1.1, 2022-09-04) fills county presidential results
for 1992 and 1996 and county Senate results for 1990 to 2020. Its files are
R `save()` files; `data_ingestion_toolbox.utility.rdata` reads them with the
standard library (pyreadr is AGPL-3.0 and needs pandas). Each row is one
county and contest with the Democratic and Republican nominees' votes and the
county's total, read as one row per nominee plus "(other candidates
combined)" for what is left, so a county's rows still add to its total.

- Only election years from 1990 are read.
- A Senate special in the same state and year (Minnesota 2018, Georgia 2020)
  is its own contest, told apart by seat class.
- An absent nominee is no row. A county with neither party's votes is
  quarantined `no_major_party_votes`; an identical repeated row keeps one
  copy (`repeated_row`); two copies that disagree are both quarantined
  (`conflicting_row`).
- Retired county codes read as their successor (`registry.COUNTY_SUCCESSOR`):
  Dade (12025) as Miami-Dade (12086), Shannon SD as Oglala Lakota, Wade
  Hampton AK as Kusilvak, and South Boston, Clifton Forge and Bedford cities
  added into Halifax, Alleghany and Bedford counties. Votes are summed, never
  apportioned. Broomfield, CO did not exist before 2001 and has no earlier
  rows. A MEDSL county row with a retired code (Bedford city before 2013,
  for example) is read the same way; its own total is dropped, so the
  county's total is the sum of its candidates' votes.
- Algara & Amlani's presidential rows from 2000 to 2020 are MEDSL's years.
  They stay in silver for `DQ-ELEC-007` and are never served, so a contest
  has one provider. The release key names the provider and file.

MEDSL's county Senate 2022 file repeats the statewide total in `totalvotes`
on every county row, so a county's total there is the sum of its candidates.

Every run re-reads each stream whole. A Congress, a governor or an election
year the warehouse never loaded is loaded by the next scheduled run; there is
no separate history run (the loader rule).

## What is served

| Relation | Rows |
| --- | --- |
| `gold_elections.seat_term` | one row per span a person held a seat with one party: `seat_label` (`WI Senate class 1`, `WI-02`, `VT-AL`, `WI governor`), `party_code`, `caucus_code` (an independent's caucus), `start_date`, `end_date` (NULL is serving), `source_label` |
| `gold_elections.contest_result` | one row per contest and area: Republican and Democratic votes, total, leading party and candidate; read from `contest_result_store`, which `publish_run` fills once per published read (`gold_elections.sync_contest_result()`) |
| `gold_elections.contest_tally` | the same contest rows computed from silver for any published read; read it for one `run_id` only |
| `gold_elections.candidate_total` | one row per candidate and area, every ballot line combined |
| `gold_elections.observation_revision`, `observation_latest` | the presidential general-election measures below, in the neutral observation shape |

| Measure | Unit | Grains |
| --- | --- | --- |
| `president_two_party_republican_share` | percent | state, county |
| `president_two_party_margin` | percentage points (positive leans Republican) | state, county |
| `president_total_votes` | votes | state, county |
| `senate_two_party_republican_share` | percent | state, county |
| `senate_two_party_margin` | percentage points (positive leans Republican) | state, county |
| `senate_total_votes` | votes | state, county |

The Senate measures are the regularly scheduled race; a special held the same
year is in `contest_result` but not in the measures, so a place has one Senate
value a year.

Each observation is dated on election day and its release is the MEDSL file
it was read from (`MEDSL countypres_2000-2024.csv`).

How the source's shapes are read:

- **Party switches.** congress-legislators records a change of party inside
  a term (`party_affiliations`); the term is split there, so Jefferson Van
  Drew's 2019 term is a Democratic span and a Republican span.
- **Independents.** Bernie Sanders is `IND` with `caucus_code = DEM`; the
  party is never rewritten to the caucus.
- **Governors.** A governor carries the party they took office in: the
  Wikidata party statement whose dates cover the start, else the newest
  before it, else an undated one.
- **Party codes (decision L-2).** `REP`, `DEM`, `IND`, `OTH`. Fusion and
  state variants of a major party (Democratic-Farmer-Labor, Democrat-Liberal,
  Republican-Conservative) are that party; an "Ind. Democrat" is `OTH`. The
  source's text is kept beside the code.
- **Modes.** Where an area publishes a `TOTAL` line it is read alone; where it
  publishes only modes (election day, absentee, early) they are summed.
- **Fusion lines.** New York's candidates appear on several lines; they are
  one candidate, with the major party among their lines.
- **House districts** are contests, not observations: districts are redrawn
  after each census and are not yet a shared geography.

Quarantined, never served as zero: a county row with no county code (Maine's
UOCAVA rows, for example), an `NA` vote count, a duplicate row, a row with no
state.

## The Dataverse guestbook on the county and House files

MEDSL's county presidential and House files sit behind a Dataverse
guestbook (guestbook 458 on the county file, 2026-11-04); any results file
that answers this way is handled the same. Dataverse 6.10 and later answer a plain
download with HTTP 400 "required Guestbook response". The client then:

1. reads the guestbook (`GET /api/guestbooks/{id}`); a guestbook with a
   required custom question fails the run as `guestbook_questions_unanswered`,
   because this pipeline answers only the four standard fields;
2. POSTs the same download URL with
   `{"guestbookResponse": {"name", "email", "institution", "position", "answers": []}}`;
3. keeps the answer if it is the file, or follows the signed URL it returns.

The four answers are the operator's, set in `infra/docker/stack.env`:
`ELECTIONS_GUESTBOOK_NAME`, `ELECTIONS_GUESTBOOK_EMAIL`,
`ELECTIONS_GUESTBOOK_INSTITUTION`, `ELECTIONS_GUESTBOOK_POSITION`, and
`DATAVERSE_API_TOKEN` if Harvard's installation requires a signed-in user.
They are sent only to Dataverse, with each download (once a month), and are
never written to a capture or the repository. With none set, the county
and House streams fail as `guestbook_required` and the other streams
still load. The Algara & Amlani files have no guestbook.
Answering the guestbook records a download in MEDSL's guestbook log; read the
dataset's terms on its Dataverse page before setting them.

## Not yet verified against the live services

The build container could not reach Wikidata or Dataverse, so:

- The governor fixture's Wikidata identifiers are invented; the query's
  properties (P39, P5087, P1001, P580, P582, P102) are the documented ones.
  `tests/external/test_elections_source_contracts.py` (EXT-031) is the check
  that confirms them; run it before the first deploy.
- MEDSL's file names are taken from its published datasets; EXT-031 confirms
  each pattern matches exactly one file.
- MEDSL publishes its data for research use under the licence stated on each
  Dataverse dataset; confirm the terms on each dataset page before serving
  results outside the warehouse.
- The guestbook POST's answer shape (the file itself or a signed URL) is
  taken from Dataverse's guide, which does not show it; EXT-031 exercises it.
- The House file's header is unchecked until a guestbook answer lets it
  download.

State legislatures are not read; that waits on decision L-1 in the plan.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_elections_schema`, re-applies the package's DDL before any capture,
and `require_shared_geography` refuses to run before the shared geography is
loaded. Create the pool if deployment automation has not:

```text
airflow pools set elections_files 1 'Elections files (serialized)'
```

## Schedule

Monthly, on the 3rd at 07:00 UTC: members change between elections (deaths,
resignations, appointments) and MEDSL republishes after certification. A read
whose files are byte-for-byte the last published read's is recorded
`unchanged` and replays nothing.

## Checks after a run

```sql
SELECT stream, status, source_file, record_count, published_at
FROM control.elections_run ORDER BY created_at DESC LIMIT 6;

SELECT office, COUNT(*) FILTER (WHERE end_date IS NULL OR end_date > CURRENT_DATE) AS serving
FROM gold_elections.seat_term GROUP BY office;

SELECT metric_key, geo_type, COUNT(*), MIN(year), MAX(year)
FROM gold_elections.observation_latest GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-ELEC-001` (uniqueness) is enforced by the silver keys.
- `DQ-ELEC-002` fails on a read left unreplayed or a replayed read that
  reached no row, and runs in the daily sweep.
- `DQ-ELEC-003` fails when two current spans hold one seat on one day.
- `DQ-ELEC-007` warns where Algara & Amlani's county presidential margin
  differs from MEDSL's by more than half a point in a year both publish
  (2000 to 2020). Review its count before trusting the 1992 and 1996 maps.
- `DQ-ELEC-004` warns when a state's county presidential or Senate sums disagree with
  its state total, or a place does not resolve.
- `DQ-ELEC-005` fails when a lean-group county row is not its change minus
  the nation's, sits in a band its window-start margin does not name, or the
  all-county groups do not count exactly the county rows.

## Lean groups (ADR-0014)

`gold_elections.lean_place_change` holds, for each window between two
presidential election years from 2000, each county's change in five measures
against the nation's change over the same window, with the county's lean from
the vote in the window's first year. `gold_elections.lean_group_change` holds
each lean band within each peer group: the count of counties and the median
and quartiles of their change net of the nation. Nothing combines measures.

| Measure | Source | Change |
| --- | --- | --- |
| `real_income_per_person` | BEA CAINC1 line 3, deflated to the window's last year with the annual mean of FRED `PCEPI` | percent growth |
| `real_gdp` | BEA CAGDP1 line 1 (chained 2017 dollars; county GDP starts in 2001) | percent growth |
| `jobs` | QCEW annual average employment, all industries, total covered | percent growth |
| `unemployment_rate` | LAUS annual averages; the nation is the 51 states' unemployed over their labor force | points |
| `poverty_rate` | SAIPE, all ages | points |

Lean bands from the two-party margin (Republican minus Democratic share, in
points): `R20_plus` (20 or more), `R0_20`, `D0_20`, `D20_plus` (20 or more the
other way). An exact tie is in no band. Peer groups: `all`, `state`,
`rural_urban` (RUCC 2023 as large metro, other metro, nonmetro next to a
metro, remote nonmetro), `starting_level` (quarter of counties by the
window's first-year value) and `industry` (ERS 2025 industry dependence).
The RUCC and typology editions are the newest loaded, used for every window.

Inputs are read only for presidential election years, since a window starts
and ends on one; with fewer than two election years loaded the rebuild reads
nothing else and writes no rows.

A county appears only where every input names it by the same county code at
both ends of a window, so Connecticut's planning regions (2022 onward),
Alaska's election districts and BEA's combined Virginia areas drop out. The
annual PCE level deflates only; it is never served.

The `elections_lean_groups` DAG runs daily at 13:00 UTC and rebuilds only
when an input published since the last rebuild (`gold_elections.lean_refresh`
records `rebuilt` or `unchanged`). It is separate from `elections_ingest`,
which stays monthly because each MEDSL county or House download answers the
Dataverse guestbook with the operator's identity. To rebuild by hand:

```sql
SELECT * FROM gold_elections.refresh_lean_groups(TRUE);

SELECT measure, window_start_year, window_end_year, lean_band, place_count,
       net_change_p25, net_change_median, net_change_p75, nation_change
FROM gold_elections.lean_group_change
WHERE peer_type = 'all' AND window_start_year = 2000 AND window_end_year = 2024
ORDER BY measure, lean_band;
```
