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

For each MEDSL stream the run first captures the dataset's file list, then
the one file whose name matches the registered pattern, in its original
format (`?format=original`). A second matching file (a new edition beside the
old) is refused rather than guessed between; update the pattern deliberately.
Every capture is committed before parsing, so a replay never needs the
network.

Every run re-reads each stream whole. A Congress, a governor or an election
year the warehouse never loaded is loaded by the next scheduled run; there is
no separate history run (the loader rule).

## What is served

| Relation | Rows |
| --- | --- |
| `gold_elections.seat_term` | one row per span a person held a seat with one party: `seat_label` (`WI Senate class 1`, `WI-02`, `VT-AL`, `WI governor`), `party_code`, `caucus_code` (an independent's caucus), `start_date`, `end_date` (NULL is serving), `source_label` |
| `gold_elections.contest_result` | one row per contest and area: Republican and Democratic votes, total, leading party and candidate |
| `gold_elections.candidate_total` | one row per candidate and area, every ballot line combined |
| `gold_elections.observation_revision`, `observation_latest` | the presidential general-election measures below, in the neutral observation shape |

| Measure | Unit | Grains |
| --- | --- | --- |
| `president_two_party_republican_share` | percent | state, county |
| `president_two_party_margin` | percentage points (positive leans Republican) | state, county |
| `president_total_votes` | votes | state, county |

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

## The county file's Dataverse guestbook

MEDSL's county presidential file sits behind a Dataverse guestbook
(guestbook 458 on 2026-11-04). Dataverse 6.10 and later answer a plain
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
stream fails as `guestbook_required` and the other five streams still load.
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
- The 2024 state presidential result is not in `1976-2020-president`; until
  MEDSL publishes a newer state file it can be summed from the county file
  (a later step), and is not served as a state observation now.

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
- `DQ-ELEC-004` warns when a state's county presidential sums disagree with
  its state total, or a place does not resolve.
