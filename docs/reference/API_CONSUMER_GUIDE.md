# API consumer guide

The stable contract for building against this API. Everything here is served
by the checked-in application and pinned by the reviewed OpenAPI snapshot
(`tests/fixtures/api/openapi_contract.json`), so a change to anything below
appears in review as a snapshot diff.

Companion contracts: [ADR-0002](../decisions/0002-api-versioning-and-deprecation.md)
(versioning and deprecation), [ADR-0003](../decisions/0003-saved-analysis-authentication-and-persistence.md)
(authentication and user-owned storage), and
[ADR-0001](../decisions/0001-data-layer-boundaries.md) (what the API may and
may not read).

## Versioning

**Every resource is served under `/api/v1`, and only there.** The unversioned
`/api` aliases that carried the original MVP paths were retired in API-008,
while the API had no downstream dependants; an unversioned data path now
answers `404`. There is exactly one public surface, so nothing can drift
between two of them.

`GET /health` and `GET /health/ready` (no `/api` prefix) are deployment
probes. They sit outside the version policy: they carry no data contract, and
versioning them would put a data-contract promise on infrastructure.

A `v1` change that would break a client belongs in `v2`; additive changes —
a new optional parameter, a new response field, a new operation, a relaxed
bound — land in `v1`. Deterministic ordering is part of the contract, because
a paging client depends on it whether or not anyone promised it.

## Discovering what to ask for

Start at the catalog; do not hard-code a source list or a per-source filter
list.

| Route | Answers |
| --- | --- |
| `GET /api/v1/catalog/sources` | Every published source system |
| `GET /api/v1/catalog/metrics` | Metric search and paging (`q`, `source_code`, `active_only`) |
| `GET /api/v1/catalog/metrics/{metric_code}` | One metric's full published semantics plus the routes that serve it; stable `404 {"detail": "metric_code not found"}` |
| `GET /api/v1/catalog/geographies` | Geography identities and attribution, from a projection refreshed on its own schedule (`geo_level`, `state_fips`, `q`, `active_only`) — see below |
| `GET /api/v1/catalog/capabilities` | **The route map.** Per source: route segment, whether the neutral routes answer, registered dataset identities, the exact routes that serve it with their query-parameter names, and `observation_filters` — the neutral filters that source supports |
| `GET /api/v1/catalog/freshness` | Per-source publication and freshness state from the warehouse's own signal |

**A value outside a closed set is refused, not answered empty.** `geo_level`
names a grain, and the vocabulary is closed: `NATIONAL`, `STATE`, `COUNTY`,
`PLACE`, `AGENCY`. A word outside it is a `422` naming the five, on every
route — `geo_level=COUNTRY` is not a grain with no rows, it is not a grain,
and an empty page would read as an answer about the warehouse. Case does not
matter and `NATION`/`US` are still accepted for `NATIONAL`, so a grain read
from the catalog can be sent straight back. `/cdc/observations` spells the
same filter `geo_type` and names the three grains CDC publishes.
`state_fips` and `county_fips` are refused the same way when they are not two
and three digits; a well-formed code that names no geography answers an empty
page, because that is a fact about the warehouse rather than the request.

A provider's own closed vocabulary is held to the same rule. On
`/usda-nass/observations` and `/usda-nass/series`, `source_desc` is `SURVEY`
or `CENSUS` — the two programs Quick Stats publishes under — and
`value_status` is one of `valid`, `missing`, `withheld`,
`insufficient_reports`, `not_applicable`, `not_available`,
`below_rounding_unit`, `quality_flagged`. On `/cdc/observations`, `dataset`
is one of the registered dataset identities `/catalog/capabilities` lists and
`adjustment` is `crude`, `age_adjusted` or `source_specific`. A word outside
any of these is a `422` naming the words, on every route that takes the
parameter; within one, case does not matter and the value is filtered as the
relation stores it. The status vocabulary is **per source** — CDC says
`suppressed` where NASS says `withheld` — so read the source's own words
rather than carrying one source's over.

**An empty filter value is no filter.** `?geo_level=`, `?commodity_desc=`
and `?release=` are the same requests as omitting them, on every route. A
client that serialises its whole parameter set therefore needs no special
case for the filters it is not using, and a saved configuration that records
`""` for a filter its source does not declare replays unchanged. The
exceptions are the two dates: `start_date` and `end_date` are typed, so an
empty value is malformed there rather than absent and is refused with the
field named.

`q` on `/catalog/metrics` and `/catalog/geographies` is a case-insensitive
**literal** substring search, not a pattern: `%` and `_` match themselves, so
`q=CENSUS_ACS` and `q=B01003_001` find those exact strings rather than
anything shaped like them. There is no wildcard syntax to reach for.

`observation_filters` is the contract for per-source filtering: a filter a
source does not declare is **rejected with a 422 naming the supported set**,
never silently ignored. `observation_dimensions` is the same kind of
contract for reading: the field names a neutral row's `dimensions` object
carries for that source. Both are published on `/catalog/capabilities` per
source and on `GET /catalog/metrics/{metric_code}` for the metric you
landed on, from one declaration, so discovering a metric never means
enumerating sources to learn the shape of its own rows. Read capabilities
once at startup rather than guessing.

`/catalog/geographies` answers a **projection refreshed on its own
schedule**, by the glossary reconciliation run rather than by the source
publishers. So a geography absent from it means "not projected yet", not "no
such geography": the observation routes can serve and fully attribute a
geography — `geo_id`, `geo_level`, the state and county names — before it
appears here. This is the same caveat this guide gives
[`/observations/latest`](#paging-a-history-and-what-orders-it), for the same
reason, and it matters more than it looks: the projection is also what
carries the geometry the vector tile layer publishes, so a geography it does
not hold yet has values and no shape. Build a picker from this resource, but
treat "not listed" as a statement about the projection, not about the
warehouse.

**A retired geography stays listed.** When a new boundary vintage stops
listing a geography -- a county consolidated, a place dissolved -- the
projection does not drop it. It publishes `geography_state: "retired"`,
`is_active: false`, and `retired_at`, and keeps the attributes the geography
was last published with. That is deliberate, and it is the contract
`freshness_state` already gives a metric: the served relations still hold
that geography's observations, so a catalog that hid it would leave rows a
client resolving geographies here could neither reach nor name. Decide in
your own client whether to show a retired geography; pass `active_only=true`
to have the API narrow the page for you, exactly as on `/catalog/metrics`.

`geo_name` is the geography's most specific published name -- its place name,
else its county name, else its state name, else its `geo_id` -- and it is the
same name here and on every observation route. It was not always: a place
answered under its own name here and under its state's name on
`/observations`, which is one geography with two names to anyone joining the
two responses.

## Observations

`GET /api/v1/observations` answers for **every** completed source (Census ACS,
BLS, FRED, Census PEP, CDC, FBI UCR, USDA NASS). The metric resolves to its
owning source through the published glossary and is read from that source's
own serving relations, so its semantics survive.

- `metric_code` (required), `limit`, `offset`.
- `scope=latest` (default) — the source's own latest publication.
- `scope=as_released` — every published release, each row carrying its release
  identity; add `release=<identity>` to pin one. A `release` without
  `scope=as_released` is a 422, because "the latest publication, but an older
  one" is a contradiction rather than a query.
- `newest_per_geography=true` — one row per geography: its newest published
  period. Valid only with `scope=latest`; sending it with
  `scope=as_released` is a 422, because an as-released read is one series
  per release and reducing it per geography would present whichever release
  sorted last as the value.
- `newest_release_per_period=true` — a **settled history**: one row per
  geography and period, from the newest release that published it. Valid
  only with `scope=as_released`, and not with a pinned `release`; both
  contradictions are a 422. The ranking is the source's own declared release
  order — the same order `/observations/releases` lists by — so you do not
  have to decide which release identity is newer, and cannot decide it
  differently from the warehouse.
- Per-source filters as declared by `/catalog/capabilities`: `geo_id`,
  `geo_level`, `state_fips`, `county_fips`, `stratum_id`,
  `adjustment_status`, `domain_desc`, `domaincat_desc`, `subject_type`,
  `subject_code`, `year_from`, `year_to`.

### A latest publication can be a series

`scope=latest` means the source's own latest publication, which is not
always one row per geography. Census PEP publishes every estimated year of
the current vintage, so `CENSUS_PEP:BIRTHS` at `geo_level=COUNTY` answers
18,864 rows: 3,144 counties times six years. That is the whole publication
and it is the right default.

If you want one value per geography — to colour a map, or to join against
another measure — pass `newest_per_geography=true` rather than paging the
publication and reducing it yourself. The ranking happens inside the
source's own relation, which is the only place that knows how its periods
order, and it is the same ranking `/distribution/bins` and
`/comparison/preflight` already apply. A page taken this way and a set of
bins therefore describe the same rows.

Because it is that ranking, a reduction declines the sources those routes
decline. CDC, USDA NASS and FBI UCR publish more than one row per geography
by design — strata, domains, agency subjects — so `newest_per_geography` and
`newest_release_per_period` answer 422 for them, carrying the same stated
reason `/distribution/bins` gives and naming the per-source filters that ask
the question those sources can answer. Reducing them anyway would present
whichever stratum the tie-break sorted first as the geography's value, with
`total` counting only the survivors: collapsing a grain you did not ask to
collapse.

`GET /api/v1/observations/releases?metric_code=...` lists a metric's published
releases newest-first with observation counts — this is how you discover what
`release=` accepts.

**A source whose latest publication is one row per geography has its history
across releases.** Census ACS serves only its newest vintage, so
`scope=latest` over one `geo_id` answers a single point rather than a trend.
That geography's history is every release that published it, which is what
`scope=as_released&newest_release_per_period=true` answers: each period as
its newest release left it.

### Census PEP spans six decades, and its measures do not

Census PEP publishes one series per decade, each its own product with its
own file, and the warehouse registers all of them. A county's population
therefore runs from 1971 to the current vintage, but the other measures
begin where the Bureau began publishing them:

| Measure | Published from |
| --- | --- |
| `CENSUS_PEP:POPESTIMATE` | July 1971 (July 1980 is absent; see below) |
| `CENSUS_PEP:CENSUSPOP` | April 1970, then each decennial count |
| `CENSUS_PEP:ESTIMATESBASE` | April 2000 |
| `CENSUS_PEP:BIRTHS`, `DEATHS`, `NATURALCHG` | July 2000 |
| `CENSUS_PEP:NPOPCHG`, `RESIDUAL`, the migration components and every rate | July 2000 |

Ask the catalog rather than this table: each measure publishes its own
first and last period, derived from the rows that actually loaded.

Three things follow from reading six publications as one series.

**A decennial count is not a July estimate.** `CENSUSPOP` is the April
enumeration that opens a decade and is a separate measure dated to 1 April.
Alabama's 2010 rows are 4,779,736 counted in April and 4,785,514 estimated
in July; neither is a revision of the other.

**July 1980 is absent, and left absent.** The 1970s table ends at 1979 and
the 1980s table opens on the April 1980 census rather than an estimate, so
the Bureau published no July 1980 county estimate in these products. The
gap is reported rather than interpolated.

**A year published twice answers once.** Decades overlap at their seams and
the Bureau republishes a decade once its following census can close it.
`scope=latest` answers the settled value: an intercensal series before a
postcensal one, then the newest vintage, then the product that publishes
that geography in its own right rather than as a rollup. Every publication
remains readable under `scope=as_released`, and each row names the
`dataset_code` and `vintage` it came from. Autauga County's 2005
population reads 4,569,805 from the intercensal series where the
postcensal file had projected 4,545,049.

The 1980s components of change are not served. The Bureau publishes them
by estimation period rather than by year, and its first and last periods
run 15 and 9 months, which this API's single observation date cannot state
without misreporting the period they cover.

### Reading a row honestly

Each row carries typed core fields plus the source's **declared** published
fields — `dimensions` is a reviewed set, not the serving relation's column
list, and `/catalog/capabilities` answers which fields it holds per source
under `observation_dimensions`. Read it once at startup, the way you read
`observation_filters`. When you need a relation's full shape, the
source-scoped routes serve it; that is what they are for.

- `value` is **text**, to preserve provider precision. Parse it yourself.
- `value` is **text**, and `null` whenever the source published no usable
  number — never zero. **Nothing is ever coerced to zero.**
- **Two sources shapes, and `publishes_value_status` on
  `/catalog/capabilities` tells you which you are reading.** Where it is
  `true` (CDC, FBI UCR, USDA NASS), an unpublished figure arrives as a row
  with `value: null` and a `value_status` saying why in the source's own
  vocabulary — `suppressed`, `withheld`, `not_reported`, each source's own
  word. Where it is `false` (BLS, FRED, Census ACS, Census PEP), the serving
  relations carry only published numbers: `value` is never null,
  `value_status` is always null, and a period the source published **without**
  a usable number is *absent from the series* rather than present and marked.
  If you chart a history from one of those sources, a gap is a gap — do not
  draw across it as though the period were continuous with its neighbours.
  The flag is on the metric resource too, so a client that searched the
  catalog does not have to enumerate sources to learn the shape of its own
  rows.
- `dimensions` carries the source's declared fields under the source's own
  published names — CDC strata and footnotes, FBI subject/offense/program,
  NASS commodity/domain/practice, Census dataset and vintage. The exact set
  per source is `observation_dimensions` on `/catalog/capabilities`, derived
  from the same declaration the rows are built from, so a field added to it
  reaches both at once and you never have to infer the shape from a row you
  happened to read.
- `uncertainty` is `null` when the source publishes none, and otherwise
  carries only the fields that source publishes — the rest stay `null`.
  Census ACS publishes `margin_of_error` and `margin_of_error_pct`; CDC
  publishes `confidence_lower` and `confidence_upper`; USDA NASS publishes
  `cv_value` with `cv_status` and `cv_symbol`, which is how it says an
  estimate is unreliable. Read them before treating a value as precise.
- `coverage` is `null` unless the source publishes a reporting basis. FBI UCR
  does: `participation_status` (a month nobody reported is `not_reported`
  with a `null` value, not zero crime), `coverage_percent` and
  `coverage_basis` for how much of the period was reported, and `population`,
  `participated_population` and `population_denominator` for what the count
  rests on.
- `release`, `as_of`, `source_record_id`, and `capture_id` trace a row back to
  its publication. What `release` *identifies* differs by source, because the
  providers differ, and the table below says so per source rather than
  leaving you to infer it.

### What a release identifies, per source

A release identity is only as strong as what the provider publishes. Three
kinds appear here, and conflating them is the mistake this table exists to
prevent:

| Source | `release` is | What it means |
| --- | --- | --- |
| Census ACS | `vintage_year` | The provider's estimate vintage. Two vintages are two ACS publications of the same period, which is why an ACS history is read per vintage |
| CDC | `release_watermark` | The provider's own release identity, carried through from the capture |
| FBI UCR | the release key | The provider's dataset release, with its own refresh date |
| USDA NASS | `release_watermark` | The provider's validated release |
| Census PEP | the release date | The Bureau's published release date for that vintage |
| BLS | `as_of` — the date the warehouse read the series | **Not a BLS publication.** The BLS response carries no release identity at all, so the honest identity is the read: the date this row's value was ingested |
| FRED | `as_of` — the date the warehouse read the series | **Not a FRED publication.** FRED publishes a revision window (`realtime_start`/`realtime_end`), and served rows now carry it — but the silver layer keeps one revision per observation, so the window on a row tells you which vintage that value belongs to, not the series' full revision history |

`realtime_start` and `realtime_end` on a FRED row are FRED's own vintage
window for that value: the period during which FRED considered it current. A
row ingested before the warehouse began capturing the window carries no dates
and reads as `0001-01-01` in the relation's keys — that is a fact about the
row, not a default, and those rows are not backfilled.

For BLS and FRED, then: a new release appears when a value is ingested that
differs from the one held, and re-serving the warehouse does not create one.
That distinction is load-bearing — the serving layer re-serves changed years
in chunks, and it used to stamp each row with the day its chunk was written,
so `/observations/releases` listed a "release" for every day a chunk happened
to be re-served and a full re-serve collapsed them all into one. It now
reports the ingestion behind each row, so `scope=as_released` lists one
release per distinct ingestion and `newest_release_per_period=true` settles
on the newest *value*, not the newest write.

### How a source identifies its metrics

A metric code is `SOURCE:<key>`, where the key is whatever that source's
publisher declares. Two shapes exist, and the catalog is what tells you which
you are holding — `source_object_type` on the metric's catalog row is
`series` or `measure`.

**BLS LAUS is published per measure.** The Local Area Unemployment Statistics
program codes a program, an area, and a measure into every series id
(`LAUCN010010000000003` is the unemployment rate for Autauga County, Alabama),
so publishing per series gave one metric per place and no BLS metric that
spanned geographies. LAUS therefore publishes seven measure-level metrics
whose observations span every published state and county:

| Metric code | Measure | Grains |
| --- | --- | --- |
| `BLS:LAU:UNEMP_RATE` | Unemployment rate | State, county |
| `BLS:LAU:UNEMP_LEVEL` | Unemployment level | State, county |
| `BLS:LAU:EMP_LEVEL` | Employment level | State, county |
| `BLS:LAU:LABOR_FORCE` | Labor force level | State, county |
| `BLS:LAU:EMP_POP_RATIO` | Employment-population ratio | State |
| `BLS:LAU:LFPR` | Labor force participation rate | State |
| `BLS:LAU:CNIP` | Civilian noninstitutional population | State |

The BLS series id is still on every row, under `dimensions.series_id`, so
lineage back to the provider's series is never lost. Grains are read from the
published rows, so `valid_geo_grains` on the catalog row is the authority —
do not assume every LAUS measure reaches counties.

The series-level LAUS codes are **retired catalog rows**: an existing link to
`BLS:LAUCN010010000000003` still resolves through
`GET /catalog/metrics/{metric_code}` and reports `freshness_state: "retired"`,
and `active_only=true` hides it from search. It no longer answers
observations, and the metric resource says so the way a client can act on:
a retired metric answers `served_by_neutral_routes: false` with
`observation_routes: []` and `observation_filters: []`, because no route
would answer it. `observation_dimensions` stays — it describes the rows the
warehouse published, which retirement does not withdraw. A saved
configuration or an evidence-packet block naming a retired metric is refused
on write and reported invalid on read, with a reason that says `retired`
rather than "not a published metric": the catalog entry is published, its
observations are not served.

Every other BLS program (CES, CPI, JOLTS, and the CPS national series) is
fixed-coded per series and keeps its series identity, so
`BLS:CES0000000001` is unchanged. The national CPS unemployment rate
(`BLS:LNS14000000`) stays a separate metric from `BLS:LAU:UNEMP_RATE`: it is a
different survey and seasonally adjusted, and BLS claims no comparability
between them.

### The geography vocabulary served rows carry

`geo_level` on a served row is always one of `NATIONAL`, `STATE`, `COUNTY`,
`PLACE` (Census PEP), or `AGENCY` (FBI UCR), and a metric's
`valid_geo_grains` in the catalog uses the same five words — so a grain read
from the catalog can be sent straight back as the `geo_level` filter and
will answer. A national row answers `geo_level=NATIONAL`. The filter is
case-insensitive, and it accepts `NATION` as an alias for `NATIONAL` because
the catalog published that word for CDC, PEP, and USDA NASS before the
vocabulary was unified; a saved configuration or a shared link holding it
keeps answering. Every source that publishes grains declares the `geo_level`
filter, including FBI UCR.

**Two routes take the grain under their provider's own name, and speak the
same vocabulary.** `/cdc/observations` calls it `geo_type` and
`/usda-nass/observations` calls it `agg_level_desc`; both accept the
vocabulary words case-insensitively with the same `NATION`/`US` aliases, and
both refuse an unknown grain by naming the words they publish. The parameter
names stay as they are — they are the providers' own — but you never have to
learn a second vocabulary to use them.

The vocabulary is one warehouse function, `gold_glossary.geo_grain(text)`,
which the publisher views and the serving routes both go through; a grain
in the catalog is derived from the rows a source actually serves, never
declared from configuration.

### Legacy observation routes

`GET /api/v1/observations/latest` and `/observations/timeseries` are the
original MVP shapes and answer for **only** Census ACS, BLS, and FRED (the
three sources in the cross-source union views). They retire with the
unversioned aliases. New work should use `/observations`.

Source-scoped routes remain for source-specific exploration:
`/api/v1/{bls,census,fred,pep}/observations/{latest,timeseries}`,
`/api/v1/cdc/observations`, `/api/v1/usda-nass/{observations,series,measures,source-notes}`.

### Which release you get, and what you get if you do not ask

**These defaults are not the same across routes.** Each resource says in its
own envelope which release selection answered, under its own field name:

| Read | How you ask | If you do not ask | The envelope says |
| --- | --- | --- | --- |
| `/observations` | `scope=latest` or `scope=as_released`, and `release=<identity>` to pin one | the source's newest publication | `scope`: `latest` or `as_released` |
| `/cdc/observations` | `release=<watermark>` to pin one; there is no history parameter | the newest release | `release_selection`: `latest_release` or `single_release` |
| `/usda-nass/observations` | `latest=true` for the newest validated release; `release_watermark=<watermark>` to pin one | **every published release** | `release_scope`: `as_released` or `latest` |

So the same bare request answers one release on `/cdc/observations` and the
whole revision history on `/usda-nass/observations`. NASS survey estimates
are revised until final, so an unqualified read there returns each figure
once per release that published it — chart it without `latest=true` and you
are plotting revisions as if they were observations. Read the envelope's
field rather than assuming: it is there to be checked.

`/cdc/observations` answers the newest release or one you name, and never the
history. For CDC revisions use `/observations?scope=as_released`, which
reaches every source through the same vocabulary.

### Paging a history, and what orders it

Every observation route — the neutral resource, the legacy pair, and the
source-scoped pair — takes `limit` and `offset`, and the envelope echoes the
page you asked for. The time-series routes gained `offset` in v1 as an
additive change; before that they counted rows into `total` that no parameter
could reach, and because history is served oldest-first the rows past `limit`
that went missing were the newest ones.

Each of those reads pages a **total order**, so two consecutive pages can
neither repeat a row nor skip one:

| Read | Ordered by |
| --- | --- |
| `/observations` | the source's own declared key for the scope you asked: its latest order for `scope=latest`, its as-released order for `scope=as_released`. Both end in a column no two rows of one metric share, which is why a reduction (`newest_per_geography`, `newest_release_per_period`) picks the same row every time |
| `/observations/releases` | the release ordering the source declares, descending, then the release identity itself — the group key, so no two rows can tie |
| `/observations/latest` | `geo_id` — the three union sources publish one latest row per geography |
| `/observations/timeseries` | `observation_date`, then the release identity the union carries: `as_of_date`, `dataset_code`, `vintage_year` |
| `/{source}/observations/latest` | `geo_id`, then the source's own remaining key — for Census PEP that is `observation_date`, `vintage_year`, `capture_id`, because its latest publication is a series |
| `/{source}/observations/timeseries` | `observation_date`, then the source's own remaining key (the BLS/FRED series, the Census dataset/vintage/variable, the PEP vintage and capture) |
| `/cdc/observations` | `asset_id`, `measure_id`, `value_type_id`, `geo_id`, `period_start`, `period_end`, `stratum_id`, then `observation_sk` — the stratified grain answers several rows for one measure and geography, so the surrogate key closes the order |
| `/usda-nass/observations` | `product_id`, `release_watermark`, `short_desc`, `geo_id`, the year, then `observation_sk` — a commodity published across several domain categories answers several rows carrying one `short_desc`, so the surrogate key closes the order |

A period can hold more than one row wherever a source republishes it, so
`observation_date` alone is not an order — pin the release with
`scope=as_released&release=…` on `/observations` if you want one publication's
series rather than all of them.

That is also why a reduction to one row per geography needs more than the
period. Where `/observations/latest` answers from the durable history — the
latest view refreshes independently, so an empty page there means "not
refreshed yet", not "no such data" — the row you get for each geography is
the newest period's newest published release: `as_of_date`, then
`dataset_code` ascending (so `acs1` precedes `acs5`), then `vintage_year`.
The same question on `/observations` is `newest_per_geography=true`, which
ranks inside the source's own relation by the order that source declares.

## Analysis

**Preflight before you compare.** `GET /api/v1/comparison/preflight?metric_code_a=…&metric_code_b=…`
returns the full compatibility verdict without moving data: `comparable`, the
`derivations` it would compute, every `rule` with a `pass` / `fail` /
`unknown` status and a reason, and `caveats`. An incompatible pair is a `200`
explanation, not an error; only an unknown metric code is a `404`.

Rules are evaluated over published semantics — units, time grains, geography
grains, aggregation characteristic, and whether the owning source has an
aligned analysis surface. **`unknown` is not incompatible**: where a source
publishes nothing to check (Census ACS publishes no units), the comparison is
served and the unverified rule travels as a caveat, naming which of
`metric_code_a` and `metric_code_b` published nothing.

Grains are compared in the vocabulary, not by spelling: the geography rule
reads the same five words and the same `NATION`/`US` aliases as the
`geo_level` filter, so a catalog row carrying a word the vocabulary replaced
shares a grain with one carrying its replacement rather than reading as a
measure published somewhere else.

Caveats also name what the comparison **cannot carry**. A comparison row
publishes `value_a`, `value_b`, `difference` and `ratio` and no uncertainty,
because the two sides' uncertainty vocabularies need not match and the
difference of two intervals is a statistic neither source published. Where a
side's source publishes one — Census ACS publishes `margin_of_error` and
`margin_of_error_pct` — the verdict says so and points at `/observations`,
where the published figures are. A `difference` is not more precise than the
estimates behind it.

`GET /api/v1/comparison` enforces exactly that verdict. Each side is reduced
to one newest value per geography inside its own relation before the join, so
a multi-period source cannot create Cartesian rows. Every row carries
`period_a`/`period_b` — the periods actually combined — and `value_a`/
`value_b` alongside the API-derived `difference` and `ratio`, which are named
in `derivations`. An incompatible pair answers `422` with the failed rules.

Two things about that answer are easy to read past.

The join is an **inner** one: a geography one side publishes and the other
does not is absent from the answer entirely, and `total` is the size of the
intersection, not of either measure. `geographies_a` and `geographies_b`
report how many geographies each side published under the same filters, so
you can see how much of each was paired — 500 counties out of 3,143 is a
different answer from 500 counties.

The two sides are **not aligned to a shared period**. Each is its own newest
value, so `period_a` and `period_b` can differ on any row, and `difference`
and `ratio` are then computed across two publications. Compare them per row
rather than assuming the pair is contemporaneous.

`GET /api/v1/distribution/bins` returns API-derived equal-width bins over one
metric's latest values, labelled `derived: true` with its `source_code` and
`units`. Counts are exact counts of provider-published numeric values; null,
suppressed, and missing values are excluded rather than binned.

`items` holds exactly `bin_count` entries, `bin_index` 1..`bin_count`, with
contiguous bounds running from `min_value` to `max_value` — a bin no
geography falls into carries `count: 0` rather than being left out, so you
can draw the histogram straight from `items`. Two degenerate answers differ:
a metric with no numeric values answers `total: 0`, null bounds and no items;
a metric whose values are all the same answers one bin whose bounds are that
value.

**The bins say which period they describe and what they could not carry.**
The reduction behind them ranks each geography's *own* newest period, so an
answer can be built from more than one: `period` is the single period every
binned row came from, or `null` when they differ, and `periods_differ` says
which case you are in. A mixed answer names no period on purpose — the
earliest or the latest would label a whole histogram with a period most of
it is not from. `caveats` carries what the analysis could not carry, the same
note `/comparison` publishes: where the source publishes an uncertainty,
equal-width bins draw boundaries the margins can straddle.

A metric whose source the API has not registered yet — the catalog answers it
with its published semantics and no routes — is declined by `/observations`
and `/distribution/bins` with the same `422` naming the source and pointing
here, never a `500`.

**The analysis routes answer for Census ACS, BLS, FRED, and Census PEP.** CDC,
USDA NASS, and FBI UCR are declined with a stated reason: they publish
stratified, multi-dimensional, or agency-grain observations that an aligned
one-value-per-geography analysis would silently collapse. Query them through
`/observations` with the appropriate stratum, domain, or subject filters.

## Saved analysis configurations

Authenticated, user-owned storage — see ADR-0003.

- `Authorization: Bearer <token>`, operator-provisioned.
- `GET|POST /api/v1/analysis-configurations`,
  `GET|PUT|DELETE /api/v1/analysis-configurations/{configuration_id}`.
- Documents are validated on write against the same capability and
  compatibility contracts above, so a saved configuration cannot encode a
  request the API would refuse.
- **A document carries only its own kind's fields.** One shape serves three
  kinds, and the three routes do not take the same parameters: an
  `observations` document carries `metric_code`, `scope`, `release` and the
  two reductions; a `distribution` document carries `metric_code` and
  `bin_count`; a `comparison` document carries `metric_code_a` and
  `metric_code_b`. A field belonging to another kind is refused rather than
  stored, because `/distribution/bins` and `/comparison` have no scope,
  release or reduction to send it to — a distribution saved "as released in
  2022" would reopen as the latest publication with nothing saying the pin
  was dropped. A field left at its default is not a refusal: it changes no
  request, so a document that simply spells `scope: "latest"` is unaffected.
- `filters` maps a filter name to **one value**, the value the route would
  receive as a query parameter. An array, an object, or a null is refused:
  these parameters are single-valued, so a document naming two cannot replay
  as what it says. A filter declared with an inclusive range — `year_from`,
  `year_to` — takes a whole number or its text, and refuses a fractional one
  or a boolean, exactly as the live route does. A filter whose values are a
  closed set or a closed shape is held to it here as well, for the same
  reason: `geo_level` must be a grain, and `state_fips` and `county_fips`
  must be two and three digits, so `state_fips: 6` is refused here exactly
  as `?state_fips=6` is refused there. Storage is not a back door for a
  request the API would refuse — a document that stored clean and replayed
  as a `422` is a broken link its owner never saw coming. Every other filter
  takes any single scalar within its declared length.
- An `observations` document records the reduction it was viewed with:
  `newest_per_geography` or `newest_release_per_period`, under the scope each
  belongs to. A view saved without one replays as the whole publication,
  which for a source whose latest publication is a series is a different set
  of rows — so store the one you asked for. Both default to `false`, and the
  same contradictions the live route refuses are refused here.
- On read, `validation` reports whether the document still matches live
  capabilities. A stale configuration is returned **unmodified** with
  `validation.valid = false` and a reason — the API never rewrites your
  content. This is also how a document stored before a rule tightened reads:
  a configuration carrying `geo_level: "NOPE"`, which storage accepted
  before it was checked, still opens, still returns exactly what was stored,
  and says why it will not replay.
- Updates send `expected_version`; a mismatch is `409` naming the current
  version. Deletion is immediate and permanent.
- These responses are `private, no-store` and are never publicly cached.

## Saved evidence packets

Authenticated, user-owned storage for composed evidence packets — see
ADR-0004. A packet is an ordered composition of blocks (narrative,
methodology, caveats, and analytical blocks that each carry a query plus the
reproducibility envelope the composer recorded). It is a separate resource
from a configuration: a configuration is a live question you re-ask, a packet
is a document you hand to someone else, so each analytical block embeds its
own query rather than referencing a configuration that could later change.

- `Authorization: Bearer <token>`, the same operator-provisioned token.
- `GET|POST /api/v1/evidence-packets`,
  `GET|PUT|DELETE /api/v1/evidence-packets/{packet_id}`.
- **Contradictions are refused; incompleteness is reported.** A write is
  `422`, naming the `block_id`, when an analytical block's envelope names a
  measure its query does not ask for, names a **source** its query does not
  read — the sources a query reads are the owning sources of its measures,
  not a field the composer decides — records a scope, a release, or a
  reduction (`newest_per_geography`, `newest_release_per_period`) its query
  does not, records a `geo_level` that is not one of the five grains at all,
  when a prose block carries a query or an envelope, when a block
  id repeats, or when a block's query is one the live routes would refuse.
  A source spelled in another case is the same source, not a contradiction,
  and neither is a grain: the vocabulary and its `NATION`/`US` aliases are
  the same here as on the routes.
  The reduction is checked for the same reason `period` is recorded: a block
  composed from one value per geography whose query replays the whole
  publication answers a different set of rows than the envelope describes.
  An analytical block that is still empty or partially filled is **stored**,
  and reported: `validation.blocks[]` names each block, whether it is valid,
  the reason, and the envelope fields still `missing`. A block whose measure
  was retired after it was stored is reported the same way, with the
  document returned unmodified — as is a contradiction a block was stored
  with before the envelope carried the field, which is read rather than
  repaired.
- **A list summary carries `block_count` and `analytical_block_count` and no
  validation verdict.** Validation is a detail-read concern; the absence of a
  verdict on a summary means *not checked*, never *valid*.
- No observation value is stored. A block replaying the latest publication
  is live; a block pinned to a release reproduces that release.
- Bounds: at most 100 blocks, 50 of them analytical; the serialized document
  is bounded by the request-body limit below.
- Updates send `expected_version`; a mismatch is `409` naming the current
  version. Deletion is immediate and permanent; deleting an account deletes
  its packets.
- These responses are `private, no-store` and are never publicly cached.

**Request bodies are bounded.** Every request body is limited to 256 KB
(`API_MAX_REQUEST_BODY_BYTES`), by declared `content-length` and as a
chunked body streams. Over the bound answers `413 {"detail": "..."}` before
any parsing. Public analytical reads carry no body and are unaffected.

## Errors

| Status | Meaning |
| --- | --- |
| `401` | Missing, malformed, unknown, or revoked bearer token. Identical for every case by design |
| `404` | Unknown identifier, or a configuration you do not own (indistinguishable on purpose) |
| `409` | Version conflict, or a name you already use |
| `413` | The request body is over the accepted size. Refused before parsing |
| `422` | A refused request. One the API can explain — an unsupported filter, a contradictory scope, an incompatible comparison, a reversed range, an invalid document — or one refused before the endpoint ran: a missing, malformed or out-of-bounds parameter. Two body shapes; see below |
| `429` | Rate limited. Honour `Retry-After` |
| `503` | The API cannot serve: database unavailable, a required serving contract missing, or (for saved analysis) storage not configured. The body is deliberately sanitized and never names warehouse objects |

Every status above is **declared in `/openapi.json`**, on each route that can
answer it, with the body it answers: `{"detail": "<sentence>"}` as the
`ErrorDetail` schema, and the two 422 bodies as a union of it and
`HTTPValidationError`. So a generated client has a branch for each of them,
and this table cannot change without a snapshot diff. The declarations follow
what the application does: a route declares a `404` only where it resolves an
identifier, a `409` only where it holds a name unique, a `413` only where it
parses a body, a `429` only where the limiter meters it (the health resource
and the deployment probes are exempt), and a `401` only where a token is
required.

A `503` never tells you which warehouse relation is missing — that detail
goes to the server log, because responses must not be usable to probe
deployment state. `/health/ready` is the one route whose `503` has two
bodies: its own readiness report, or the sanitized refusal when no session
could be opened at all. Both are declared.

**`422` has two bodies, and which one you get says who refused the
request.** A refusal the API itself decided — an unsupported filter, a
contradictory scope, an incompatible comparison, a reversed range, a
document whose meaning the API rejects — answers `{"detail": "<sentence>"}`,
the same shape every other status above uses. A request refused before the
endpoint ran — a missing required parameter, one outside its declared
bounds, a value of the wrong type, a body that fails schema validation —
answers the `HTTPValidationError` the contract declares for it:

```json
{"detail": [{"loc": ["query", "limit"],
             "msg": "Input should be less than or equal to 1000",
             "type": "less_than_equal"}]}
```

`loc` is the path to what was refused (`["query", "limit"]`,
`["body", "blocks", 0, "type"]`), `msg` says why, and `type` is the stable
machine-readable reason. Entries also carry the `input` they refused and a
`ctx` holding the bound; treat those as diagnostic. Read `detail`'s type
before rendering it: a client that assumes a string shows the reader a bare
status code on the one class of error the API can explain.

## Caching, limits, and correlation

- **Every** public analytical GET is cacheable — the catalog, `/observations`
  and its releases, the legacy pair, all eight source-scoped observation
  routes, CDC, USDA NASS, distribution, and both comparison routes. They
  answer with `x-cache: HIT|MISS` and
  `Cache-Control: public, max-age=<ttl>`. The authenticated resources never
  do, and neither does a health probe, whose answer must describe now. The cache key includes the served
  contract's fingerprint and the **warehouse publication epoch**, so a
  republication is reflected within the deployment's freshness window rather
  than after the TTL. You do not need to bust anything.
- **A failure is never cacheable.** Only a `200` carries
  `Cache-Control: public, max-age=<ttl>` and an `x-cache` label. Every other
  status on those same paths — a `404`, a `422`, a rate-limited `429`, a
  sanitized `503` — answers `Cache-Control: no-store` and no `x-cache` at
  all, because a response the cache was never a candidate to answer has no
  hit or miss to report. So a shared cache in front of this API cannot serve
  one client's refusal to another, and `Retry-After` on a `429` means what it
  says.
- Rate limits, when enabled, are per client and split by cost class: catalog
  reads and analytical reads spend independent budgets. Cache hits cost no
  budget. "Per client" means the address the request arrived from — or, when
  the deployment has declared the reverse proxies in front of the API, the
  address those proxies forwarded. Sending your own `X-Forwarded-For` from an
  undeclared hop changes nothing: the header is read only from a hop the
  deployment trusts, so it can never be used to claim a second budget.
- Every response carries `X-Request-ID`. Send your own (`[A-Za-z0-9._-]`, ≤64
  chars) to correlate your logs with the server's; anything else is replaced.
- Pagination is `limit`/`offset` with documented deterministic ordering per
  resource. `offset` is bounded; page with filters rather than deep offsets.
  The observation reads' orders are in [Paging a history, and what orders
  it](#paging-a-history-and-what-orders-it); every other paged read is here:

| Read | Ordered by |
| --- | --- |
| `/catalog/metrics` | `metric_code` — the catalog's own unique key, so no two rows can tie |
| `/catalog/geographies` | `geo_id` — the primary key of the published geography dimension |
| `/comparison` | `geo_id`. Each side is reduced to one row per geography before the join, so the joined answer holds one row per geography and the key is the whole order |
| `/usda-nass/series` | `product_id`, `short_desc`, `geo_id`, then `series_id` — a digest over the exact tuple the series view groups by, unique per row by construction, which closes the order where one `short_desc` spans several domain categories |
| `/analysis-configurations` | `name`, then `configuration_id`. Names are unique per owner, and the id closes the order regardless |
| `/evidence-packets` | `name`, then `packet_id`, on the same basis |

  Each of those is a total order for the same reason the observation reads'
  are: two consecutive pages can neither repeat a row nor skip one.

## What this API will not do

- Return provider-published facts and API-derived values without
  distinguishing them.
- Convert missing, suppressed, invalid, or non-reporting values to zero.
- Serve a comparison whose published semantics contradict each other.
- Read raw captures, control state, or silver internals.
- Collapse a source's strata, domains, or subject grain into a single number
  you did not ask for.
