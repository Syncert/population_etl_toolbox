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
| `GET /api/v1/catalog/geographies` | Geography identities and attribution |
| `GET /api/v1/catalog/capabilities` | **The route map.** Per source: route segment, whether the neutral routes answer, registered dataset identities, the exact routes that serve it with their query-parameter names, and `observation_filters` — the neutral filters that source supports |
| `GET /api/v1/catalog/freshness` | Per-source publication and freshness state from the warehouse's own signal |

`q` on `/catalog/metrics` and `/catalog/geographies` is a case-insensitive
**literal** substring search, not a pattern: `%` and `_` match themselves, so
`q=CENSUS_ACS` and `q=B01003_001` find those exact strings rather than
anything shaped like them. There is no wildcard syntax to reach for.

`observation_filters` is the contract for per-source filtering: a filter a
source does not declare is **rejected with a 422 naming the supported set**,
never silently ignored. Read capabilities once at startup rather than
guessing.

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

Each row carries typed core fields plus everything the source publishes:

- `value` is **text**, to preserve provider precision. Parse it yourself.
- `value` is `null` whenever the source did not publish a usable number, and
  `value_status` says why in the source's own vocabulary (`suppressed`,
  `withheld`, `missing`, `not_reported`, …). `value_status` is `null` when the
  source publishes no status vocabulary at all — which is distinguishable
  from a published `valid`. **Nothing is ever coerced to zero.**
- `dimensions` carries the source's own published fields under their own
  names (CDC strata and footnotes, FBI subject/offense/program, NASS
  commodity/domain/practice, Census dataset and vintage).
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
  its publication.

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
observations.

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
served and the unverified rule travels as a caveat.

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
- `filters` maps a filter name to **one value**, the value the route would
  receive as a query parameter. An array, an object, or a null is refused:
  these parameters are single-valued, so a document naming two cannot replay
  as what it says. A filter declared with an inclusive range — `year_from`,
  `year_to` — takes a whole number or its text, and refuses a fractional one
  or a boolean, exactly as the live route does. Every other filter takes any
  single scalar within its declared length, so `state_fips: 6` and
  `state_fips: "06"` are both accepted, as `?state_fips=6` is.
- An `observations` document records the reduction it was viewed with:
  `newest_per_geography` or `newest_release_per_period`, under the scope each
  belongs to. A view saved without one replays as the whole publication,
  which for a source whose latest publication is a series is a different set
  of rows — so store the one you asked for. Both default to `false`, and the
  same contradictions the live route refuses are refused here.
- On read, `validation` reports whether the document still matches live
  capabilities. A stale configuration is returned **unmodified** with
  `validation.valid = false` and a reason — the API never rewrites your
  content.
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
  measure its query does not ask for, records a scope or release its query
  does not, when a prose block carries a query or an envelope, when a block
  id repeats, or when a block's query is one the live routes would refuse.
  An analytical block that is still empty or partially filled is **stored**,
  and reported: `validation.blocks[]` names each block, whether it is valid,
  the reason, and the envelope fields still `missing`. A block whose measure
  was retired after it was stored is reported the same way, with the
  document returned unmodified.
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
| `422` | A request the API can explain: an unsupported filter, a contradictory scope, an incompatible comparison, a reversed range, or an invalid document |
| `429` | Rate limited. Honour `Retry-After` |
| `503` | The API cannot serve: database unavailable, a required serving contract missing, or (for saved analysis) storage not configured. The body is deliberately sanitized and never names warehouse objects |

Every error body is `{"detail": "..."}`. A `503` never tells you which
warehouse relation is missing — that detail goes to the server log, because
responses must not be usable to probe deployment state.

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
