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
- `uncertainty` is `null` when the source publishes none; otherwise margins
  of error, confidence bounds, or the CV trio.
- `coverage` carries FBI UCR participation context — a month nobody reported
  is `not_reported` with `null` value and a `participation_status`, not zero
  crime.
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

`geo_level` on a served row is always `NATIONAL`, `STATE`, or `COUNTY`, and
the `geo_level` filter matches that vocabulary. A national row answers
`geo_level=NATIONAL`.

### Legacy observation routes

`GET /api/v1/observations/latest` and `/observations/timeseries` are the
original MVP shapes and answer for **only** Census ACS, BLS, and FRED (the
three sources in the cross-source union views). They retire with the
unversioned aliases. New work should use `/observations`.

Source-scoped routes remain for source-specific exploration:
`/api/v1/{bls,census,fred,pep}/observations/{latest,timeseries}`,
`/api/v1/cdc/observations`, `/api/v1/usda-nass/{observations,series,measures,source-notes}`.

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

`GET /api/v1/comparison` enforces exactly that verdict. Each side is reduced
to one newest value per geography inside its own relation before the join, so
a multi-period source cannot create Cartesian rows. Every row carries
`period_a`/`period_b` — the periods actually combined — and `value_a`/
`value_b` alongside the API-derived `difference` and `ratio`, which are named
in `derivations`. An incompatible pair answers `422` with the failed rules.

`GET /api/v1/distribution/bins` returns API-derived equal-width bins over one
metric's latest values, labelled `derived: true` with its `source_code` and
`units`. Counts are exact counts of provider-published numeric values; null,
suppressed, and missing values are excluded rather than binned.

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
- On read, `validation` reports whether the document still matches live
  capabilities. A stale configuration is returned **unmodified** with
  `validation.valid = false` and a reason — the API never rewrites your
  content.
- Updates send `expected_version`; a mismatch is `409` naming the current
  version. Deletion is immediate and permanent.
- These responses are `private, no-store` and are never publicly cached.

## Errors

| Status | Meaning |
| --- | --- |
| `401` | Missing, malformed, unknown, or revoked bearer token. Identical for every case by design |
| `404` | Unknown identifier, or a configuration you do not own (indistinguishable on purpose) |
| `409` | Version conflict, or a name you already use |
| `422` | A request the API can explain: an unsupported filter, a contradictory scope, an incompatible comparison, a reversed range, or an invalid document |
| `429` | Rate limited. Honour `Retry-After` |
| `503` | The API cannot serve: database unavailable, a required serving contract missing, or (for saved analysis) storage not configured. The body is deliberately sanitized and never names warehouse objects |

Every error body is `{"detail": "..."}`. A `503` never tells you which
warehouse relation is missing — that detail goes to the server log, because
responses must not be usable to probe deployment state.

## Caching, limits, and correlation

- Cacheable public analytical GETs answer with `x-cache: HIT|MISS` and
  `Cache-Control: public, max-age=<ttl>`. The cache key includes the served
  contract's fingerprint and the **warehouse publication epoch**, so a
  republication is reflected within the deployment's freshness window rather
  than after the TTL. You do not need to bust anything.
- Rate limits, when enabled, are per client and split by cost class: catalog
  reads and analytical reads spend independent budgets. Cache hits cost no
  budget.
- Every response carries `X-Request-ID`. Send your own (`[A-Za-z0-9._-]`, ≤64
  chars) to correlate your logs with the server's; anything else is replaced.
- Pagination is `limit`/`offset` with documented deterministic ordering per
  resource. `offset` is bounded; page with filters rather than deep offsets.

## What this API will not do

- Return provider-published facts and API-derived values without
  distinguishing them.
- Convert missing, suppressed, invalid, or non-reporting values to zero.
- Serve a comparison whose published semantics contradict each other.
- Read raw captures, control state, or silver internals.
- Collapse a source's strata, domains, or subject grain into a single number
  you did not ask for.
