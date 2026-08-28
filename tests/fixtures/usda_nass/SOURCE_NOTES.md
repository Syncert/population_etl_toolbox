# USDA NASS Quick Stats source notes

These notes record the provider contract the reviewed fixtures in this
directory encode. They are evidence for the registry in
`src/data_ingestion_toolbox/usda_nass/registry.py`; when the provider changes,
the notes, the registry, and the fixtures move together.

## Endpoints

| Path | Purpose | Response envelope |
| --- | --- | --- |
| `/api/api_GET` | Retrieve records for a bounded selection | `{"data": [ … ]}` |
| `/api/get_counts` | Preflight the record count for the same selection | `{"count": "…"}` |
| `/api/get_param_values` | Enumerate the provider's own domain for one parameter | `{"<param>": [ … ]}` |

Host: `https://quickstats.nass.usda.gov`. Reference:
<https://quickstats.nass.usda.gov/api/>.

## Authentication

A registered API key is required and is passed as the `key` **query
parameter**, not a header. The adapter therefore keeps two parameter sets
apart: the registered selections, which are fingerprinted, captured, and
replayed; and the outgoing transport query, which adds `key` immediately before
the request and discards it with the response. `key` never appears in a request
fingerprint, a captured parameter set, a log line, or an exception message.

## Record limit

A single `api_GET` call returns at most **50,000** records. A selection above
that ceiling is refused by the provider with an `error` envelope containing
`exceeds limit = 50000` rather than truncated data. The adapter preflights
every registered slice through `get_counts` and refuses to retrieve a slice at
or above the ceiling, so an over-limit partition can never reach publication. A
response whose record count equals the ceiling is treated as truncated for the
same reason: it is indistinguishable from a cut-off page.

A selection matching no rows is also answered with an `error` envelope
(`unable to find row(s) matching your criteria`), so a zero-count slice is
recorded as empty from its preflight and no data request is issued.

## Record fields

Quick Stats returns every documented field on every record, using an empty
string where a field does not apply. A record missing one of the consumed
fields is a contract change, not an absent value, so it is quarantined rather
than parsed. The consumed field list is frozen in
`registry.QUICK_STATS_FIELDS` and covers the What, Where, When, and Value
dimensions, including `short_desc`, `domain_desc`, `domaincat_desc`,
`location_desc`, `reference_period_desc`, `week_ending`, `load_time`, `Value`,
and `CV (%)`.

## Value and CV symbols

`Value` and `CV (%)` are text. A value may be a thousands-separated number
(`"90,594,000"`), a decimal (`"179.3"`), a provider symbol, or empty. The exact
source text is always retained beside the typed result, and no symbol is ever
converted to zero.

| Symbol | NASS meaning | Warehouse `value_status` |
| --- | --- | --- |
| `(D)` | Withheld to avoid disclosing data for individual operations | `withheld` |
| `(S)` | Insufficient number of reports to establish an estimate | `insufficient_reports` |
| `(X)` | Not applicable | `not_applicable` |
| `(Z)` | Less than half the rounding unit | `below_rounding_unit` |
| `(NA)` | Not available | `not_available` |
| `(H)` | Provider quality marker for a very high coefficient of variation | `quality_flagged` |
| `(L)` | Provider quality marker for a very low coefficient of variation | `quality_flagged` |
| *(empty)* | Not published for this record | `missing` |

`(Z)` is deliberately **not** zero: it means the true value rounds below the
displayed unit. A published `"0"` is a real numeric zero and parses to
`value_status = 'valid'`.

Source: the NASS Quick Stats glossary,
<https://quickstats.nass.usda.gov/src/glossary.pdf>, and the Quick Stats API
documentation linked above. `(H)` and `(L)` are recorded as provider quality
markers; the pipeline preserves the exact symbol and never converts it to a
numeric coefficient of variation.

## Geography

`agg_level_desc` names the aggregate level. Only `NATIONAL`, `STATE`, and
`COUNTY` are modeled:

| `agg_level_desc` | Canonical identity | Source of the codes |
| --- | --- | --- |
| `NATIONAL` | `us:1` | fixed |
| `STATE` | `state:SS` | `state_fips_code`, falling back to `state_ansi` |
| `COUNTY` | `state:SS\|county:CCC` | `county_ansi`, falling back to `county_code` |

Every other level — `AGRICULTURAL DISTRICT`, `REGION : MULTI-STATE`,
`WATERSHED`, `ZIP CODE`, congressional districts — is retained in the raw
capture and in the silver source record with `geo_type = 'unsupported'` and no
`geo_id`. It is never coerced into a county. A `COUNTY` row without an exact
three-digit county code (for example `OTHER (COMBINED) COUNTIES`) is
quarantined rather than mapped by name.

## Period

Crop survey products in this registry are `freq_desc = ANNUAL` with
`reference_period_desc = YEAR` and `begin_code`/`end_code` of `00`.
`week_ending` is empty for annual records and carries an ISO date for weekly
progress and condition records. All four fields are preserved.

## Observation grain

One published observation is uniquely identified by the complete Quick Stats
grain, not by commodity, geography, and year:

```text
registered product
x extraction release (max load_time)
x full commodity classification
    (sector_desc, group_desc, commodity_desc, class_desc,
     prodn_practice_desc, util_practice_desc)
x statistic identity
    (source_desc, statisticcat_desc, short_desc, unit_desc, freq_desc)
x domain member (domain_desc, domaincat_desc)
x geography (agg_level_desc and the exact location codes)
x period (year, freq_desc, begin_code, end_code, reference_period_desc,
          week_ending)
```

`silver_nass.observation_revision.source_record_id` is the SHA-256 of exactly
those source fields, and `silver_nass.fact_crop_observation` is unique on
`(product_id, release_watermark, source_record_id)`. `Value`, `CV (%)`, and
`load_time` are revision attributes and are deliberately outside the grain, so
a revised value replaces nothing: it arrives as a new release beside the one it
revises.

## Release identity and revisions

Quick Stats publishes no dataset-metadata document. A release is identified by
the evidence the provider does expose:

- the `get_counts` preflight for every registered slice;
- the observed record field signature; and
- the maximum `load_time` across the captured rows, which is the incremental
  key (`registry.NassProduct.incremental_field`).

Survey estimates are revised until final, so a later extraction of the same
slice can publish a formerly withheld value or change a published one.
`corn_survey_annual_revised.json` is exactly that case: it is retained as a new
release beside the original rather than overwriting it.

## Survey and Census of Agriculture

`source_desc` separates the two programs. Survey products have their own
frequencies, forecasts, finals, and revisions; the Census of Agriculture is
periodic and enumerated. They are registered as distinct products with distinct
parser contract versions and never merge, even where labels and periods
overlap.

## Fixture inventory

| File | Contents |
| --- | --- |
| `corn_survey_annual.json` | Corn survey acreage/yield/production at all three levels, with withheld county values and CVs |
| `soybeans_survey_annual.json` | Soybeans survey measures, including `(Z)` and `(NA)` county records |
| `wheat_survey_annual.json` | Winter wheat survey measures, including `(S)` county records |
| `hay_survey_annual.json` | Hay survey measures in `TONS`/`TONS / ACRE`, including an `(X)` record |
| `corn_census_county.json` | Census of Agriculture corn measures with published CVs |
| `corn_survey_annual_revised.json` | A later extraction of the corn county slice with revised and newly published values |
| `boundary_records.json` | Records and error envelopes the contract must reject or classify explicitly |
| `expected_contracts.json` | Expected parsed outcome for every reviewed row |

Every fixture is a real-shaped sample, not a provider extract: the numbers are
representative rather than authoritative, and no fixture contains a credential.
