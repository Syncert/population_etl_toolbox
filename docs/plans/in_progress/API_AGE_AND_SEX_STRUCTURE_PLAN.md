---
id: api-age-and-sex-structure
branch: claude/project-thread-w7xkh3
depends_on:
  - api-per-capita-and-shares
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/api tests/unit/semantics tests/unit/shared -q
  - ruff format --check . ; ruff check .
---

# The API serves a place's age and sex structure beside every level it sits in

## Status

In progress, 2026-10-31: part 1 built on this branch (`/age-structure`, `/age-structure/history`, `/age-structure/cohorts`; API-184..186). EXPLAIN (ANALYZE, BUFFERS) on OFFICE_LAB, 2026-10-31, live warehouse under load, 52 codes: latest read (Madison, Dane, Wisconsin, nation) Index Scan ix_mv_acs_latest_metric_geo, 208 rows, 407 ms cold / 2.5 ms warm; pinned read prunes to the one vintage partition and uses (metric_code, geo_id, observation_date), 3.6–3.7 ms cold, about 4.5 ms planning (Wisconsin and Texas sets); history read 832 rows on the (metric_code, geo_id, observation_date) index in every populated partition, 298–584 ms cold / 7–15 ms warm. The narrow pinned range relies on observation_date = MAKE_DATE(vintage_year, 1, 1); the live count of rows breaking that was 0 of 1,080,837,935. No new index. Part 1 of the API side of
[`AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md`](../to_do/AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md),
governed by [ADR-0013](../../decisions/0013-age-and-sex-structure.md)
(accepted by Nick on 2026-10-31 with the level toggle). The web plan,
[`WEB_AGE_AND_SEX_STRUCTURE_PLAN.md`](WEB_AGE_AND_SEX_STRUCTURE_PLAN.md),
builds against the contract below and can be built in parallel against
fixtures.

Part 2 (separate PR, after this one): the derived summary relation
`gold_census.age_structure_summary` with its index DDL, the summary
indicators as map metrics, and the by-age rate schemes. Kept apart because it
carries DDL that OFFICE_LAB must pre-build, while this part reads only
existing relations through existing indexes.

## Deliverables

1. `docs/semantics/age_bands.json` + JSON Schema + loader
   (`apps/api/services/age_structure_service.py` reads it once): scheme
   `acs_b01001_five_year` (version 1, status `draft`) mapping each of the 18
   bands, per sex, to its B01001 lines, plus the broad groups and the
   indicators. A unit test proves every line `_003`–`_025` and `_027`–`_049`
   is used exactly once per sex.
2. `GET /api/v1/age-structure` (one place, every enclosing level, one answer).
3. `GET /api/v1/age-structure/history` (one place across releases).
4. `GET /api/v1/age-structure/cohorts` (one place, two non-overlapping
   releases).
5. Unit tests, behavioural catalog rows, consumer-guide section, OpenAPI
   snapshot.

## Contract

All values are strings in the neutral contract's decimal form (as
`/observations/derived`), `null` when unavailable, never `0` for missing.
Shares are percentages of the place's total population (both sexes); ratios
are per 100 people aged 18 to 64.

### `GET /api/v1/age-structure`

Query: `geo_id` (required), `dataset` = `acs5` (default) | `acs1`,
`release` = `latest` (default) | a vintage year, `compare_geo_ids` (comma
list, at most 3, for the compare page).

```jsonc
{
  "geo_id": "state:55|place:48000",
  "geo_level": "PLACE",
  "geo_name": "Madison city, Wisconsin",
  "dataset": "acs5",
  "scheme": {"scheme_id": "acs_b01001_five_year", "version": 1, "status": "draft"},
  "default_reference_geo_id": "state:55|county:025",
  "places": [
    {
      "geo_id": "state:55|place:48000",
      "geo_level": "PLACE",
      "geo_name": "Madison city, Wisconsin",
      "role": "place",                 // place | reference | compare
      "reference_level": null,         // COUNTY | STATE | NATIONAL for references
      "overlap_weight": null,          // a city's area share inside its county reference
      "release": "2023",
      "period_start": "2019-01-01",
      "period_end": "2023-12-31",
      "unavailable_reason": null,      // not_published | release_mismatch
      "total":  {"count": "269840", "count_moe": "22"},
      "men":    {"count": "134512", "count_moe": "410"},
      "women":  {"count": "135328", "count_moe": "402"},
      "bands": [
        {
          "band_id": "0-4", "age_start": 0, "age_end": 4, "label": "0 to 4",
          "lines": {"men": ["B01001_003"], "women": ["B01001_027"]},
          "men":   {"count": "6812", "count_moe": "512", "share": "2.52", "share_moe": "0.19",
                    "cv_percent": "4.6", "reliable": true, "unavailable_reason": null},
          "women": { /* same shape */ }
        }
        // ... 18 bands, youngest first; "85+" has age_end null
      ],
      "median_age": {                  // provider's own (B01002), derived: false
        "all":   {"value": "31.6", "moe": "0.2"},
        "men":   {"value": "31.0", "moe": "0.3"},
        "women": {"value": "32.3", "moe": "0.3"}
      },
      "indicators": {                  // derived: true
        "under_18":    {"count": "...", "count_moe": "...", "share": "...", "share_moe": "..."},
        "age_18_64":   { /* same */ },
        "age_65_plus": { /* same */ },
        "age_85_plus": { /* same */ },
        "child_dependency_ratio":    {"value": "26.4", "moe": "0.9", "moe_method": "ratio"},
        "old_age_dependency_ratio":  { /* same */ },
        "total_dependency_ratio":    { /* same */ },
        "sex_ratio":                 {"value": "99.4", "moe": "0.6", "moe_method": "ratio"}
      },
      "derived": true
    }
    // then each reference in toggle order (county, state, nation for a city),
    // then each compare_geo_id
  ]
}
```

- **References** (ADR-0013 rule 4), next level up first:
  city and tract → county, state, nation; county → state, nation;
  metro → nation (and its state when the reference records it inside one);
  state → nation; nation → none. A city's county is the `part_of` county,
  else the `intersects` county with the largest `overlap_weight` (area share,
  as the reference records it), whose weight is returned.
- Every place is read at the **same release and dataset** as the requested
  place. A reference without that release is `unavailable_reason:
  "not_published"` with no bands (never a different release silently).
- A band or group with a missing or suppressed line has `count: null` and
  `unavailable_reason: "suppressed_input"` or `"line_missing"`; the place's
  other bands are still served.
- `reliable` is `cv_percent <= 30` where `cv = (moe / 1.645) / count`; a
  zero count is `reliable: false`.
- Margins: a sum's MOE is the root sum of squares of its lines' MOEs; a
  share uses ADR-0009's proportion formula against the total (`B01001_001`),
  falling back to the ratio formula when the radicand is negative; a
  dependency or sex ratio uses the ratio formula. Controlled MOEs (no MOE
  published on a total at state or nation) count as zero.
- `404` unknown `geo_id`; `422` bad `dataset`, `release`, or more than 3
  `compare_geo_ids`; `503` when the ACS serving relation is absent.

### `GET /api/v1/age-structure/history`

Query: `geo_id`, `dataset` (default `acs5`), `periods` = `non_overlapping`
(default) | `all`, `include_bands` (default `false`).

```jsonc
{
  "geo_id": "...", "geo_level": "...", "geo_name": "...", "dataset": "acs5",
  "scheme": { /* as above */ },
  "periods": "non_overlapping",
  "items": [
    // newest first; each the same block as one "places" entry, with
    // "bands": [] unless include_bands=true
  ]
}
```

`non_overlapping` keeps the newest release and then each release whose
period does not overlap the last one kept (for `acs5`: 2019–2023, 2014–2018,
2009–2013; for `acs1`: every release, which never overlap). Reads
`gold_census.rpt_acs_observations` (as released), not `mv_acs_latest`.

### `GET /api/v1/age-structure/cohorts`

Query: `geo_id`, `dataset` (default `acs5`), `from_release`, `to_release`.

```jsonc
{
  "geo_id": "...", "dataset": "acs5", "scheme": { },
  "from": {"release": "2018", "period_start": "2014-01-01", "period_end": "2018-12-31"},
  "to":   {"release": "2023", "period_start": "2019-01-01", "period_end": "2023-12-31"},
  "years_apart": 5,
  "comparable": true,
  "refusal": null,  // {"reason": "overlapping_periods" | "period_gap_not_five_years"
                    //   | "boundary_changed" | "not_published", "detail": "..."}
  "caveat": "A change between two surveys mixes births abroad, deaths, moves and sampling error; it is not one group of people followed over time.",
  "items": [
    {
      "from_band": "25-29", "to_band": "30-34",
      "men":   {"from_count": "...", "to_count": "...", "change": "...", "change_moe": "...",
                "intervals_overlap": false},
      "women": { /* same */ }
    }
    // 16 items, 0-4 → 5-9 through 75-79 → 80-84; nothing moves into or out
    // of 85+, whose upper end is open
  ]
}
```

- Comparable only when the periods do not overlap and their ends are exactly
  five years apart (one band). Tracts across geography vintages (2010 vs 2020
  tract codes, read from the geography's vintage) are refused
  `boundary_changed`.
- `change_moe` is the root sum of squares of the two counts' MOEs; no
  wording, no verdict (ADR-0011): the web writes the sentence.

## Reads and performance (prevention list)

- `/age-structure`: one statement on `gold_census.mv_acs_latest` with
  `metric_code = ANY(:codes)` (49 B01001 codes + 3 B01002 codes) and
  `geo_id = ANY(:geo_ids)` (at most 7), `vintage_year = :release` when pinned
  (else newest per code and place, then checked to be one release). Served by
  `ix_mv_acs_latest_metric_geo (metric_code, geo_id)`; about 360 rows. A
  pinned older release reads `rpt_acs_observations` the same way (API-182's
  `vintage_year` match) through `ix_rpt_acs_observations_metric_geo_date`.
- References resolved once per request from the place's identity and one
  relationship read for a city or metro (never the whole related list).
- `/history`: one statement on `rpt_acs_observations`,
  `metric_code = ANY(:codes) AND geo_id = :geo_id`, about 1,700 rows for a
  5-year place across releases; plain equality on every filter, no computed
  expression in `WHERE`.
- No new index. `EXPLAIN (ANALYZE, BUFFERS)` of each statement on OFFICE_LAB
  for a tract, a city and a county is recorded in the evidence before the
  plan moves to `needs_review`.

## Acceptance criteria

1. On a fixture of Dane County's B01001 lines, the 18 bands per sex sum to
   `B01001_002` / `_026`, each band's MOE is the root sum of squares of its
   lines, `under_18 + age_18_64 + age_65_plus` equals the total, and the
   dependency ratios equal the hand-computed values.
2. A city answers with references county, state, nation in that order and
   `default_reference_geo_id` the county; a county with state, nation; a state
   with nation; the nation with none. A city that intersects two counties
   picks the larger `overlap_weight` and returns it.
3. A suppressed line leaves its band `null` with `suppressed_input` and the
   other bands served; a reference with no row for the release is
   `not_published`, never another release.
4. History with `non_overlapping` returns 2023, 2018, 2013 for a 5-year
   fixture with every release; `all` returns every release.
5. Cohorts between 2018 and 2023 5-year are comparable with 16 items; 2019 and
   2023 are refused `overlapping_periods`; 2013 and 2023 are refused
   `period_gap_not_five_years`.
6. Median ages are B01002's values, `derived: false`.
7. OpenAPI snapshot, consumer guide, Ruff and the API unit tier pass.

## Checkpoint

Next pickup: write `age_bands.json` and its loader test, then the pure band
and margin functions with their tests, then the reads and routes.
