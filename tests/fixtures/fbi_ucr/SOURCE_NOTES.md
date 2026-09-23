# FBI CDE fixture source notes

These small fixtures come from the official FBI Crime Data Explorer API,
documented at `https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi` and served
from `https://api.usa.gov/crime/fbi/cde/LATEST`. They were retrieved on
2026-08-27. JSON indentation was normalized for review; provider keys, series
labels, month keys, numeric text, and null/omitted fields were not
reinterpreted.

Request URLs below are shown redacted. Every real request also carries the
`API_KEY` query parameter, which is supplied at request execution from the
`FBI_CDE_API_KEY` environment secret and never appears in a fixture, a capture,
a request fingerprint, a log line, or an error summary.

## Captured provider responses

| Fixture | Redacted request | Notes |
| --- | --- | --- |
| `agency_directory_WI.json` | `GET /agency/byStateAbbr/WI` | Exact provider entries for the six reviewed ORIs, kept under their published county grouping keys. |
| `summarized_national_V.json` | `GET /summarized/national/V?from=01-2023&to=06-2023` | Provider-published national violent-crime series. |
| `summarized_state_WI_V.json` | `GET /summarized/state/WI/V?from=01-2023&to=06-2023` | Provider-published Wisconsin series plus the national comparison series the provider includes. |
| `summarized_agency_WI0130000_V.json` | `GET /summarized/agency/WI0130000/V?from=01-2023&to=06-2023` | Dane County Sheriff's Office; contains real reported zeros in the clearance series. |

The captured responses were retrieved over a wider window and trimmed to the
registered `01-2023`..`06-2023` period by
`tests/support/build_fbi_fixtures.py`. Trimming removes whole month keys only;
no value was altered.

## The nine further summarized offenses

Captured live on 2026-09-23 with `GET /summarized/{national|state/WI|agency/<ORI>}/<CODE>?from=01-1990&to=06-2023`
for each of `ASS`, `BUR`, `LAR`, `MVT`, `HOM`, `RPE`, `ROB`, `ARS`, and `P`,
over national, Wisconsin, and the six reviewed ORIs: 72 requests, every one
`200`. The whole registered window was captured so the evidence below covers
all 402 months; the fixtures were then trimmed to `01-2023`..`06-2023` with

```text
python -m tests.support.build_fbi_fixtures --offense <CODE> <captured-payload-directory>
```

and stored as `summarized_national_<CODE>.json`,
`summarized_state_WI_<CODE>.json`, and `summarized_agency_<ORI>_<CODE>.json`.
Every one of these 72 fixtures is captured provider bytes; none is derived.
The responses carried `last_refresh_date` `09/15/2026` and `max_data_date`
`09/2026`.

What every offense publishes, at every grain, for all 402 months:

| Offense | Containers | Series suffixes | Absent series |
| --- | --- | --- | --- |
| `ASS` Assault | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `BUR` Burglary | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `LAR` Larceny | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `MVT` Motor Vehicle Theft | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `HOM` Homicide | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `RPE` Rape | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `ROB` Robbery | `actuals`, `rates` | `Offenses`, `Clearances` | none |
| `ARS` Arson | `actuals`, `rates` | `Offenses`, `Clearances` | none -- arson publishes rates |
| `P` Property Crime | `actuals`, `rates` | `Offenses`, `Clearances` | none |

- Series labels are `<subject> Offenses` and `<subject> Clearances` for every
  offense. Rape is published under one label; the provider does not split it
  into legacy and revised series in this endpoint, so the 2013 definition
  change is not visible in the labels.
- State and agency responses carry the same national and state comparison
  series in `rates` as the violent-crime responses do.
- `tooltips.leftYAxisHeaders` reads `Offenses per 100,000 people` and
  `Offenses` for every offense.
- National and Wisconsin series hold a number for every month. Agency series
  hold explicit `null` for months the agency did not report -- the key is
  present with a null value, rather than absent as in the derived
  violent-crime scenario. The Menominee Tribal fixtures carry six real null
  months in `01-2023`..`06-2023`. Both shapes replay as `not_reported`.
- Where an agency's covered population is `0` (University of Wisconsin: Green
  Bay and the Wisconsin State Patrol in every month of the window, Menominee
  Tribal in some earlier months), the provider publishes the agency's own
  **rate** as `-1`. That is a sentinel for "no rate exists", not a value, and
  replay quarantines it as `negative_measure_value`. The live
  violent-crime pipeline already did the same (1,804 such quarantined rows in
  the 2026-08-15 release on the development warehouse); the violent-crime
  agency fixtures were derived, so they never showed it.

## States beyond Wisconsin

Every product registers all 52 `STATE_CODE_CONTRACT` codes. Fixtures exist for
two further subjects only; every other state is proved at the registry level
(canonical code and documented endpoint), not by a fixture. Captured live on
2026-09-23 over the registered window for all ten offenses, trimmed with
`python -m tests.support.build_fbi_fixtures --offense <CODE> --state <XX> <dir>`,
stored as `summarized_state_PA_<CODE>.json` and `summarized_state_VI_<CODE>.json`:

- **Pennsylvania** (`PA`, FIPS `42`) -- partial agency participation that
  changes inside the fixture window: participated population is about 76% of
  the state population in January 2023 and 79% in February, then 99% from
  March. Every month carries a published value.
- **U.S. Virgin Islands** (`VI`, FIPS `78`) -- the provider's label matches the
  registry's (`U.S. Virgin Islands`). The territory published no offense value
  for any month in `01-2023`..`06-2023` (explicit `null`, participated
  population `null`), and 259 of 402 months are null over the whole window. Its
  own rate series carry the `-1` sentinel in 262 months. It resolves to the
  territory geography `state:78`, which the development warehouse's
  `silver_ref.dim_geo_entity` holds.

## Reviewed discovery evidence retained by these fixtures

- One ORI can be grouped under a comma-joined county key (`"DANE, ROCK"`),
  proving that agency-to-county is optional and many-to-many.
- `NOT SPECIFIED` appears as a county grouping key, proving the provider
  publishes agencies with no county association at all.
- The agency response includes city, county, university/college, tribal, and
  state-police agency types, all keyed by ORI rather than by a Census place.
- A state response carries the national comparison series alongside the state
  series, and an agency response carries the state and national comparison
  series in `rates` while `actuals` holds only the agency's own totals. Series
  therefore belong to a subject and are never attributed by position.

## Derived scenario fixtures

The live source does not currently exhibit every case the pipeline must
handle in one window, so the following are derived from the captured agency
response's exact structure. Series labels, container names, month keys, and
population sections keep the provider's shape; only the numeric scenario values
were authored. Each is documented here so a reviewer can tell captured evidence
from constructed evidence.

| Fixture | Derived from | Scenario |
| --- | --- | --- |
| `summarized_agency_WI0137000_V.json` | captured agency response | Municipal agency with a reviewed place mapping; reports every month. |
| `summarized_agency_WI0540300_V.json` | captured agency response | Municipal agency associated with two counties; reports every month. |
| `summarized_agency_WI0050700_V.json` | captured agency response | Campus agency publishing reported zeros. |
| `summarized_agency_WI0400100_V.json` | captured agency response | Tribal agency that did not report two months: the month keys are absent and `participated_population` is `0` for them. |
| `summarized_agency_WIWSP0000_V.json` | captured agency response | Statewide agency with no county association. |
| `summarized_national_V_revised.json` | captured national response | Same request answered by a later `last_refresh_date` with one corrected value, for the retained-revision path. |
| `provider_error_body.json` | live api.data.gov response text | The structured error document api.data.gov returns for a rejected request. |

Regenerate the fixtures with:

```text
python -m tests.support.build_fbi_fixtures <captured-payload-directory>
```

These fixtures are bounded samples for deterministic replay. They are not full
provider downloads and must not be treated as current published crime counts.
