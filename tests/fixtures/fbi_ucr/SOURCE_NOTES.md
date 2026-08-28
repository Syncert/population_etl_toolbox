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
