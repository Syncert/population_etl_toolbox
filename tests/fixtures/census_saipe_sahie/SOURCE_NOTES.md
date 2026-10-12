# Census SAIPE and SAHIE fixtures

Real responses from the Census Data API, captured 2026-10-06 with the request
parameters `census_saipe_sahie.registry` builds for estimate year 2023
(`get=NAME,<every registered variable>`, `for=<grain>:*`, `time=2023`, and for
SAHIE the fixed categories `AGECAT=0&IPRCAT=0&SEXCAT=0&RACECAT=0`). The key
was sent as the `key` query parameter and is not in any file here.

| File | Grain | Rows |
| --- | --- | --- |
| `saipe_2023_us.json` | nation (`for=us:*`; SAIPE answers `us` = `00`) | 1 |
| `saipe_2023_state.json` | every state and DC | 51 |
| `saipe_2023_county.json` | counties, narrowed with `in=state:10` (Delaware) to keep the fixture small | 3 |
| `sahie_2023_us.json` | nation (SAHIE answers `us` = `1`) | 1 |
| `sahie_2023_state.json` | every state and DC | 51 |
| `sahie_2023_county.json` | counties, `in=state:10` | 3 |

The registered county request is `for=county:*` with no `in` clause (one
call answers every county); the `in` clause here only narrows the fixture,
and the row shape is the same.

Endpoints: https://api.census.gov/data/timeseries/poverty/saipe and
https://api.census.gov/data/timeseries/healthins/sahie. Variable lists:
`.../variables.json` on each. A year and grain the API does not publish answers
`204 No Content` with an empty body (SAIPE has no county estimates for 1990
to 1992); the tests build that case rather than storing an empty file.
