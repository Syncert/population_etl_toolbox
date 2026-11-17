# 2020 Census redistricting data fixtures

Real responses from the Census Data API (`https://api.census.gov/data/2020/dec/pl`),
read on OFFICE_LAB on 2026-11-12 with
`get=NAME,P5_001N,...,P5_010N,P1_001N,H1_001N,H1_002N,H1_003N`. The key was
sent as the `key` query parameter and is not in any file here.

| File | Slice it answers | Request actually made | Rows |
| --- | --- | --- | --- |
| `pl_us_1.json` | nation | `for=us:*` | 1 |
| `pl_state_all.json` | every state | `for=state:*` (50 states, DC, Puerto Rico) | 52 |
| `pl_county_all.json` | every county | `for=county:*&in=state:55`, narrowed to Wisconsin to keep the fixture small | 72 |
| `pl_tract_55.json` | Wisconsin's tracts | `for=tract:*&in=state:55 county:025`, narrowed to Dane County | 125 |

The registered requests are `for=county:*` with no `in` clause and
`for=tract:*&in=state:55`; the narrower requests here keep the files small
and return rows of the same shape. Dane County (55025) reads 17,465 people
in group quarters of 561,504: 1,806 in nursing facilities, 9,925 in college
housing and 1,541 in correctional facilities for adults.
