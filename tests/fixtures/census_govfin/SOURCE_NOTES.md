# Census government finance fixtures

Read on OFFICE_LAB on 2026-11-05 (the cloud cannot reach census.gov); the
reads and the Bureau's documentation are kept in the project files under
`taxes/govfin/`.

## Census Data API answers (verbatim)

| File | Request |
| --- | --- |
| `asslgf_2022_us.json` | `timeseries/govslocalfin?get=AGG_DESC,AMOUNT,AMOUNT_CV,GOVTYPE,YEAR&for=us:*&time=2022` |
| `asslgf_2022_state.json` | the same with `for=state:55` (production asks `state:*`) |
| `qtax_2024_us.json` | `timeseries/eits/qtax?get=CELL_VALUE,DATA_TYPE_CODE,TIME_SLOT_ID,ERROR_DATA,CATEGORY_CODE,SEASONALLY_ADJ,GEO_LEVEL_CODE&for=us:*&time=2024` |
| `qtax_2024_state.json` | the same with `for=state:55` |

`govslocalfin` labels none of its `AGG_DESC` codes. The codes the registry
reads were matched figure for figure against the item-code sums of the same
year's `timeseries/govsstatefin` detail rows for Wisconsin's state
government, using the item lists of Census's `Finance_Aggregate_Lines_2017.xlsx`
(in the 2017 unit-file zip): LF0003 intergovernmental revenue (16,450,672),
LF0004 from the federal government, LF0006 from local governments, LF0007
general revenue from own sources, LF0008 taxes (23,363,814), LF0009 property,
LF0011 general sales, LF0012 selective sales, LF0023 individual income,
LF0024 corporate income, LF0041 current charges, LF0059 miscellaneous
general revenue. LF0005 (aid from the state) has no state-government value
and is read for local governments; LF0231 is debt outstanding, the sum of
LF0232 (short-term, 0 for the state) and LF0233 (long-term).

## Census of Governments unit-file excerpts (constructed around real lines)

`finestdat_2022_excerpt.txt`, `fin_pid_2022_excerpt.txt` and
`statetypepu_2022_excerpt.txt` follow the 2022 Public Use Files Technical
Documentation layouts (unit record 32 characters, PID 146, state totals 35).
Each opens with the first real lines of the 2022 zip's files (Alabama's
state government and Autauga County; the nation's totals) and continues with
constructed Wisconsin governments whose amounts are chosen so the county-area
sums can be checked by hand:

| Government | Code | Records |
| --- | --- | --- |
| State of Wisconsin | 550000000001 | T01, T09, T40, T41, B46, 49U |
| Dane County | 551025000001 | T01, T09, A61, C46, 44T, E44 |
| Madison city | 552025000002 | T01, T13, A91, U20 (imputed), B89, 64V |
| Madison school district | 555025000003 | T01, C21, D21, U99 |
| Milwaukee sewerage district | 554079000004 | T01, A80 |
| Statewide drainage board | 554000000005 | T99 (no county: state roll-up only) |

Two further unit lines are malformed on purpose: a government code with a
letter in it and a record cut short. The Wisconsin state-totals lines agree
with the local roll-up for T01, T09, T13 and T99.
