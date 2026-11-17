# IRS SOI county and state income fixtures

Delaware (state 10: Kent 001, New Castle 003, Sussex 005), plus the nation,
Other Areas and Puerto Rico from SOI's state file. The rows are SOI's own.
They were fetched on OFFICE_LAB from `https://www.irs.gov/pub/irs-soi/` on
2026-11-05, because the cloud build host cannot reach irs.gov.

| File | SOI product | What was kept |
| --- | --- | --- |
| `23incyallnoagi.csv`, `11incyallnoagi.csv` | County data, all returns (`agi_stub` 0) | SOI's header line and Delaware's four rows (state total `COUNTYFIPS` 000 and three counties), byte for byte |
| `23incyallagi.csv`, `11incyallagi.csv` | County data by size of AGI (2023: stubs 1-8; 2011: stubs 1-7) | SOI's header line and Delaware's rows, byte for byte |
| `23in55cmcsv.csv` | Historic Table 2, every state (`AGI_STUB` 0-10) | SOI's header line and the `US`, `DE`, `OA` and `PR` rows, byte for byte |
| `2010countydata.zip` | 2010 county data | A new zip holding `10incyallnoagi.csv` and `10incyallagi.csv`, each SOI's header and Delaware rows |
| `1989countyincome.zip` | 1989 county income workbooks | A new zip holding SOI's `delaware89ci.xls`, unchanged |
| `1996countyincome.zip` | 1996 | `1996CountyIncome/DELAWARE96ci.xls`, unchanged |
| `2003countyincome.zip` | 2003 | `2003CountyIncome/DELAWARE03ci.xls`, unchanged |
| `2009countyincome.zip` | 2009 | `2009 County Income/county income 2009 DE.xls`, unchanged |
| `1997countyincome.zip` | 1997 | `1997CountyIncome/ALASKA97ci.xls`, unchanged: the one workbook that prints its boroughs by code with no name column |
| `12cyallnoagi.csv` | 2012 county data, all returns | SOI's header line and Alabama's first two rows (state total and Autauga, 001), as OFFICE_LAB fetched them; 2012 is the one year named `12cy...`, not `12incy...` |

Rows and workbooks are SOI's bytes. Only the containers are new: SOI's zips
hold every state, so the fixture zips hold Delaware's member alone, under
SOI's own member path, with a fixed timestamp (2026-11-05). The CSV rows
were selected from SOI's files without being rewritten.

The 1997 Alaska workbook was exported on 2026-11-05 from the warehouse's
stored capture of `1997countyincome.zip`. Values the tests rely on: 1989 Delaware had 308,166 returns (Kent 48,600,
New Castle 205,299, Sussex 54,267); 2009, 371,587; 2010, 427,479; 2023,
507,370 in the county file and 507,390 in the state file. The nation in
2023 had 159,949,000. 1997 Alaska: 233,249 returns, its 27 boroughs summing
to the same; 2012 Alabama: 2,050,090 returns.

Refresh: rerun the selection on OFFICE_LAB, in a scratch folder, and keep
the file names.
