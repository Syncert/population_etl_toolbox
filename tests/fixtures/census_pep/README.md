# Census PEP fixtures

These are exact header-plus-one-row excerpts from registered U.S. Census
Population Estimates Program bulk CSV releases, retrieved 2026-08-24.

| Fixture | Official source | Retained row | Purpose |
| --- | --- | --- | --- |
| `nst_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/state/totals/NST-EST2025-ALLDATA.csv` | United States (`SUMLEV=010`) | Current release layout and observations through 2025 |
| `nst_2024.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/state/totals/NST-EST2024-ALLDATA.csv` | United States (`SUMLEV=010`) | Prior release and revised 2024 estimate comparison |
| `subcounty_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv` | Abbeville city, Alabama (`SUMLEV=162`) | Incorporated-place codes and population columns |

The closed-decade fixtures below were retrieved 2026-09-10 and cut the same
way: the file's own header, then whole rows selected byte-for-byte. Values,
columns and text encoding are exactly the source's. Line endings are not:
the repository normalises them on checkout, as it does for every fixture
here, and the readers accept either.

| Fixture | Official source | Retained rows | Purpose |
| --- | --- | --- | --- |
| `co_2010s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv` | Alabama (`SUMLEV=040`), Autauga County (`SUMLEV=050`) | Vintage 2020 layout: `NATURALINC` spelling, `CENSUS2010POP`, and a state row published as a rollup |
| `nst_2010s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/state/totals/nst-est2020-alldata.csv` | United States (`SUMLEV=010`), Alabama (`SUMLEV=040`) | The same state row published in its own right, and the July 2020 seam the 2020s series revises |
| `co_2000s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2000-2009/counties/totals/co-est2009-alldata.csv` | Alabama (`SUMLEV=040`), Autauga County (`SUMLEV=050`) | Vintage 2009 layout and `CENSUS2000POP` |

Alabama and Autauga County are deliberate: both have existed unchanged
across every decade registered here, so a fixture assertion tests the
pipeline rather than a boundary change.

The pre-CSV products are printed tables and fixed-width cell files rather
than "all data" CSVs, so their fixtures keep the page furniture the readers
have to work through. Retrieved 2026-09-10.

| Fixture | Official source | Retained rows | Purpose |
| --- | --- | --- | --- |
| `legacy_table_1970s.txt` | `https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/totals/e7079co.txt` | United States, Alabama, Autauga County, Charlottesville city; both half-decade blocks of each | The printed table layout: a `Census` column beside `Estimate` columns, and an area name that wraps onto a continuation line carrying its values |
| `legacy_table_1980s.txt` | `https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/e8089co.txt` | United States, Alabama, Autauga County; both blocks | The same layout one decade on, read by the same reader |
| `legacy_cells_1990s.txt` | `https://www2.census.gov/programs-surveys/popest/datasets/1990-2000/counties/asrh/co-99-10.txt` | Autauga County, all ten years, with the file's prose preamble | Eight race-by-Hispanic-origin cells and no published total; the preamble the reader must pass over |
| `co_2020s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv` | Alabama (`SUMLEV=040`), Autauga County (`SUMLEV=050`) | The current decade for the same two geographies, so one county's series can be asserted end to end |

Keep the page furniture. The block headers in the printed tables are what
tell the reader which years and which measure each column carries, and the
preamble is what the cell reader has to skip by shape rather than by
counting lines.

Keep each fixture lossless: do not rename columns, reformat values, or add
derived fields. Tests may interpret the source rows but fixtures must remain
source-shaped and replayable with network access disabled.
