# Census PEP fixtures

These are exact header-plus-one-row excerpts from registered U.S. Census
Population Estimates Program bulk CSV releases, retrieved 2026-08-24.

| Fixture | Official source | Retained row | Purpose |
| --- | --- | --- | --- |
| `nst_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/state/totals/NST-EST2025-ALLDATA.csv` | United States (`SUMLEV=010`) | Current release layout and observations through 2025 |
| `nst_2024.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/state/totals/NST-EST2024-ALLDATA.csv` | United States (`SUMLEV=010`) | Prior release and revised 2024 estimate comparison |
| `subcounty_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv` | Abbeville city, Alabama (`SUMLEV=162`) | Incorporated-place codes and population columns |

The closed-decade fixtures below were retrieved 2026-09-10 and cut the same
way: the file's own header, then whole rows selected byte-for-byte, rejoined
with the separator the source file uses.

| Fixture | Official source | Retained rows | Purpose |
| --- | --- | --- | --- |
| `co_2010s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv` | Alabama (`SUMLEV=040`), Autauga County (`SUMLEV=050`) | Vintage 2020 layout: `NATURALINC` spelling, `CENSUS2010POP`, and a state row published as a rollup |
| `nst_2010s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/state/totals/nst-est2020-alldata.csv` | United States (`SUMLEV=010`), Alabama (`SUMLEV=040`) | The same state row published in its own right, and the July 2020 seam the 2020s series revises |
| `co_2000s.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2000-2009/counties/totals/co-est2009-alldata.csv` | Alabama (`SUMLEV=040`), Autauga County (`SUMLEV=050`) | Vintage 2009 layout and `CENSUS2000POP` |

Alabama and Autauga County are deliberate: both have existed unchanged
across every decade registered here, so a fixture assertion tests the
pipeline rather than a boundary change.

Keep each fixture lossless: do not rename columns, reformat values, or add
derived fields. Tests may interpret the source rows but fixtures must remain
source-shaped and replayable with network access disabled.
