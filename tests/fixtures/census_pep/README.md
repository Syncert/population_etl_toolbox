# Census PEP fixtures

These are exact header-plus-one-row excerpts from registered U.S. Census
Population Estimates Program bulk CSV releases, retrieved 2026-08-24.

| Fixture | Official source | Retained row | Purpose |
| --- | --- | --- | --- |
| `nst_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/state/totals/NST-EST2025-ALLDATA.csv` | United States (`SUMLEV=010`) | Current release layout and observations through 2025 |
| `nst_2024.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/state/totals/NST-EST2024-ALLDATA.csv` | United States (`SUMLEV=010`) | Prior release and revised 2024 estimate comparison |
| `subcounty_2025.csv` | `https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv` | Abbeville city, Alabama (`SUMLEV=162`) | Incorporated-place codes and population columns |

Keep each fixture lossless: do not rename columns, reformat values, or add
derived fields. Tests may interpret the source rows but fixtures must remain
source-shaped and replayable with network access disabled.
