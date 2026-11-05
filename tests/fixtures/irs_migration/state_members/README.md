# Delaware members of SOI's county migration zips

Copied verbatim on 2026-11-05 from SOI's zips at
`https://www.irs.gov/pub/irs-soi/`:

| Member | Zip | Layout |
| --- | --- | --- |
| `C9091dei.txt`, `C9091deo.txt` | `1990to1991countymigration.zip` (`1990to1991CountyMigrationInflow/`, `...Outflow/`) | Fixed-width text, returns and exemptions |
| `co934dei.xls`, `co934deo.xls` | `1993to1994countymigration.zip` | Workbook, codes `00 001` / `63 0xx`, total money income |
| `co0304DEi.xls`, `co0304DEo.xls` | `2003to2004countymigration.zip` | Workbook, CSV codes, AGI |
| `co1011ide.xls`, `co1011ode.xls` | `county1011.zip` | Workbook, codes as numbers, footnotes |

`tests/support/irs_migration_zips.py` packs one pair's members into a zip
for the adapter, the DAG stub and the database tests.
