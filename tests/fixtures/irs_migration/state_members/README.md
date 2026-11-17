# Members of SOI's county migration zips

Copied verbatim on 2026-11-05 from SOI's zips at
`https://www.irs.gov/pub/irs-soi/`:

| Member | Zip | Layout |
| --- | --- | --- |
| `C9091dei.txt`, `C9091deo.txt` | `1990to1991countymigration.zip` (`1990to1991CountyMigrationInflow/`, `...Outflow/`) | Fixed-width text, returns and exemptions |
| `co934dei.xls`, `co934deo.xls` | `1993to1994countymigration.zip` | Workbook, codes `00 001` / `63 0xx`, total money income |
| `co0304DEi.xls`, `co0304DEo.xls` | `2003to2004countymigration.zip` | Workbook, CSV codes, AGI |
| `co1011ide.xls`, `co1011ode.xls` | `county1011.zip` | Workbook, codes as numbers, footnotes |
| `C9091aki.txt`, `C9091ako.txt` | `1990to1991countymigration.zip` | Alaska text: total lines whose cut labels never say "Total Migration"; Same State, Same Region, Different Region and All Migration Flows lines |
| `C9293Vti.xls`, `C9293Vto.xls` | `1992to1993countymigration.zip` | Vermont workbooks with every remaining-flow code, `63 010` to `63 030` |
| `co989idi.xls` | `1998to1999countymigration.zip` (`...Inflow/`) | Idaho workbook with revised figures (`r  128`) |
| `co989usi.xls`, `co989uso.xls` | `1998to1999countymigration.zip` (`...Inflow/`, `...Outflow/`) | US-level members: bare Excel 4.0 worksheets, no title, a tenth "Median" column |
| `co203ndi.xls` | `2002to2003countymigration.zip` (`...Inflow/`) | North Dakota workbook whose first county total has "North Dakota" in a twelfth cell |

The members added on 2026-11-05 for the 1998-2002 fix were exported from
the captures stored on the warehouse (byte for byte as SOI served them).

`tests/support/irs_migration_zips.py` packs one pair's members into a zip
for the adapter, the DAG stub and the database tests.
