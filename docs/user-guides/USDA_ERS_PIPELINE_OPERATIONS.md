# Operating the USDA ERS county codes and atlas pipeline

The `usda_ers_ingest` DAG captures and publishes three USDA Economic Research
Service county products: the Rural-Urban Continuum Codes (RUCC), the County
Typology Codes and four Food Environment Atlas indicators. RUCC and the
Typology are classifications -- codes and flags, not quantities -- and every
row says so. Every row credits "USDA, Economic Research Service".

## What is registered

`src/data_ingestion_toolbox/usda_ers/registry.py` names each file under
`https://www.ers.usda.gov/media/`. None needs a credential.

| Product | Path | Layout | Encoding |
| --- | --- | --- | --- |
| RUCC 2023 | `5768/2023-rural-urban-continuum-codes.csv` | `FIPS,State,County_Name,Attribute,Value` | Windows-1252 |
| County Typology Codes 2025 | `6174/ers-county-typology-codes-2025-edition.csv` | `FIPStxt,...,Attribute,Value,...` | UTF-8 |
| Food Environment Atlas, July 2025 | `5570/food-environment-atlas-csv-files.zip` (member `StateAndCountyData.csv`) | `FIPS,State,County,Variable_Code,Value` | UTF-8 with BOM |

Every file is long: one row per county and attribute. Only the registered
attributes are loaded; the rest (most of the Atlas's 280 variables) are
counted as out of scope. A response that does not decode, lacks the zip
member or the registered columns fails the capture.

## What is served

| Measure | From | Unit | Year |
| --- | --- | --- | --- |
| `rural_urban_continuum_code` | `RUCC_2023`, with ERS's `Description` as `code_label` | code (1-9) | 2023 |
| twelve Typology flags, e.g. `farming_dependent`, `persistent_poverty` | `High_Farming_2025`, ... | flag (0/1) | 2025 |
| `industry_dependence` | `Industry_Dependence_2025` | code (0-5) | 2025 |
| `snap_authorized_stores` | `SNAPS17`, `SNAPS23` | stores | 2017, 2023 |
| `snap_authorized_stores_per_1000` | `SNAPSPTH17`, `SNAPSPTH23` | stores per 1,000 people | 2017, 2023 |
| `snap_households_low_store_access` | `LACCESS_SNAP15`, `LACCESS_SNAP19` | households | 2015, 2019 |
| `snap_households_low_store_access_pct` | `PCT_LACCESS_SNAP15`, `PCT_LACCESS_SNAP19` | percent | 2015, 2019 |

RUCC's `Population_2020` is kept in silver, not published. ERS's mapping
from `industry_dependence` codes 1-5 to the five industries is not stated in
its documentation, so no label is served for it.

| Cell | Served as |
| --- | --- |
| Typology `0` | `valid`, value 0: the county is not flagged |
| Typology `99` | `not_applicable`, `not_computed_for_geography` (Connecticut: ACS-based flags exist for its planning regions, the others for its eight legacy counties) |
| Typology `-1` (persistent poverty, 24 counties) | `not_applicable`, `not_determined`; ERS does not define it |
| Atlas `-9999` | `missing`, `not_available` |
| Atlas `-8888` | `missing`, `county_did_not_exist` |
| Atlas `N/A` | `missing`, `incomplete_data` |
| Atlas empty cell | `missing`, `blank` |

A county with no RUCC code (American Samoa's uninhabited Rose and Swains
Islands) has no RUCC row. Counties resolve through the shared geography by
FIPS, never by name; Connecticut's planning regions, its legacy counties and
the territories resolve where the shared dimension holds them and are
recorded as `canonical_geography_absent` otherwise.

## Schedule and scope

The DAG runs at 17:00 UTC on the 25th of each month and reads every
registered file. ERS replaces files in place and sends no validator, so
every read downloads the files (about 13 MB together); a read whose bytes
equal the file's last published capture is recorded `unchanged` and replays
nothing. A replaced file is a new release beside the old one, keyed by
product, edition and read time. To add an edition (RUCC 2013, Typology
2015), register it after checking its layout; ERS says editions are not
comparable.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_usda_ers_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set usda_ers_files 1 'USDA ERS files (serialized)'
```

## What a run does

1. Commits one capture per file and records it in `control.usda_ers_file`
   with its checksum, as `captured` or `unchanged`.
2. Replays a `captured` file into `silver_usda_ers.observation_revision`,
   one row per county and registered attribute. A bad FIPS, a code or flag
   outside its domain, a text value, a short row or a repeated county and
   attribute goes to `observation_quarantine`. It writes the geography
   resolution ledger and conforms `silver_usda_ers.fact_observation`,
   committing only if the counts reconcile.
3. Marks the file `published`, which exposes it through
   `gold_usda_ers.observation_revision` and `observation_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT product, edition, status, row_count, in_scope_row_count
FROM control.usda_ers_file ORDER BY created_at DESC LIMIT 6;

SELECT metric_key, value_status, missing_reason, COUNT(*)
FROM gold_usda_ers.observation_latest GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
```

## Quality rules

- `DQ-ERS-001` (uniqueness) and `DQ-ERS-003` (a sentinel or unset flag
  carries no number) are enforced by the fact's key and named CHECK
  constraints; the parser quarantines a code or flag outside its domain.
- `DQ-ERS-002` fails on a captured file left unreplayed or a replayed one
  that reached no fact, and runs in the daily sweep.
- `DQ-ERS-004` warns on a county row that did not resolve or a RUCC code
  without ERS's label.
