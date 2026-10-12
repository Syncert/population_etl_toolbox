# Operating the FEMA National Risk Index and declarations pipeline

The `fema_nri_ingest` DAG captures and publishes two FEMA products for
counties: expected annual losses and hazard frequencies from the National
Risk Index (NRI), and counts of disaster declarations from OpenFEMA. NRI
values are FEMA's modelled estimates for planning, not measurements; a
declaration count is this warehouse's count of FEMA's records. Every row
carries FEMA's notice: "This product uses the Federal Emergency Management
Agency's OpenFEMA API, but is not endorsed by FEMA."

## What is read

`src/data_ingestion_toolbox/fema_nri/registry.py` names both streams. Neither
needs a credential.

| Stream | Service | Paging |
| --- | --- | --- |
| `nri` | FEMA's ArcGIS layer `National_Risk_Index_Counties` (owner `FEMA_NationalRiskIndex`) | 2,000 records a page, ordered by `OBJECTID`, registered fields only, no geometry |
| `declarations` | `https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries` | 10,000 rows a page (`$top`/`$skip`), ordered by `id`, `$select` of the registered fields |

The NRI table zip on fema.gov refuses scripted requests (HTTP 403), so the
pipeline reads FEMA's own map service, which publishes the same county
table (3,232 rows, `NRI_VER = 'December 2025'`, which is v1.20.0). A page
that is not JSON, or that carries an ArcGIS `error` object (ArcGIS answers
some failures with HTTP 200), fails the capture; so does a stream that pages
past `max_pages`. The failed run keeps its errors in the ingestion ledger
and its pages are withdrawn.

## What is served

| Measure | From | Unit |
| --- | --- | --- |
| `expected_annual_loss` | `EAL_VALT` (rated by `EAL_RATNG`) | dollars per year |
| `expected_annual_loss_<hazard>` (18 hazards) | `<HAZARD>_EALT` (rated by `<HAZARD>_EALR`) | dollars per year |
| `annualized_frequency_<hazard>` (inland flooding, tornado, wildfire, hurricane, heat wave) | `<HAZARD>_AFREQ` | events per year |
| `major_disaster_declarations`, `emergency_declarations`, `fire_management_declarations` | distinct `disasterNumber` per county, calendar year of `declarationDate` and `declarationType` (DR, EM, FM) | declarations |

The NRI's `RISK_*` scores and ratings, social vulnerability and community
resilience are relative ranks and are not read.

| NRI rating | Served as |
| --- | --- |
| `Not Applicable` | `not_applicable`, `hazard_not_applicable`, no value |
| `Insufficient Data` | `missing`, `insufficient_data`, no value |
| `Data Unavailable` | `missing`, `data_unavailable`, no value |
| any other (`Very Low` .. `Very High`, `No Expected Annual Losses`, `No Rating`) | the value as FEMA publishes it |
| no value and no such rating | `missing`, `blank` |

A declaration row whose county code is `000` is a statewide or non-county
area (a tribal area, for example): kept in silver as an `area` and never
counted toward a county. OpenFEMA still designates Connecticut's eight legacy
counties; they resolve only where the shared geography holds them. Counties,
territories and the freely associated states (Micronesia, the Marshall
Islands, Palau) are read by FIPS, never by name. A county-year with no
declaration has no row.

## Schedule and scope

The DAG runs daily at 06:00 UTC; OpenFEMA refreshes declarations every
twenty minutes. An NRI read whose pages are byte-for-byte the last published
read's is recorded `unchanged` and replays nothing; a new NRI version is a
new release beside the old one. A declarations read keeps only revisions
(`id`, `hash`) it has not seen, and counts are made from the newest revision
of each row. A full declarations read is eight pages (70,431 rows on
2026-10-07).

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_fema_nri_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set fema_files 1 'FEMA NRI and OpenFEMA pages (serialized)'
```

## Checks after a run

```sql
SELECT stream, status, nri_version, page_count, record_count
FROM control.fema_nri_run ORDER BY created_at DESC LIMIT 6;

SELECT metric_key, value_status, COUNT(*)
FROM gold_fema_nri.observation_latest GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-FEMA-001` (uniqueness) and `DQ-FEMA-003` (a non-measure rating carries
  no number) are enforced by the silver keys and named CHECK constraints.
- `DQ-FEMA-002` fails on a read left unreplayed or a replayed NRI read that
  reached no fact, and runs in the daily sweep.
- `DQ-FEMA-004` warns on a negative loss or frequency, or a county that did
  not resolve.
