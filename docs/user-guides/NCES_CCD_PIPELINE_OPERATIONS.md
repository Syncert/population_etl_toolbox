# Operating the NCES school pipeline

The `nces_ccd_ingest` DAG captures NCES's Common Core of Data (CCD) public
school files and NCES EDGE's public-school geocodes, and publishes county
and state school figures. NCES publishes schools, not counties: each figure
is this warehouse's sum over the schools EDGE places in the county or state,
and every row says how many placed schools had no value.

## What is read

`src/data_ingestion_toolbox/nces_ccd/registry.py` names, per school year
(2023-24 and 2024-25), these zips. No credential is needed.

| Component | File (2024-25) | Kept |
| --- | --- | --- |
| EDGE geocode | `programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip` (pipe-delimited `.TXT`, no header) | each school's physical state (`STFIP`), county (`CNTY`), operating state (`OPSTFIPS`), coordinates |
| Directory | `ccd/Data/zip/ccd_sch_029_2425_w_1a_073025.zip` | status, school type, charter flag, level |
| Staff | `ccd/Data/zip/ccd_sch_059_2425_l_1a_073025.zip` | teachers (FTE), the `Education Unit Total` row |
| Lunch | `ccd/Data/zip/ccd_sch_033_2425_l_2a_073025.zip` | FRPL total, free, reduced-price, direct certification |

The file names come from the CCD Data File Tool's catalog
(`https://nces.ed.gov/ccd/datatables/api/File`). NCES puts the release in
the name (`1a`, `2a`), so a new release is a registry change; it is captured
beside the earlier one and served in its place for that school year.

**Membership (enrollment) is not registered.** NCES compresses the membership
zips (`ccd_sch_052_*`, about 210 MB, 2.3 GB uncompressed) with Deflate64,
which the standard library cannot read. The component and its measure
(`student_membership`) are defined; registering the two files is the only
change once a Deflate64 reader is chosen.

## Values and placement

Every CCD row carries `DMS_FLAG`. Only `Reported` keeps a number;
`Not reported` and `Missing` are `missing` and `Suppressed` is
`suppressed`, each with no value -- never zero. A blank or negative
`Reported` value, an unknown flag, a malformed identifier, a wrong school
year, an EDGE county outside its state, or a repeated row quarantines that
row alone. `NCESSCH` (twelve characters) and `LEAID` (seven) stay strings.

Schools are placed by EDGE's `CNTY` and `STFIP`, resolved to the shared
geography by code. BIE (`OPSTFIPS` 59) and DoDEA (63) schools are placed
where they physically stand and are never resolved as a state. A county
code the shared geography lacks is recorded `canonical_geography_absent`.

## What is served

`gold_nces_ccd.observation_revision` serves, per published file, the county
and state sum of placed schools' Reported values for `operating_schools`,
`charter_schools`, `teacher_fte`, `frpl_eligible`, `free_lunch_eligible`,
`reduced_price_lunch_eligible` and `direct_certification`, with
`schools_with_value`, `schools_without_value` and `completeness`. A grain
where no placed school reported has status `missing` and no value. Rows
carry `year` = the fall of the school year. `gold_nces_ccd.observation_latest`
serves the newest release, then the newest read. District facts are not
published and nothing is apportioned by area.

## Schedule and scope

The DAG runs at 20:00 UTC on the 3rd of January, April, July and October,
one mapped task per file through the one-slot `nces_ccd_files` pool. A read
whose bytes equal the file's last published capture is recorded `unchanged`
and replays nothing.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_nces_ccd_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set nces_ccd_files 1 'NCES CCD and EDGE files (serialized)'
```

## Checks after a run

```sql
SELECT component, school_year, release_version, status, row_count, kept_row_count
FROM control.nces_ccd_file ORDER BY created_at DESC LIMIT 10;

SELECT metric_key, geo_type, completeness, COUNT(*)
FROM gold_nces_ccd.observation_latest GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
```

## Quality rules

- `DQ-NCES-001` (uniqueness) and `DQ-NCES-003` (a withheld count carries no
  number; none is negative) are enforced by silver keys and named CHECK
  constraints.
- `DQ-NCES-002` fails on a captured file left unreplayed or a replayed one
  that reached no row, and runs in the daily sweep.
- `DQ-NCES-004` warns on a school with a published value that the year's
  geocode file does not place in a county the shared geography holds, or a
  school whose FRPL count exceeds its membership (once membership is read).

Source: U.S. Department of Education, National Center for Education
Statistics, Common Core of Data and EDGE school geocodes. NCES publications
state that their contents are in the public domain unless noted.
