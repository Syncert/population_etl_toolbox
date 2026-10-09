# EIA retail gasoline pipeline operations

The `eia_retail_gasoline_ingest` DAG loads the U.S. Energy Information
Administration's weekly retail gasoline prices (EIA-878 survey) through API
v2, route `petroleum/pri/gnd` (grocery-and-gasoline-prices).

## What is registered

| Product | Grade | Metric code |
| --- | --- | --- |
| `EPMR` | Regular gasoline | `EIA:EPMR` |
| `EPMM` | Midgrade gasoline | `EIA:EPMM` |
| `EPMP` | Premium gasoline | `EIA:EPMP` |
| `EPM0` | All grades | `EIA:EPM0` |

Diesel is not registered. Each price is in U.S. dollars per gallon (`$/GAL`)
for the week starting its `period` (a Monday).

Areas are read from their EIA code, never their name:

| Code | Area | Served as |
| --- | --- | --- |
| `NUS` | United States | `us:1`, `NATIONAL` |
| `S` + USPS code (`SCA`) | A state | `state:<FIPS>`, through the USPS code the shared reference's Census Gazetteer carries |
| `R...` | A PADD or sub-district | `area:eia:<code>`, `PROVIDER_AREA` |
| `Y...` | A city | `area:eia:<code>`, `PROVIDER_AREA`; never assigned to a CBSA |

EIA's PADDs and cities are loaded from the route's own `duoarea` facet by
the DAG's `load_eia_areas` task. That list needs the key, so the reference
DAG does not load it.

## Credential

`EIA_API_KEY`, free from <https://www.eia.gov/opendata/register.php>. It is
read from the environment when the capture runs, sent only as the `api_key`
query parameter, and never written to a request record, a capture, a log or
an error. The client refuses an answer that echoes the key.

## Schedule and scope

Tuesdays at 15:00 UTC: EIA publishes Monday's prices on Monday afternoon.
A warehouse with no published read reads every week from 2015-01-05; later
runs read from eight weeks before the newest published week, so a revised
week is read again. Requests go through the one-slot `eia_api` pool, one a
second.

## What a run does

1. `ensure_eia_schema` applies the control, silver, gold and publisher DDL.
2. `require_shared_geography` refuses to run on an unloaded reference.
3. `load_eia_areas` captures the `duoarea` facet and loads the PADDs and
   cities.
4. `ingest_batch_weeks` captures every page of the window (sorted by week
   and series, 5,000 rows a page) before parsing any, replays them into
   `silver_eia.price_revision` and `silver_eia.fact_retail_price`, and
   publishes the read.

A total that changes while paging, or a read that ends short, fails the run
with its captures kept. A row whose grade, unit, week, area or value cannot
be read is set aside in `silver_eia.observation_quarantine` with its reason;
a week with no price is kept as `missing` with no number.

## Checks after a run

```sql
SELECT status, row_total, parsed_row_count FROM control.eia_read ORDER BY created_at DESC LIMIT 5;
SELECT geo_level, COUNT(*) FROM gold_eia.observation_latest GROUP BY 1;
SELECT source_code, status FROM silver_ref.geography_resolution
 WHERE provider_source = 'EIA' AND status <> 'resolved';
```

## Quality rules

- `DQ-EIA-001` (uniqueness) and `DQ-EIA-003` (a price is positive, a missing
  week has no number) are enforced by the fact's key and named CHECKs.
- `DQ-EIA-002` reconciles each read's rows and runs in the daily sweep.
- `DQ-EIA-004` (every area resolves) is declared; unresolved areas are in
  `silver_ref.geography_resolution`.

Data are used under EIA's
[copyrights and reuse policy](https://www.eia.gov/about/copyrights_reuse.php);
cite "U.S. Energy Information Administration, Gasoline and Diesel Fuel
Update".
