# SILVER REF Dimensions

## Scope
Documents shared conformed dimensions in silver_ref used by all SILVER transforms and downstream GOLD serving.

## Objects
- silver_ref.dim_geo
- silver_ref.dim_time

## dim_geo Contract
Purpose: canonical geography hierarchy and lookup.

Key fields:

- geo_sk (surrogate key)
- geo_level (us, state, county)
- geo_id (canonical grammar)
- state_fips, county_fips
- name, state_name, county_name
- geom and coordinate metadata
- first_seen_year, last_seen_year, source lineage fields

Canonical geo_id grammar:

- us:1
- state:XX
- state:XX|county:YYY

## dim_time Contract
Purpose: canonical date dimension for day-level joins.

Key fields:

- time_sk
- date_key
- year, quarter, month, day
- period boundary flags (month/quarter/year start/end)
- day/week attributes

## Refresh and Dependency Order
Recommended dependency:

1. refresh silver_ref dimensions
2. execute source SILVER transforms
3. execute GOLD refresh procedures

SILVER transforms should treat missing dimension joins as data-quality signals and log counts before filtering/drop behavior.

## Operational Checks
1. Verify dim_time date coverage for ingest window before large backfills.
2. Verify dim_geo canonical padding for state/county FIPS before county loads.
3. Confirm unique grammar consistency in geo_id prior to SILVER merge.
