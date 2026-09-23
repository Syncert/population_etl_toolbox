-- 029: USDA NASS "OTHER (COMBINED) COUNTIES" is not a county.
--
-- Quick Stats publishes the counties it suppresses for disclosure as one
-- "OTHER (COMBINED) COUNTIES" row (or, in older years, "OTHER COUNTIES") per
-- agricultural district, under county_code 998 with a blank county_ansi. The
-- code is exact and three digits, so `geography_identity` resolved it to
-- `state:SS|county:998`, and every district of a state collided on that one
-- identity: `USDA_NASS:soybeans_survey_annual:3e38e231…` carried nine
-- different 1990 values for `state:05|county:998`, one per Arkansas district,
-- identical in every published dimension. No Census county carries code 998,
-- so the rows were `unmapped` -- and `unmapped` is still published, so the
-- API served them at the COUNTY grain under a fabricated identity. The
-- explorer map-display sweep (WEB-118) found it: 24 county maps failed.
--
-- `geography_identity` now resolves code 998 to `unsupported` with its
-- evidence kept (`geo_source_code = SS998`, `asd_code`, `county_name`,
-- `location_desc`), the path unsupported aggregate levels already take: the
-- row stays in silver, and gold and the publisher exclude it. Silver's
-- revision and fact writes are `ON CONFLICT DO NOTHING`, so a replay never
-- rewrites a stored row; this step rewrites the rows already stored to what
-- the code now produces. No value changes, and nothing is deleted.
--
-- Rerunnable: every statement matches only rows still in the old shape.

UPDATE silver_nass.observation_revision
   SET geo_type = 'unsupported',
       geo_id = NULL,
       geo_source_code = state_fips || county_fips,
       county_fips = NULL
 WHERE geo_type = 'county'
   AND county_fips = '998';

UPDATE silver_nass.fact_crop_observation
   SET geo_type = 'unsupported',
       geo_id = NULL,
       geo_sk = NULL,
       geography_status = 'unsupported',
       geo_source_code = state_fips || county_fips,
       county_fips = NULL
 WHERE geo_type = 'county'
   AND county_fips = '998';

-- The resolution ledger keyed these rows as county source codes `SS998`.
-- They are the same provider codes under the unsupported type, which is the
-- key the conformance step now writes, so a later run upserts onto these rows
-- rather than beside them.
UPDATE silver_ref.geography_resolution
   SET source_geo_type = 'unsupported',
       geo_sk = NULL,
       resolution_method = NULL,
       status = 'unsupported',
       reason_code = 'unsupported_aggregate_level',
       resolved_at = NOW()
 WHERE provider_source = 'USDA_NASS'
   AND source_geo_type = 'county'
   AND source_code ~ '^[0-9]{2}998$'
   AND NOT EXISTS (
       SELECT 1
         FROM silver_ref.geography_resolution AS existing
        WHERE existing.provider_source = geography_resolution.provider_source
          AND existing.provider_dataset = geography_resolution.provider_dataset
          AND existing.source_geo_type = 'unsupported'
          AND existing.source_code = geography_resolution.source_code
          AND existing.source_vintage = geography_resolution.source_vintage
   );

-- A ledger row the guard above left behind has an unsupported twin already;
-- the county-typed one describes an identity that no longer exists.
DELETE FROM silver_ref.geography_resolution
 WHERE provider_source = 'USDA_NASS'
   AND source_geo_type = 'county'
   AND source_code ~ '^[0-9]{2}998$';
