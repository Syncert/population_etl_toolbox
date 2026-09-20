-- 025: a county resolved from a provider label is published as derived.
--
-- `_load_county_relationships` joins the FBI's county *label* to
-- `silver_ref.dim_geo_current.county_name` -- upper-cased, one legal suffix
-- stripped -- and wrote `resolution_method =
-- 'reviewed_county_name_crosswalk'`, `confidence_class = 'reviewed'`. No
-- reviewed artifact backs it. The place path earns `reviewed` from
-- `silver_fbi.reviewed_place_crosswalk`, a table with a reviewer, an evidence
-- URL and a review note; the state path records `exact` from the registered
-- state-code contract. The county path had neither and claimed the stronger
-- of the two, so `gold_fbi.agency_observation_area_filter` published
-- `filter_confidence_class = 'reviewed'` over a name match, and a consumer
-- filtering on that token could not tell the two apart.
--
-- AGENTS.md: "do not infer identity from names when authoritative
-- identifiers are required." The match stays -- it is exact and
-- uniqueness-checked, and removing it would darken every county filter in
-- the product -- but it is published as what it is.
--
-- Two vocabularies widen, and nothing already stored becomes invalid:
-- `derived` and `county_label_match` are new spellings, and
-- `agency_county_unresolved` separates "the provider named a county and it
-- did not resolve" from `agency_only`, which says the provider named none.
-- Existing rows are rewritten to the new method and class, because the rows
-- themselves never were reviewed.

-- Drop, rewrite, then constrain -- in that order, and the order is the whole
-- point.
--
-- This step shipped with the two `ADD CONSTRAINT`s before the `UPDATE` that
-- makes the rows satisfy them. `ADD CONSTRAINT` validates every existing row
-- immediately, so on any warehouse actually holding what this step exists to
-- correct it failed outright:
--
--     ERROR: check constraint
--     "agency_geography_relationship_resolution_method_check" of relation
--     "agency_geography_relationship" is violated by some row
--
-- On a fresh bootstrap the table is empty when this runs, so the order could
-- not matter and nothing reported it. The one shape it had to work on is the
-- one it could not. Rewriting first is not enough on its own either: the
-- *old* constraint does not admit `county_label_match`, so both have to be
-- dropped before the rewrite and added after it.

ALTER TABLE silver_fbi.agency_geography_relationship
    DROP CONSTRAINT IF EXISTS agency_geography_relationship_resolution_method_check;
ALTER TABLE silver_fbi.agency_geography_relationship
    DROP CONSTRAINT IF EXISTS agency_geography_relationship_confidence_class_check;

-- Rewrite what the old code wrote. The relationship is unchanged; only the
-- claim about how it was established is corrected.
UPDATE silver_fbi.agency_geography_relationship
   SET resolution_method = 'county_label_match',
       confidence_class = CASE WHEN confidence_class = 'reviewed'
                               THEN 'derived' ELSE confidence_class END,
       updated_at = NOW()
 WHERE resolution_method = 'reviewed_county_name_crosswalk';

UPDATE silver_fbi.agency_geography_relationship
   SET reason_code = 'county_label_unmatched',
       updated_at = NOW()
 WHERE relationship_type = 'county'
   AND reason_code = 'canonical_county_absent';

ALTER TABLE silver_fbi.agency_geography_relationship
    ADD CONSTRAINT agency_geography_relationship_resolution_method_check
    CHECK (resolution_method IS NULL OR resolution_method IN (
        'exact_state_code', 'county_label_match', 'reviewed_place_crosswalk'
    ));

ALTER TABLE silver_fbi.agency_geography_relationship
    ADD CONSTRAINT agency_geography_relationship_confidence_class_check
    CHECK (confidence_class IN ('exact', 'reviewed', 'derived', 'unresolved'));

ALTER TABLE silver_fbi.fact_reporting_participation
    DROP CONSTRAINT IF EXISTS fact_reporting_participation_geography_status_check;
ALTER TABLE silver_fbi.fact_reporting_participation
    ADD CONSTRAINT fact_reporting_participation_geography_status_check
    CHECK (geography_status IN (
        'provider_geo_exact', 'agency_only', 'agency_county_bridged',
        'agency_place_bridged', 'agency_county_unresolved', 'ambiguous',
        'unsupported'
    ));

ALTER TABLE silver_fbi.fact_crime_observation
    DROP CONSTRAINT IF EXISTS fact_crime_observation_geography_status_check;
ALTER TABLE silver_fbi.fact_crime_observation
    ADD CONSTRAINT fact_crime_observation_geography_status_check
    CHECK (geography_status IN (
        'provider_geo_exact', 'agency_only', 'agency_county_bridged',
        'agency_place_bridged', 'agency_county_unresolved', 'ambiguous',
        'unsupported'
    ));
