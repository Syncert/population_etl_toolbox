-- 026: correct the release dates the refresh clock already materialised.
--
-- `gold_bls.fact_bls_observation`, `gold_fred.fact_fred_observation` and
-- `gold_census.fact_acs_observation` published `CURRENT_DATE AS as_of_date`,
-- and the chunked serving refresh wrote that literal into the reporting
-- tables. A served "release" was therefore the calendar day a chunk was last
-- written: the driver re-serves only changed years, so re-serving 2019 on
-- Monday and 2020 on Tuesday made `/observations/releases` list two published
-- releases the provider never published, and a full re-serve collapsed every
-- release into one.
--
-- The views now read `ingested_at::DATE` -- see the comment in each gold DDL
-- for why that is the honest identity -- but every row already served carries
-- the old value until its year is re-served, and a forced full re-serve of
-- ACS is 68 million rows. `updated_at` in these relations *is* the silver
-- row's `ingested_at`, so the correction is exact and needs no re-serve.
--
-- The view definitions are not restated here. They live in the phase DDL
-- files, which the bootstrap applies in the `gold` phase before this step and
-- which the ingestion DAGs re-apply on every run; a second copy of a
-- sixty-line view body in a migration is a copy that drifts from the one the
-- warehouse actually gets.

UPDATE gold_bls.rpt_bls_observations
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;

UPDATE gold_bls.mv_bls_latest
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;

UPDATE gold_fred.rpt_fred_observations
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;

UPDATE gold_fred.mv_fred_latest
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;

UPDATE gold_census.rpt_acs_observations
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;

UPDATE gold_census.mv_acs_latest
   SET as_of_date = updated_at::DATE
 WHERE as_of_date <> updated_at::DATE;
