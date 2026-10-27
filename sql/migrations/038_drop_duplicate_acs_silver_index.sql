-- Drop an index that duplicates a unique one (load performance review).
--
-- `silver_census.fact_demographics` carried two identical B-tree indexes on
-- `(dataset, table_id, variable_code, geo_id, estimate_year)`: the unique
-- `fact_demographics_uk`, which the upsert's `ON CONFLICT` uses, and
-- `idx_fact_demo_upsert_key`, which nothing needs. On the internal stack in
-- October 2026 the duplicate was 31 GB with 24 scans against the unique
-- index's 110 million, and every row the ACS transform writes paid to
-- maintain both. `silver_census.sql` no longer creates it. Idempotent.

DROP INDEX IF EXISTS silver_census.idx_fact_demo_upsert_key;
