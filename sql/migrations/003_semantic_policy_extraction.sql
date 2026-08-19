-- ARCH-003 beta migration: remove authored semantics and consumer policy.
-- Export any non-placeholder authored content before applying this destructive beta cutover.

CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.dim_metric;
DROP VIEW IF EXISTS gold.dim_metric_catalog;
DROP VIEW IF EXISTS gold_glossary.dim_metric;

ALTER TABLE IF EXISTS gold_glossary.dim_metric_catalog
    DROP COLUMN IF EXISTS business_definition,
    DROP COLUMN IF EXISTS caveats,
    DROP COLUMN IF EXISTS dashboard_suitability,
    DROP COLUMN IF EXISTS comparability_group,
    DROP COLUMN IF EXISTS do_not_compare_with,
    DROP COLUMN IF EXISTS recommended_aggregation,
    DROP COLUMN IF EXISTS owner_team,
    DROP COLUMN IF EXISTS is_active;

CREATE OR REPLACE VIEW gold_glossary.dim_metric AS
SELECT
    metric_code,
    metric_display_name,
    source_code,
    source_object_type,
    source_object_key,
    units,
    measure_kind,
    valid_geo_grains,
    valid_time_grains,
    aggregation_characteristic,
    physical_lineage,
    publisher_contract_version,
    source_watermark,
    source_run_id,
    publication_time,
    harvested_at,
    freshness_state,
    freshness_state = 'current' AS is_active
FROM gold_glossary.dim_metric_catalog;

CREATE OR REPLACE VIEW gold.dim_metric_catalog AS
SELECT * FROM gold_glossary.dim_metric_catalog;

CREATE OR REPLACE VIEW gold.dim_metric AS
SELECT * FROM gold_glossary.dim_metric;

COMMENT ON SCHEMA gold_glossary IS
    'Harvested source identity, units, grains, lineage, and freshness only. Reviewed definitions live under docs/semantics.';
