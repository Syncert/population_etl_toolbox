-- ARCH-002 beta migration: independently owned, provider-extensible glossary harvest.

CREATE SCHEMA IF NOT EXISTS gold_glossary;

CREATE TABLE IF NOT EXISTS gold_glossary.publisher_registry (
    source_code                TEXT PRIMARY KEY,
    publisher_schema           TEXT NOT NULL,
    publisher_view             TEXT NOT NULL DEFAULT 'metric_publisher',
    publisher_contract_version TEXT NOT NULL,
    discovery_status           TEXT NOT NULL DEFAULT 'active'
        CHECK (discovery_status IN ('active', 'invalid', 'unavailable', 'retired')),
    last_discovered_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error                 TEXT,
    UNIQUE (publisher_schema, publisher_view)
);

CREATE TABLE IF NOT EXISTS gold_glossary.dim_source_system (
    source_system_sk   BIGSERIAL PRIMARY KEY,
    source_code        TEXT NOT NULL UNIQUE,
    source_name        TEXT NOT NULL,
    source_type        TEXT NOT NULL,
    reference_url      TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_harvested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_harvested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE gold_glossary.dim_source_system
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS first_harvested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS last_harvested_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE gold_glossary.dim_source_system
    DROP CONSTRAINT IF EXISTS dim_source_system_source_type_check;

CREATE TABLE IF NOT EXISTS gold_glossary.dim_metric_catalog (
    metric_catalog_sk          BIGSERIAL PRIMARY KEY,
    metric_code                TEXT NOT NULL UNIQUE,
    source_code                TEXT NOT NULL
        REFERENCES gold_glossary.dim_source_system(source_code),
    source_object_type         TEXT NOT NULL,
    source_object_key          TEXT,
    metric_display_name        TEXT NOT NULL,
    business_definition        TEXT,
    caveats                    TEXT,
    units                      TEXT,
    measure_kind               TEXT,
    valid_geo_grains           TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    aggregation_characteristic TEXT,
    physical_lineage           JSONB NOT NULL DEFAULT '{}'::JSONB,
    publisher_contract_version TEXT,
    source_watermark           TEXT,
    source_run_id              UUID,
    publication_time           TIMESTAMPTZ,
    harvested_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    freshness_state            TEXT NOT NULL DEFAULT 'current'
        CHECK (freshness_state IN ('current', 'stale', 'retired')),
    missing_harvest_count      INTEGER NOT NULL DEFAULT 0
        CHECK (missing_harvest_count >= 0),
    dashboard_suitability      TEXT NOT NULL DEFAULT 'PUBLIC_SAFE',
    comparability_group        TEXT,
    do_not_compare_with        TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    recommended_aggregation    TEXT,
    owner_team                 TEXT,
    is_active                  BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_code, source_object_type, source_object_key)
);

ALTER TABLE gold_glossary.dim_metric_catalog
    ADD COLUMN IF NOT EXISTS source_object_key TEXT,
    ADD COLUMN IF NOT EXISTS units TEXT,
    ADD COLUMN IF NOT EXISTS measure_kind TEXT,
    ADD COLUMN IF NOT EXISTS aggregation_characteristic TEXT,
    ADD COLUMN IF NOT EXISTS physical_lineage JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN IF NOT EXISTS publisher_contract_version TEXT,
    ADD COLUMN IF NOT EXISTS source_watermark TEXT,
    ADD COLUMN IF NOT EXISTS source_run_id UUID,
    ADD COLUMN IF NOT EXISTS publication_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS harvested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS freshness_state TEXT NOT NULL DEFAULT 'current',
    ADD COLUMN IF NOT EXISTS missing_harvest_count INTEGER NOT NULL DEFAULT 0;

-- Transitional compatibility only. ARCH-003 removes these authored-policy
-- columns after report/API consumers stop reading them. They are declared here
-- so an empty-database bootstrap does not depend on legacy source DDL running
-- before the independently owned glossary migration.
ALTER TABLE gold_glossary.dim_metric_catalog
    ADD COLUMN IF NOT EXISTS business_definition TEXT,
    ADD COLUMN IF NOT EXISTS caveats TEXT,
    ADD COLUMN IF NOT EXISTS dashboard_suitability TEXT NOT NULL DEFAULT 'PUBLIC_SAFE',
    ADD COLUMN IF NOT EXISTS comparability_group TEXT,
    ADD COLUMN IF NOT EXISTS do_not_compare_with TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    ADD COLUMN IF NOT EXISTS recommended_aggregation TEXT,
    ADD COLUMN IF NOT EXISTS owner_team TEXT,
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE gold_glossary.dim_metric_catalog
    DROP CONSTRAINT IF EXISTS dim_metric_catalog_source_object_type_check;

CREATE TABLE IF NOT EXISTS gold_glossary.publisher_harvest_state (
    source_code                TEXT PRIMARY KEY
        REFERENCES gold_glossary.dim_source_system(source_code),
    publisher_contract_version TEXT NOT NULL,
    last_source_watermark      TEXT,
    last_source_run_id         UUID,
    last_publication_time      TIMESTAMPTZ,
    last_harvest_started_at    TIMESTAMPTZ,
    last_harvest_completed_at  TIMESTAMPTZ,
    status                     TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'success', 'failed', 'stale')),
    last_error                 TEXT
);

CREATE TABLE IF NOT EXISTS control.publisher_ready_event (
    event_id                   UUID PRIMARY KEY,
    source_code                TEXT NOT NULL,
    publisher_contract_version TEXT NOT NULL,
    source_watermark           TEXT NOT NULL,
    source_run_id              UUID,
    publication_time           TIMESTAMPTZ NOT NULL,
    status                     TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'processed', 'failed', 'superseded')),
    attempt_count              INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at                 TIMESTAMPTZ,
    processed_at               TIMESTAMPTZ,
    last_error                 TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_code, publisher_contract_version, source_watermark)
);

CREATE INDEX IF NOT EXISTS publisher_ready_event_pending_idx
    ON control.publisher_ready_event (status, available_at, created_at);
CREATE INDEX IF NOT EXISTS metric_catalog_source_freshness_idx
    ON gold_glossary.dim_metric_catalog (source_code, freshness_state);

COMMENT ON TABLE gold_glossary.dim_metric_catalog IS
    'Automatically harvested source facts only; authored semantics and serving policy are forbidden.';
COMMENT ON COLUMN gold_glossary.dim_metric_catalog.aggregation_characteristic IS
    'Nullable source-supported aggregation behavior; unknown is stored as NULL.';
