-- raw_bls.sql
-- BLS raw schema + long table + metadata + slice ledger
-- Mirrors the conventions used in raw_census.sql

CREATE SCHEMA IF NOT EXISTS raw_bls;

-- -----------------------------
-- 1) Fact table (long format)
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw_bls.bls_long (
    id              BIGSERIAL PRIMARY KEY,

    program         TEXT NOT NULL,               -- e.g. 'la', 'ln', 'ce', 'cu', 'jt' (expand later)
    series_id       TEXT NOT NULL,               -- BLS series ID

    year            INTEGER NOT NULL,
    period          TEXT NOT NULL,               -- e.g. 'M01'..'M12', 'M13' annual avg, 'Q01'.. (program-dependent)
    period_name     TEXT,                        -- optional friendly label returned by API

    value           NUMERIC,
    footnotes       JSONB,                       -- optional: store raw footnotes array/object if returned
    is_latest       BOOLEAN,                     -- optional: if API flags latest

    -- Optional derived fields (populate when you can parse series/area codes)
    geo_level       TEXT,                        -- 'us'/'state'/'county'/'msa'/etc. (optional)
    geo_id          TEXT,                        -- align to your geo_dim style if you choose (optional)
    state_fips      TEXT,
    county_fips     TEXT,

    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS bls_long_uniq
    ON raw_bls.bls_long (program, series_id, year, period);

-- Compact helper index: speeds up DISTINCT(program) and other quick program lookups.
-- (GROUP BY program still needs to scan all rows for exact counts.)
CREATE INDEX IF NOT EXISTS bls_long_program_idx
    ON raw_bls.bls_long (program);

CREATE INDEX IF NOT EXISTS bls_long_qc_slice
    ON raw_bls.bls_long (program, year);

CREATE INDEX IF NOT EXISTS bls_long_qc_series
    ON raw_bls.bls_long (program, series_id);

CREATE INDEX IF NOT EXISTS bls_long_qc_geo
    ON raw_bls.bls_long (geo_level, geo_id);

-- Basic domain checks (lightweight, avoids weird junk)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'bls_long_period_chk'
    ) THEN
        ALTER TABLE raw_bls.bls_long
        ADD CONSTRAINT bls_long_period_chk
        CHECK (period ~ '^[A-Z][0-9]{2}$'); -- e.g. M01, M13, Q01 (program-dependent but format-consistent)
    END IF;
END $$;

-- -----------------------------
-- 2) Dataset/program availability table
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw_bls.bls_datasets (
    program          TEXT NOT NULL,             -- 'laus', 'ces', ...
    year             INTEGER NOT NULL,

    title            TEXT,
    is_available     BOOLEAN NOT NULL DEFAULT TRUE,

    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at TIMESTAMPTZ,

    PRIMARY KEY (program, year)
);

-- -----------------------------
-- 3) Series metadata table
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw_bls.bls_series (
    program          TEXT NOT NULL,
    series_id        TEXT NOT NULL,

    -- Common descriptive fields (store what you can)
    title            TEXT,
    seasonal         TEXT,
    measure          TEXT,
    area_code        TEXT,                      -- if applicable
    area_text        TEXT,                      -- if applicable

    raw_metadata     JSONB,                     -- store the raw series metadata payload if you want

    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (program, series_id)
);

CREATE INDEX IF NOT EXISTS bls_series_program_idx
    ON raw_bls.bls_series (program);

-- -----------------------------
-- 4) Slice ledger (like acs_ingestion_slices)
-- -----------------------------
-- BLS slices differ from ACS:
-- - The API request is usually: series_ids[] + time window
-- So we track: program + year_start/year_end + series_hash
CREATE TABLE IF NOT EXISTS raw_bls.bls_ingestion_slices (
    id BIGSERIAL PRIMARY KEY,

    program        TEXT NOT NULL,                -- 'laus', etc.
    year_start     INTEGER NOT NULL,
    year_end       INTEGER NOT NULL,
    
    geo_level      TEXT,                         -- 'us'/'state'/'county' (for LAUS), NULL for other programs
    state_fips     TEXT,                         -- state FIPS for county-level LAUS, NULL otherwise

    series_hash    TEXT,
    series_count   INTEGER,

    status         TEXT NOT NULL,                -- planned/running/success/empty/failed
    rows_loaded    BIGINT NOT NULL DEFAULT 0,

    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    series_hash_seen_at TIMESTAMPTZ,

    last_error     TEXT
);

-- Domain checks (mirrors ACS style)
ALTER TABLE raw_bls.bls_ingestion_slices
  ADD CONSTRAINT chk_bls_status CHECK (status IN ('planned','running','success','empty','failed')),
  ADD CONSTRAINT chk_bls_years CHECK (year_start >= 1900 AND year_end >= year_start AND year_end <= EXTRACT(YEAR FROM CURRENT_DATE) + 1),
  ADD CONSTRAINT chk_bls_rows_loaded_non_negative CHECK (rows_loaded >= 0);

ALTER TABLE raw_bls.bls_ingestion_slices
  ADD CONSTRAINT chk_bls_started_before_finished
  CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at));

-- Uniqueness per program + time window + geo_level + state_fips
-- NOTE: We do NOT include series_hash in uniqueness because you want one row per window; hash is used for skip logic.
-- For LAUS (program='la'), we need separate rows for different geo_levels and state_fips.
-- For other programs, geo_level and state_fips are NULL.
-- Using COALESCE to handle NULLs in unique index.
CREATE UNIQUE INDEX IF NOT EXISTS bls_ingestion_slices_uniq
ON raw_bls.bls_ingestion_slices (program, year_start, year_end, COALESCE(geo_level, ''), COALESCE(state_fips, ''));

CREATE INDEX IF NOT EXISTS bls_ingestion_slices_status_idx
ON raw_bls.bls_ingestion_slices (status);

CREATE INDEX IF NOT EXISTS bls_ingestion_slices_hash_idx
ON raw_bls.bls_ingestion_slices (series_hash);

CREATE INDEX IF NOT EXISTS bls_ingestion_slices_geo_idx
ON raw_bls.bls_ingestion_slices (geo_level);

CREATE INDEX IF NOT EXISTS bls_ingestion_slices_state_idx
ON raw_bls.bls_ingestion_slices (state_fips);