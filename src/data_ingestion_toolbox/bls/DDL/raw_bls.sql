-- raw_bls.sql
-- BLS source metadata. Immutable responses live in raw_capture and parsed
-- observations live in silver_bls.observation_revision.

CREATE SCHEMA IF NOT EXISTS raw_bls;

-- -----------------------------
-- Dataset/program availability table
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
