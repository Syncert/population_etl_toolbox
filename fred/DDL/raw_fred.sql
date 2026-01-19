-- raw_fred.sql
-- FRED raw schema + long table + metadata + slice ledger
-- Mirrors the conventions used in raw_census.sql

CREATE SCHEMA IF NOT EXISTS raw_fred;

-- -----------------------------
-- 1) Fact table (long format)
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw_fred.fred_long (
    id              BIGSERIAL PRIMARY KEY,

    domain          TEXT,                         -- optional logical grouping (housing/macro/etc.)
    series_id       TEXT NOT NULL,                -- FRED series ID

    obs_date        DATE NOT NULL,
    value           NUMERIC,
    is_missing      BOOLEAN NOT NULL DEFAULT FALSE, -- true if value was '.' or null-like in API

    -- useful metadata carried per row (optional)
    realtime_start  DATE,
    realtime_end    DATE,

    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS fred_long_uniq
    ON raw_fred.fred_long (series_id, obs_date, realtime_start, realtime_end);

CREATE INDEX IF NOT EXISTS fred_long_qc_slice
    ON raw_fred.fred_long (obs_date);

CREATE INDEX IF NOT EXISTS fred_long_qc_series
    ON raw_fred.fred_long (series_id);

-- -----------------------------
-- 2) Dataset/availability table (optional but matches ACS structure)
-- -----------------------------
-- FRED doesn’t really have “years available” in the same way, but you may want
-- a table to track series sync status or annual coverage checks.
CREATE TABLE IF NOT EXISTS raw_fred.fred_datasets (
    domain          TEXT NOT NULL,               -- e.g. 'housing'
    series_id       TEXT NOT NULL,

    is_available    BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at TIMESTAMPTZ,

    PRIMARY KEY (domain, series_id)
);

-- -----------------------------
-- 3) Series metadata table
-- -----------------------------
CREATE TABLE IF NOT EXISTS raw_fred.fred_series (
    series_id        TEXT NOT NULL,
    title            TEXT,
    units            TEXT,
    frequency        TEXT,
    seasonal_adjustment TEXT,

    observation_start DATE,
    observation_end   DATE,

    notes            TEXT,

    raw_metadata      JSONB,

    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (series_id)
);

-- -----------------------------
-- 4) Slice ledger (like acs_ingestion_slices)
-- -----------------------------
-- FRED slices are typically: series_ids[] + date range
-- We'll track: domain + date_start/date_end + series_hash
CREATE TABLE IF NOT EXISTS raw_fred.fred_ingestion_slices (
    id BIGSERIAL PRIMARY KEY,

    domain         TEXT NOT NULL,                 -- group label; use 'default' if you don’t care
    date_start     DATE NOT NULL,
    date_end       DATE NOT NULL,

    series_hash    TEXT,
    series_count   INTEGER,

    status         TEXT NOT NULL,                 -- planned/running/success/empty/failed
    rows_loaded    BIGINT NOT NULL DEFAULT 0,

    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    series_hash_seen_at TIMESTAMPTZ,

    last_error     TEXT
);

ALTER TABLE raw_fred.fred_ingestion_slices
  ADD CONSTRAINT chk_fred_status CHECK (status IN ('planned','running','success','empty','failed')),
  ADD CONSTRAINT chk_fred_dates CHECK (date_end >= date_start),
  ADD CONSTRAINT chk_fred_rows_loaded_non_negative CHECK (rows_loaded >= 0);

ALTER TABLE raw_fred.fred_ingestion_slices
  ADD CONSTRAINT chk_fred_started_before_finished
  CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at));

CREATE UNIQUE INDEX IF NOT EXISTS fred_ingestion_slices_uniq
ON raw_fred.fred_ingestion_slices (domain, date_start, date_end);

CREATE INDEX IF NOT EXISTS fred_ingestion_slices_status_idx
ON raw_fred.fred_ingestion_slices (status);

CREATE INDEX IF NOT EXISTS fred_ingestion_slices_hash_idx
ON raw_fred.fred_ingestion_slices (series_hash);