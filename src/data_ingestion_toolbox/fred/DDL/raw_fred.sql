-- raw_fred.sql
-- FRED source metadata. Immutable responses live in raw_capture and parsed
-- observations live in silver_fred.observation_revision.

CREATE SCHEMA IF NOT EXISTS raw_fred;

-- -----------------------------
-- Dataset/availability table (optional but matches ACS structure)
-- -----------------------------
-- FRED doesnâ€™t really have â€œyears availableâ€ in the same way, but you may want
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
