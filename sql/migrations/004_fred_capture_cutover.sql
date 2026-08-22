-- ARCH-007 beta cutover: captured FRED observation revisions are parsed in silver.
-- A clean beta reset/re-ingestion is preferred; this file is also safe on bootstrap.

CREATE SCHEMA IF NOT EXISTS silver_fred;

CREATE TABLE IF NOT EXISTS silver_fred.observation_revision (
    capture_id               UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    observation_index        INTEGER NOT NULL CHECK (observation_index >= 0),
    domain                   TEXT,
    series_id                TEXT NOT NULL,
    observation_date_source  TEXT NOT NULL,
    value_source             TEXT,
    realtime_start_source    TEXT,
    realtime_end_source      TEXT,
    observation_date         DATE NOT NULL,
    value                    NUMERIC,
    value_status             TEXT NOT NULL
        CHECK (value_status IN ('valid', 'missing', 'invalid')),
    realtime_start           DATE,
    realtime_end             DATE,
    parsed_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version           TEXT NOT NULL DEFAULT 'fred-observations-v1',
    PRIMARY KEY (capture_id, observation_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS observation_revision_current_idx
    ON silver_fred.observation_revision (
        domain, series_id, observation_date, realtime_start DESC
    );

ALTER TABLE silver_fred.fact_economic_indicators
    ADD COLUMN IF NOT EXISTS source_value TEXT,
    ADD COLUMN IF NOT EXISTS value_status TEXT NOT NULL DEFAULT 'valid',
    ADD COLUMN IF NOT EXISTS realtime_start DATE,
    ADD COLUMN IF NOT EXISTS realtime_end DATE,
    ADD COLUMN IF NOT EXISTS capture_id UUID
        REFERENCES raw_capture.response_capture(capture_id);

ALTER TABLE silver_fred.fact_economic_indicators
    DROP CONSTRAINT IF EXISTS fact_economic_indicators_value_status_check;
ALTER TABLE silver_fred.fact_economic_indicators
    ADD CONSTRAINT fact_economic_indicators_value_status_check
    CHECK (value_status IN ('valid', 'missing', 'invalid'));
