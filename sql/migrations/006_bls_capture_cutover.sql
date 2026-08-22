-- ARCH-006 beta cutover: complete BLS responses are captured before silver parsing.
CREATE SCHEMA IF NOT EXISTS silver_bls;

CREATE TABLE IF NOT EXISTS silver_bls.observation_revision (
    capture_id          UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    observation_index   INTEGER NOT NULL CHECK (observation_index >= 0),
    program             TEXT NOT NULL,
    series_id           TEXT NOT NULL,
    year_source         TEXT NOT NULL,
    period_source       TEXT NOT NULL,
    period_name_source  TEXT,
    value_source        TEXT,
    latest_source       TEXT,
    footnotes_source    TEXT,
    year                INTEGER NOT NULL,
    period              TEXT NOT NULL,
    period_name         TEXT,
    value               NUMERIC,
    value_status        TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'invalid')),
    is_latest           BOOLEAN NOT NULL DEFAULT FALSE,
    parsed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version      TEXT NOT NULL DEFAULT 'bls-timeseries-v1',
    PRIMARY KEY (capture_id, observation_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS bls_observation_revision_current_idx
    ON silver_bls.observation_revision (program, series_id, year, period, is_latest DESC);
