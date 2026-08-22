-- ARCH-004 beta foundation.
-- This is a reproducible fresh-bootstrap DDL, not a production compatibility migration.
-- Existing prototype raw_* schemas are intentionally left alone until source cutover.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS raw_capture;

CREATE TABLE IF NOT EXISTS control.schema_migration_state (
    component_name TEXT PRIMARY KEY,
    ddl_hash       TEXT NOT NULL,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS control.serving_refresh_state (
    source_code                 TEXT PRIMARY KEY
        CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    last_silver_ingested_at     TIMESTAMPTZ NOT NULL DEFAULT '-infinity'::TIMESTAMPTZ,
    last_refresh_started_at     TIMESTAMPTZ,
    last_refresh_completed_at   TIMESTAMPTZ,
    last_window_start           DATE,
    last_window_end             DATE,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS control.serving_refresh_chunk_state (
    source_code                         TEXT NOT NULL
        CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    chunk_start                         DATE NOT NULL,
    chunk_end                           DATE NOT NULL,
    target_silver_ingested_at           TIMESTAMPTZ NOT NULL,
    completed_silver_ingested_at        TIMESTAMPTZ,
    status                              TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'FAILED')),
    attempt_count                       INTEGER NOT NULL DEFAULT 0,
    last_refresh_started_at             TIMESTAMPTZ,
    last_refresh_completed_at           TIMESTAMPTZ,
    last_error                          TEXT,
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_code, chunk_start, chunk_end),
    CHECK (chunk_end >= chunk_start)
);

CREATE INDEX IF NOT EXISTS serving_refresh_chunk_state_status_idx
    ON control.serving_refresh_chunk_state (source_code, status, chunk_start);

CREATE TABLE IF NOT EXISTS control.acs_ingestion_slices (
    id                     BIGSERIAL PRIMARY KEY,
    dataset                TEXT NOT NULL CHECK (dataset IN ('acs1', 'acs5')),
    year                   INTEGER NOT NULL CHECK (
        year BETWEEN 2000 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1
    ),
    geo_level              TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county')),
    state_fips             TEXT,
    variables_hash         TEXT,
    variables_count        INTEGER,
    status                 TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'success', 'empty', 'failed')
    ),
    rows_loaded            BIGINT NOT NULL DEFAULT 0 CHECK (rows_loaded >= 0),
    started_at             TIMESTAMPTZ,
    finished_at            TIMESTAMPTZ,
    variables_hash_seen_at TIMESTAMPTZ,
    last_error             TEXT,
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at)),
    CHECK (
        (geo_level IN ('us', 'state') AND state_fips IS NULL)
        OR (geo_level = 'county' AND state_fips ~ '^[0-9]{2}$')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS acs_ingestion_slices_uniq_nostate
    ON control.acs_ingestion_slices (dataset, year, geo_level)
    WHERE state_fips IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS acs_ingestion_slices_uniq_state
    ON control.acs_ingestion_slices (dataset, year, geo_level, state_fips)
    WHERE state_fips IS NOT NULL;
CREATE INDEX IF NOT EXISTS acs_ingestion_slices_status_idx
    ON control.acs_ingestion_slices (status);
CREATE INDEX IF NOT EXISTS acs_ingestion_slices_hash_idx
    ON control.acs_ingestion_slices (variables_hash);

CREATE TABLE IF NOT EXISTS control.bls_ingestion_slices (
    id                  BIGSERIAL PRIMARY KEY,
    program             TEXT NOT NULL,
    year_start          INTEGER NOT NULL,
    year_end            INTEGER NOT NULL,
    geo_level           TEXT,
    state_fips          TEXT,
    series_hash         TEXT,
    series_count        INTEGER,
    status              TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'success', 'empty', 'failed')
    ),
    rows_loaded         BIGINT NOT NULL DEFAULT 0 CHECK (rows_loaded >= 0),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    series_hash_seen_at TIMESTAMPTZ,
    last_error          TEXT,
    CHECK (
        year_start >= 1900 AND year_end >= year_start
        AND year_end <= EXTRACT(YEAR FROM CURRENT_DATE) + 1
    ),
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at))
);

CREATE UNIQUE INDEX IF NOT EXISTS bls_ingestion_slices_uniq
    ON control.bls_ingestion_slices (
        program, year_start, year_end, COALESCE(geo_level, ''), COALESCE(state_fips, '')
    );
CREATE INDEX IF NOT EXISTS bls_ingestion_slices_status_idx
    ON control.bls_ingestion_slices (status);
CREATE INDEX IF NOT EXISTS bls_ingestion_slices_hash_idx
    ON control.bls_ingestion_slices (series_hash);
CREATE INDEX IF NOT EXISTS bls_ingestion_slices_geo_idx
    ON control.bls_ingestion_slices (geo_level);
CREATE INDEX IF NOT EXISTS bls_ingestion_slices_state_idx
    ON control.bls_ingestion_slices (state_fips);

CREATE TABLE IF NOT EXISTS control.fred_ingestion_slices (
    id                  BIGSERIAL PRIMARY KEY,
    domain              TEXT NOT NULL,
    date_start          DATE NOT NULL,
    date_end            DATE NOT NULL CHECK (date_end >= date_start),
    series_hash         TEXT,
    series_count        INTEGER,
    status              TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'success', 'empty', 'failed')
    ),
    rows_loaded         BIGINT NOT NULL DEFAULT 0 CHECK (rows_loaded >= 0),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    series_hash_seen_at TIMESTAMPTZ,
    last_error          TEXT,
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at)),
    UNIQUE (domain, date_start, date_end)
);

CREATE INDEX IF NOT EXISTS fred_ingestion_slices_status_idx
    ON control.fred_ingestion_slices (status);
CREATE INDEX IF NOT EXISTS fred_ingestion_slices_hash_idx
    ON control.fred_ingestion_slices (series_hash);

CREATE TABLE IF NOT EXISTS control.ingestion_run (
    run_id           UUID PRIMARY KEY,
    source_code      TEXT NOT NULL CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    status           TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'success', 'partial', 'failed', 'cancelled')
    ),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    source_watermark JSONB,
    error_summary    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at)),
    UNIQUE (run_id, source_code)
);

CREATE TABLE IF NOT EXISTS control.ingestion_request (
    request_id          UUID PRIMARY KEY,
    run_id              UUID NOT NULL,
    source_code         TEXT NOT NULL CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    endpoint            TEXT NOT NULL CHECK (BTRIM(endpoint) <> ''),
    request_parameters  JSONB NOT NULL DEFAULT '{}'::JSONB,
    request_fingerprint TEXT NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    status              TEXT NOT NULL CHECK (
        status IN ('planned', 'running', 'captured', 'empty', 'quarantined', 'failed')
    ),
    attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts        INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    next_retry_at       TIMESTAMPTZ,
    source_watermark    JSONB,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (attempt_count <= max_attempts),
    CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at)),
    FOREIGN KEY (run_id, source_code)
        REFERENCES control.ingestion_run(run_id, source_code),
    UNIQUE (request_id, run_id, source_code, endpoint, request_fingerprint)
);

CREATE TABLE IF NOT EXISTS raw_capture.payload_blob (
    payload_checksum   TEXT PRIMARY KEY CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    checksum_algorithm TEXT NOT NULL DEFAULT 'sha256'
        CHECK (checksum_algorithm = 'sha256'),
    payload            BYTEA NOT NULL,
    payload_size       BIGINT NOT NULL CHECK (payload_size = OCTET_LENGTH(payload)),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_capture.response_capture (
    capture_id             UUID PRIMARY KEY,
    request_id             UUID NOT NULL,
    run_id                 UUID NOT NULL,
    source_code            TEXT NOT NULL CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    endpoint               TEXT NOT NULL CHECK (BTRIM(endpoint) <> ''),
    request_parameters     JSONB NOT NULL DEFAULT '{}'::JSONB,
    request_fingerprint    TEXT NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    retrieved_at           TIMESTAMPTZ NOT NULL,
    http_status            SMALLINT NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    response_headers       JSONB NOT NULL DEFAULT '{}'::JSONB,
    media_type             TEXT NOT NULL CHECK (BTRIM(media_type) <> ''),
    payload_schema_version TEXT,
    source_revision        TEXT,
    payload_checksum       TEXT NOT NULL
        REFERENCES raw_capture.payload_blob(payload_checksum),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (request_id, run_id, source_code, endpoint, request_fingerprint)
        REFERENCES control.ingestion_request(
            request_id, run_id, source_code, endpoint, request_fingerprint
        ),
    UNIQUE (capture_id, run_id, source_code)
);

CREATE TABLE IF NOT EXISTS control.capture_quarantine (
    quarantine_id    UUID PRIMARY KEY,
    capture_id       UUID NOT NULL,
    run_id           UUID NOT NULL,
    source_code      TEXT NOT NULL CHECK (source_code ~ '^[A-Z0-9][A-Z0-9_-]*$'),
    parser_version   TEXT NOT NULL CHECK (BTRIM(parser_version) <> ''),
    error_code       TEXT NOT NULL CHECK (BTRIM(error_code) <> ''),
    error_summary    TEXT NOT NULL CHECK (BTRIM(error_summary) <> ''),
    status           TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'replaying', 'resolved', 'ignored')
    ),
    replay_attempts  INTEGER NOT NULL DEFAULT 0 CHECK (replay_attempts >= 0),
    last_replayed_at TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, parser_version, error_code),
    CHECK (status <> 'resolved' OR resolved_at IS NOT NULL),
    FOREIGN KEY (capture_id, run_id, source_code)
        REFERENCES raw_capture.response_capture(capture_id, run_id, source_code)
);

CREATE INDEX IF NOT EXISTS ingestion_run_source_status_idx
    ON control.ingestion_run (source_code, status, created_at DESC);
CREATE INDEX IF NOT EXISTS ingestion_request_run_status_idx
    ON control.ingestion_request (run_id, status);
CREATE INDEX IF NOT EXISTS ingestion_request_retry_idx
    ON control.ingestion_request (next_retry_at)
    WHERE status = 'failed' AND next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS response_capture_request_identity_idx
    ON raw_capture.response_capture (
        source_code, request_fingerprint, payload_checksum, retrieved_at DESC
    );
CREATE INDEX IF NOT EXISTS response_capture_run_idx
    ON raw_capture.response_capture (run_id, retrieved_at);
CREATE INDEX IF NOT EXISTS capture_quarantine_status_idx
    ON control.capture_quarantine (source_code, status, created_at);

CREATE OR REPLACE FUNCTION raw_capture.reject_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'raw capture relations are append-only'
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS payload_blob_reject_mutation
    ON raw_capture.payload_blob;
CREATE TRIGGER payload_blob_reject_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON raw_capture.payload_blob
    FOR EACH STATEMENT EXECUTE FUNCTION raw_capture.reject_mutation();

DROP TRIGGER IF EXISTS response_capture_reject_mutation
    ON raw_capture.response_capture;
CREATE TRIGGER response_capture_reject_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON raw_capture.response_capture
    FOR EACH STATEMENT EXECUTE FUNCTION raw_capture.reject_mutation();

COMMENT ON SCHEMA raw_capture IS
    'Append-only provider response payloads and retrieval envelopes.';
COMMENT ON SCHEMA control IS
    'Mutable ingestion execution, retry, watermark, and quarantine state.';
COMMENT ON COLUMN raw_capture.response_capture.request_parameters IS
    'Sanitized request parameters only; never store credentials.';
COMMENT ON COLUMN raw_capture.response_capture.response_headers IS
    'Allowlisted response headers only; never store authorization or cookies.';
