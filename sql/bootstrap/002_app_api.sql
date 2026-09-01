-- API-owned application storage for saved analysis configurations (ADR-0003).
--
-- This schema is NOT warehouse content: it holds user-owned application data,
-- it is absent from the warehouse manifest, and no ETL process reads or writes
-- it. It exists so user-scoped persistence never requires granting the public
-- serving role any mutation right -- `api_reader` stays read-only over the gold
-- schemas and receives nothing here.

CREATE SCHEMA IF NOT EXISTS app_api;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_app_writer') THEN
        CREATE ROLE api_app_writer LOGIN PASSWORD 'api_app_writer';
    END IF;
END
$$;

-- Granted against whichever database this file is applied to, so one reviewed
-- bootstrap serves the Compose stack, an externally hosted deployment, and the
-- disposable test database without a hardcoded name drifting between them.
DO $$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO api_app_writer', current_database()
    );
END
$$;

-- One account per issued credential. The token itself is never stored: only
-- its SHA-256 digest, so a database or backup leak yields nothing presentable.
CREATE TABLE IF NOT EXISTS app_api.user_account (
    user_account_id   BIGSERIAL PRIMARY KEY,
    display_label     TEXT NOT NULL,
    token_sha256      TEXT NOT NULL UNIQUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS user_account_active_token_idx
    ON app_api.user_account (token_sha256)
    WHERE revoked_at IS NULL;

-- One row per saved configuration. `document` is the user's own analysis
-- intent (query, filters, visualization), stored verbatim; the API validates
-- it against the live capability and compatibility contracts on write but
-- never rewrites it. `version` supports optimistic concurrency: an update
-- states the version it read, and a mismatch is refused rather than silently
-- overwriting a concurrent edit.
CREATE TABLE IF NOT EXISTS app_api.saved_analysis_configuration (
    configuration_id  BIGSERIAL PRIMARY KEY,
    owner_user_id     BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    document          JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_user_id, name)
);

CREATE INDEX IF NOT EXISTS saved_analysis_owner_idx
    ON app_api.saved_analysis_configuration (owner_user_id, configuration_id);

GRANT USAGE ON SCHEMA app_api TO api_app_writer;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA app_api TO api_app_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app_api TO api_app_writer;
