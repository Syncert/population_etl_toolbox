-- API-owned application storage: accounts and their credentials (ADR-0005),
-- saved analysis configurations (ADR-0003), and evidence packets (ADR-0004).
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

-- One row per account (ADR-0003, extended by ADR-0005). An account is created
-- either by an operator (`provision_app_api.py --issue-token`) or by a visitor
-- completing the OIDC authorization-code flow; the two differ only in which
-- columns are populated, never in how they are authorized afterwards.
--
-- `display_label` is an OPERATOR label and is never rendered to another
-- account. The stranger-visible name is `public_display_name`, which stays
-- NULL until the account first publishes something and is chosen at that
-- moment -- reusing the operator label as a public name would publish text
-- written on the assumption nobody outside the deployment would read it
-- (ADR-0005 §3).
--
-- `(issuer, subject)` is the provider's stable identifier for a person and is
-- the ONLY thing sign-in matches on. `email` is contact information, never a
-- key: accounts are never linked or merged on a matching address, because a
-- provider asserting an address it never verified would otherwise take over
-- the account that owns it.
CREATE TABLE IF NOT EXISTS app_api.user_account (
    user_account_id     BIGSERIAL PRIMARY KEY,
    display_label       TEXT NOT NULL,
    issuer              TEXT,
    subject             TEXT,
    email               TEXT,
    public_display_name TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at          TIMESTAMPTZ,
    blocked_at          TIMESTAMPTZ
);

-- The migration half, for a database bootstrapped before ADR-0005. Every
-- column is nullable, so an operator account created by the previous shape is
-- valid under this one without being touched.
ALTER TABLE app_api.user_account
    ADD COLUMN IF NOT EXISTS issuer              TEXT,
    ADD COLUMN IF NOT EXISTS subject             TEXT,
    ADD COLUMN IF NOT EXISTS email               TEXT,
    ADD COLUMN IF NOT EXISTS public_display_name TEXT,
    ADD COLUMN IF NOT EXISTS blocked_at          TIMESTAMPTZ;

-- One identity per account. Partial, because NULL `issuer` is the normal
-- state of an operator account and a plain UNIQUE would be satisfied by any
-- number of them -- which is correct, but only by accident of how Postgres
-- compares NULLs. Saying `WHERE issuer IS NOT NULL` makes it deliberate.
CREATE UNIQUE INDEX IF NOT EXISTS user_account_identity_idx
    ON app_api.user_account (issuer, subject)
    WHERE issuer IS NOT NULL AND subject IS NOT NULL;

-- Public names are unique case-insensitively, so one account cannot dress as
-- another (ADR-0005 §3). Released for reuse when changed, which is why this is
-- an index on the live value rather than a history table.
CREATE UNIQUE INDEX IF NOT EXISTS user_account_public_name_idx
    ON app_api.user_account (LOWER(public_display_name))
    WHERE public_display_name IS NOT NULL;

-- One row per credential (ADR-0005 §2). Splitting credentials out of
-- `user_account` is what lets one account hold several at once: the operator
-- token it may have been created with, plus one access/refresh pair per
-- browser it is signed in from.
--
-- Only the digest is ever stored, for every kind, which is ADR-0003's rule
-- unchanged: a database or backup leak yields nothing presentable.
--
-- `session_family` is shared by every token descended from one sign-in. It is
-- what lets reuse detection revoke a compromised session without signing the
-- reader out of their other devices, and it is NULL for an operator token,
-- which belongs to no session and never rotates.
CREATE TABLE IF NOT EXISTS app_api.account_credential (
    credential_id   BIGSERIAL PRIMARY KEY,
    user_account_id BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('operator', 'access', 'refresh')),
    session_family  UUID,
    token_sha256    TEXT NOT NULL UNIQUE,
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ
);

-- The lookup every authenticated request makes. Partial on the live rows,
-- because a revoked credential is never a match and there is no reason for the
-- index to carry the accumulating history of them.
CREATE INDEX IF NOT EXISTS account_credential_live_idx
    ON app_api.account_credential (token_sha256)
    WHERE revoked_at IS NULL;

-- "Sign out everywhere" and reuse detection both revoke by family, and
-- deletion cascades by account.
CREATE INDEX IF NOT EXISTS account_credential_family_idx
    ON app_api.account_credential (session_family)
    WHERE session_family IS NOT NULL;

CREATE INDEX IF NOT EXISTS account_credential_account_idx
    ON app_api.account_credential (user_account_id);

-- ADR-0005 §6: existing operator tokens keep working, with no expiry and no
-- forced migration. Each becomes one `kind = 'operator'` credential carrying
-- the digest, `created_at` and `revoked_at` it already had -- the digest is
-- copied rather than regenerated, so the tokens in circulation are the same
-- tokens and nobody has to be told to fetch a new one.
--
-- The column is then dropped, deliberately. Leaving it in place would give a
-- live digest two homes, and the failure that produces is the worst kind: a
-- credential revoked in one place and still honoured from the other. The copy
-- and the drop are one statement pair inside one transaction, so a database
-- that has the column always still has its rows.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'app_api'
          AND table_name = 'user_account'
          AND column_name = 'token_sha256'
    ) THEN
        INSERT INTO app_api.account_credential (
            user_account_id, kind, token_sha256, issued_at, revoked_at
        )
        SELECT user_account_id, 'operator', token_sha256, created_at, revoked_at
        FROM app_api.user_account
        WHERE token_sha256 IS NOT NULL
        ON CONFLICT (token_sha256) DO NOTHING;

        ALTER TABLE app_api.user_account DROP COLUMN token_sha256;
    END IF;
END
$$;

-- One row per sign-in that has started and not yet come back (ADR-0005 s1).
--
-- This table is what "a `state` parameter bound to the caller's session"
-- means when the caller has no session yet, which is every sign-in. The
-- browser holds one value -- an opaque handle, in a short-lived `HttpOnly`
-- cookie -- and this row holds everything that handle unlocks. Without it,
-- `state` would be a value the caller both supplies and is checked against,
-- which checks nothing.
--
-- The handle is stored as a digest, like every other credential in this
-- schema. `state` is too: it is only ever compared, never re-sent, so there
-- is no reason to keep a readable copy.
--
-- `nonce` and `code_verifier` are readable, because both have to leave here
-- intact -- the verifier goes to the token endpoint and the nonce is compared
-- against a claim the provider echoed. They are single-use values that expire
-- in minutes and grant nothing to a holder who does not also have the
-- authorization code, and the row is deleted the moment it is spent.
CREATE TABLE IF NOT EXISTS app_api.sign_in_transaction (
    transaction_id  BIGSERIAL PRIMARY KEY,
    handle_sha256   TEXT NOT NULL UNIQUE,
    state_sha256    TEXT NOT NULL,
    nonce           TEXT NOT NULL,
    code_verifier   TEXT NOT NULL,
    redirect_uri    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL
);

-- Abandoned sign-ins are the common case: somebody clicks sign in and closes
-- the tab. They are swept on the next start rather than by a scheduled job,
-- so a deployment with no scheduler does not accumulate them.
CREATE INDEX IF NOT EXISTS sign_in_transaction_expiry_idx
    ON app_api.sign_in_transaction (expires_at);

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

-- One row per evidence packet (ADR-0004): an ordered composition of blocks,
-- stored as one JSONB document so block order and the packet's optimistic
-- version move together. Analytical blocks embed their own query rather than
-- referencing a saved configuration, so editing a configuration later can
-- never silently rewrite what an issued packet argued. No observation value
-- is ever stored here; a block's query is replayed live.
CREATE TABLE IF NOT EXISTS app_api.evidence_packet (
    packet_id         BIGSERIAL PRIMARY KEY,
    owner_user_id     BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    document          JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_user_id, name)
);

CREATE INDEX IF NOT EXISTS evidence_packet_owner_idx
    ON app_api.evidence_packet (owner_user_id, packet_id);

-- The grants below are positional: they cover the tables that exist when they
-- run. Re-running this whole file against a deployed database is therefore
-- the migration for any table added above; every statement is idempotent.
GRANT USAGE ON SCHEMA app_api TO api_app_writer;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA app_api TO api_app_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app_api TO api_app_writer;
