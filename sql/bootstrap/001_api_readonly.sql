-- The public analytical serving role: what the API and Martin may read.
--
-- ETL continues to connect as the warehouse owner; `api_reader` receives
-- SELECT on the gold schemas and nothing anywhere else. This file is the one
-- statement of that policy -- `scripts/provision_api_readonly.py` applies it
-- rather than repeating it, because two copies of a privilege policy drift and
-- the copy that drifts is the one nobody reads.
--
-- Nothing here names a database or an owning role. Both are derived, for the
-- reason `002_app_api.sql` gives beside its own `current_database()`: one
-- reviewed bootstrap has to serve the Compose stack, an externally hosted
-- deployment whose owner is `airflow_admin` and whose database is named per
-- deployment, and the disposable test database. A literal `population_etl` or
-- `analytics` here is a file that cannot be applied to two of those three, and
-- that is what made the second copy necessary.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
        -- A login role with a placeholder secret. Deployments set a real one
        -- through `scripts/provision_api_readonly.py`, which owns password
        -- handling; this exists so the Compose stack and the disposable test
        -- database come up without one.
        CREATE ROLE api_reader LOGIN PASSWORD 'api_reader';
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO api_reader', current_database()
    );
END
$$;

-- Belt and braces beside the grants below: even if a future schema is added to
-- the list and a write privilege is granted by mistake, this role's sessions
-- cannot write.
ALTER ROLE api_reader SET default_transaction_read_only = on;

-- `gold` is created here because the contract views live in it and the role
-- must be able to see the schema before anything is granted in it. It is owned
-- by whoever owns this database -- the ETL identity, whatever it is called in
-- this deployment.
DO $$
DECLARE
    _owner TEXT := (
        SELECT pg_catalog.pg_get_userbyid(datdba)
          FROM pg_catalog.pg_database
         WHERE datname = current_database()
    );
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION %I', _owner);
END
$$;

-- The schema list is the policy. Everything the API and Martin may read is
-- named here and nowhere else; a relation outside these schemas is one the
-- serving role cannot reach, which is what
-- `tests/integration/database/test_api_reader_privileges.py` asserts in both
-- directions.
DO $$
DECLARE
    _schema TEXT;
    _owner TEXT := (
        SELECT pg_catalog.pg_get_userbyid(datdba)
          FROM pg_catalog.pg_database
         WHERE datname = current_database()
    );
BEGIN
    FOREACH _schema IN ARRAY ARRAY[
        'gold', 'gold_glossary', 'gold_bls', 'gold_cdc', 'gold_census',
        'gold_fbi', 'gold_fred', 'gold_nass', 'gold_pep'
    ]
    LOOP
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = _schema) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO api_reader', _schema);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO api_reader', _schema);
            EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO api_reader', _schema);
            -- Default privileges are recorded per granting role, so they must
            -- name the identity that will create the relations. That is this
            -- database's owner, derived rather than assumed: recorded against
            -- the wrong role they apply to nothing, silently, and the next
            -- relation a transform creates is unreadable by the API.
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO api_reader',
                _owner, _schema
            );
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON SEQUENCES TO api_reader',
                _owner, _schema
            );
        END IF;
    END LOOP;
END
$$;
