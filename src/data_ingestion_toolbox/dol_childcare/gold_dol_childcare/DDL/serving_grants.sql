-- The API reads gold only, as `api_reader`. `sql/bootstrap/001_api_readonly.sql`
-- grants it on a fresh install, but a warehouse bootstrapped before this
-- source existed never re-applies that file, so the source's own DDL grants
-- what the API reads: usage of the gold schema, SELECT on its relations, and
-- SELECT on the relations the schema's owner creates in it later. The gold
-- views read silver as their owner, so the API needs no silver grant.
-- Skipped where the role does not exist (a test database). Rerun-safe.
DO $$
DECLARE
    _owner TEXT := (
        SELECT pg_catalog.pg_get_userbyid(datdba)
          FROM pg_catalog.pg_database
         WHERE datname = current_database()
    );
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
        GRANT USAGE ON SCHEMA gold_dol_childcare TO api_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA gold_dol_childcare TO api_reader;
        -- Only the owner (or a member of it) may record its defaults; a
        -- connection that is neither still grants every relation above, and
        -- this file is re-applied whenever the source's DDL changes.
        BEGIN
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA gold_dol_childcare GRANT SELECT ON TABLES TO api_reader',
                _owner
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'default privileges for % in gold_dol_childcare not recorded: %', _owner, SQLERRM;
        END;
    END IF;
END $$;
