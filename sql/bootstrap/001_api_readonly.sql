-- Internal Compose bootstrap for the public analytical serving role.
-- ETL continues to use the analytics owner; API and Martin receive SELECT only.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
        CREATE ROLE api_reader LOGIN PASSWORD 'api_reader';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE population_etl TO api_reader;
ALTER ROLE api_reader SET default_transaction_read_only = on;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION analytics;

DO $$
DECLARE
    _schema TEXT;
BEGIN
    FOREACH _schema IN ARRAY ARRAY['gold', 'gold_glossary', 'gold_bls', 'gold_census', 'gold_fred']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = _schema) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO api_reader', _schema);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO api_reader', _schema);
            EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO api_reader', _schema);
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA %I GRANT SELECT ON TABLES TO api_reader',
                _schema
            );
            EXECUTE format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA %I GRANT SELECT ON SEQUENCES TO api_reader',
                _schema
            );
        END IF;
    END LOOP;
END
$$;
