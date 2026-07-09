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
GRANT USAGE ON SCHEMA gold TO api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO api_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA gold TO api_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA gold
    GRANT SELECT ON TABLES TO api_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA gold
    GRANT SELECT ON SEQUENCES TO api_reader;
