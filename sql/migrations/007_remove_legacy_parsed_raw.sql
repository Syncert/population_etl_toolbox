-- Final capture-first cutover: parsed observations are silver revisions, never raw.
-- Safe for fresh installs (relations do not exist) and beta reset/replay cutovers.

DROP TABLE IF EXISTS raw_census.acs_long;
DROP TABLE IF EXISTS raw_bls.bls_long;
DROP TABLE IF EXISTS raw_fred.fred_long;
