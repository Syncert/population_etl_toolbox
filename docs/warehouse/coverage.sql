-- What the live warehouse actually holds, per source, grain and year.
--
-- Read-only. Run it with psql against the warehouse; it creates one TEMP
-- table, fills it, prints it, and leaves nothing behind. It answers the
-- "believed loaded" column of docs/warehouse/data-sources.md.
--
--   psql "$WAREHOUSE_DSN" -f docs/warehouse/coverage.sql > coverage.txt
--
-- ACS is read from its ingestion ledger (control.acs_ingestion_slices), not
-- from gold_census.rpt_acs_observations: the serving table is hundreds of GB
-- and a GROUP BY over it is a full scan. Every other source is read from its
-- own latest-serving relation, whose geography and year columns are found at
-- run time, so a column rename does not break the script.

SET statement_timeout = '15min';

CREATE TEMP TABLE coverage (
    source       TEXT,
    relation     TEXT,
    grain        TEXT,
    first_year   INTEGER,
    last_year    INTEGER,
    years        INTEGER,
    row_count    BIGINT
);

-- ACS and the 2000/1990 decennial censuses, from the ingestion ledger.
INSERT INTO coverage
SELECT 'CENSUS_ACS:' || dataset, 'control.acs_ingestion_slices', geo_level,
       MIN(year), MAX(year), COUNT(DISTINCT year), SUM(rows_loaded)
  FROM control.acs_ingestion_slices
 WHERE status IN ('success', 'empty')
 GROUP BY dataset, geo_level;

DO $$
DECLARE
    rel        RECORD;
    geo_col    TEXT;
    year_expr  TEXT;
    year_col   TEXT;
    year_type  TEXT;
BEGIN
    FOR rel IN
        SELECT * FROM (VALUES
            ('CENSUS_PEP',         'gold_pep',           'rpt_pep_observations'),
            ('BLS',                'gold_bls',           'rpt_bls_observations'),
            ('FRED',               'gold_fred',          'rpt_fred_observations'),
            ('BEA',                'gold_bea',           'observation_latest'),
            ('BLS_QCEW',           'gold_bls_qcew',      'observation_latest'),
            ('CDC',                'gold_cdc',           'latest_release_observation'),
            ('CENSUS_BPS',         'gold_census_bps',    'observation_latest'),
            ('CENSUS_CBP',         'gold_census_cbp',    'observation_latest'),
            ('CENSUS_LODES',       'gold_census_lodes',  'observation_latest'),
            ('CENSUS_SAIPE_SAHIE', 'gold_census_sae',    'estimate_latest'),
            ('EIA',                'gold_eia',           'observation_latest'),
            ('EPA_AQS',            'gold_epa_aqs',       'observation_latest'),
            ('FBI_UCR',            'gold_fbi',           'latest_release_observation'),
            ('FBI_UCR (county rollup)', 'gold_fbi',      'latest_county_rollup'),
            ('FCC_BDC',            'gold_fcc_bdc',       'observation_latest'),
            ('FEMA_NRI',           'gold_fema_nri',      'observation_latest'),
            ('FHFA_HPI',           'gold_fhfa_hpi',      'observation_latest'),
            ('HUD_FMR_IL',         'gold_hud_fmr_il',    'observation_latest'),
            ('IRS_MIGRATION',      'gold_irs_migration', 'total_observation_latest'),
            ('NCES_CCD',           'gold_nces_ccd',      'observation_latest'),
            ('NOAA_NORMALS',       'gold_noaa_normals',  'observation_latest'),
            ('USDA_ERS',           'gold_usda_ers',      'observation_latest'),
            ('USDA_NASS',          'gold_nass',          'latest_release_observation')
        ) AS t(source, schema_name, relation_name)
    LOOP
        IF to_regclass(format('%I.%I', rel.schema_name, rel.relation_name)) IS NULL THEN
            INSERT INTO coverage VALUES (rel.source,
                rel.schema_name || '.' || rel.relation_name || ' (absent)', NULL, NULL, NULL, 0, 0);
            CONTINUE;
        END IF;

        SELECT column_name INTO geo_col
          FROM information_schema.columns
         WHERE table_schema = rel.schema_name AND table_name = rel.relation_name
           AND column_name IN ('geo_level', 'geo_grain', 'geo_type', 'agg_level_desc', 'subject_type')
         ORDER BY array_position(ARRAY['geo_level', 'geo_grain', 'geo_type', 'agg_level_desc', 'subject_type'], column_name::TEXT)
         LIMIT 1;

        SELECT column_name, data_type INTO year_col, year_type
          FROM information_schema.columns
         WHERE table_schema = rel.schema_name AND table_name = rel.relation_name
           AND column_name IN ('year', 'observation_year', 'estimate_year', 'observation_date',
                               'period_start', 'period_year', 'fiscal_year', 'vintage_year')
         -- The year observed, never the vintage that published it.
         ORDER BY array_position(ARRAY['year', 'observation_year', 'estimate_year', 'observation_date',
                                       'period_start', 'period_year', 'fiscal_year', 'vintage_year'],
                                 column_name::TEXT)
         LIMIT 1;

        IF year_col IS NULL THEN
            year_expr := 'NULL::INTEGER';
        ELSIF year_type IN ('date', 'timestamp with time zone', 'timestamp without time zone') THEN
            year_expr := format('EXTRACT(YEAR FROM %I)::INTEGER', year_col);
        ELSE
            year_expr := format('NULLIF(LEFT(%I::TEXT, 4), '''')::INTEGER', year_col);
        END IF;

        EXECUTE format(
            'INSERT INTO coverage
             SELECT %L, %L, %s, MIN(y), MAX(y), COUNT(DISTINCT y), COUNT(*)
               FROM (SELECT %s AS g, %s AS y FROM %I.%I) AS s
              GROUP BY g',
            rel.source, rel.schema_name || '.' || rel.relation_name,
            CASE WHEN geo_col IS NULL THEN 'NULL::TEXT' ELSE 'g::TEXT' END,
            COALESCE(quote_ident(geo_col), 'NULL::TEXT'), year_expr,
            rel.schema_name, rel.relation_name);
    END LOOP;
END $$;

-- The reference geography: which boundary vintages are loaded per grain.
INSERT INTO coverage
SELECT 'CENSUS_GEO (boundaries)', 'silver_ref.dim_geo_geometry_version', entity.geo_type,
       MIN(geometry.boundary_vintage), MAX(geometry.boundary_vintage),
       COUNT(DISTINCT geometry.boundary_vintage), COUNT(*)
  FROM silver_ref.dim_geo_geometry_version AS geometry
  JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_sk = geometry.geo_sk
 GROUP BY entity.geo_type;

SELECT * FROM coverage ORDER BY source, grain;

-- Metrics the catalog serves, per source, with the grains it says each serves.
SELECT source_code,
       COUNT(*) AS metrics,
       ARRAY(SELECT DISTINCT unnest(ARRAY_AGG(g)) ORDER BY 1) AS grains,
       MAX(source_watermark) AS newest_watermark
  FROM gold_glossary.dim_metric_catalog
  LEFT JOIN LATERAL unnest(valid_geo_grains) AS g ON TRUE
 GROUP BY source_code
 ORDER BY source_code;
