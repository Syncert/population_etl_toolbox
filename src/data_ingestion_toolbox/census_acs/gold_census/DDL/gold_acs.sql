-- census_acs/gold_census/DDL/gold_acs.sql
-- REFACTORED: Source-First Architecture
-- Subject-scoped gold DDL for ACS objects — no unified wide table.
-- Per-source serving table with ACS-specific columns only.

CREATE SCHEMA IF NOT EXISTS gold_census;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Shared glossary objects are owned by the ordered warehouse bootstrap.
-- This source component only owns relations in gold_census.

CREATE TABLE IF NOT EXISTS gold_census.dim_acs_table (
    acs_table_sk      BIGSERIAL PRIMARY KEY,
    dataset_code      TEXT NOT NULL CONSTRAINT dim_acs_table_dataset_code_census_check
        CHECK (dataset_code IN ('acs1', 'acs5', 'dec2000sf1', 'dec2000sf3',
                                'dec1990stf1', 'dec1990stf3')),
    vintage_year      INTEGER NOT NULL,
    table_id          TEXT NOT NULL,
    table_title       TEXT,
    concept           TEXT,
    universe          TEXT,
    -- 0 is a census: one day's count, pooling no survey years.
    survey_span_years INTEGER NOT NULL
        CONSTRAINT dim_acs_table_survey_span_years_census_check
        CHECK (survey_span_years IN (0, 1, 5)),
    reference_url     TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, table_id)
);

CREATE TABLE IF NOT EXISTS gold_census.dim_acs_variable (
    acs_variable_sk        BIGSERIAL PRIMARY KEY,
    acs_table_sk           BIGINT NOT NULL REFERENCES gold_census.dim_acs_table(acs_table_sk),
    dataset_code           TEXT NOT NULL CONSTRAINT dim_acs_variable_dataset_code_census_check
        CHECK (dataset_code IN ('acs1', 'acs5', 'dec2000sf1', 'dec2000sf3',
                                'dec1990stf1', 'dec1990stf3')),
    vintage_year           INTEGER NOT NULL,
    variable_code          TEXT NOT NULL,
    variable_label         TEXT,
    concept                TEXT,
    universe               TEXT,
    value_role             TEXT NOT NULL CHECK (value_role IN ('ESTIMATE', 'MOE', 'ANNOTATION')),
    denominator_hint       TEXT,
    is_publishable_default BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, variable_code)
);

-- ============================================================
-- ACS FACT VIEW (source of truth)
-- ============================================================

CREATE OR REPLACE VIEW gold_census.fact_acs_observation AS
SELECT
    s.geo_id,
    -- The vocabulary through `gold_glossary.geo_grain` (migration 018, moved
    -- ahead of this phase by 021); the identity-shape inference below is a
    -- different rule and stays. The final ELSE keeps the downstream
    -- `geo_level TEXT NOT NULL` satisfiable (DB-037).
    CASE
        WHEN COALESCE(TRIM(s.geo_level), '') <> ''
            THEN gold_glossary.geo_grain(s.geo_level)
        WHEN s.geo_id = 'us:1'             THEN 'NATIONAL'
        WHEN s.geo_id LIKE 'state:%|county:%' THEN 'COUNTY'
        WHEN s.geo_id LIKE 'state:%'       THEN 'STATE'
        ELSE 'NATIONAL'
    END AS geo_level,
    s.time_sk,
    MAKE_DATE(s.estimate_year, 1, 1) AS observation_date,
    s.duration_start,
    s.duration_end,
    av.acs_table_sk,
    av.acs_variable_sk,
    s.dataset     AS dataset_code,
    s.estimate_year AS vintage_year,
    s.estimate_value,
    s.margin_of_error,
    s.margin_of_error_pct,
    NULL::TEXT AS estimate_annotation,
    NULL::TEXT AS moe_annotation,
    -- ACS's release identity is `vintage_year` (the registry's release
    -- expression), and `as_of` is served from this column -- so the same rule
    -- applies to it as to BLS and FRED: it is the row's own ingestion
    -- evidence, never the refresh's clock (DB-039). `CURRENT_DATE` here moved
    -- an ACS row's `as_of` every time a chunk was re-served, on a field the
    -- consumer guide says traces a row back to its publication.
    s.ingested_at::DATE AS as_of_date,
    s.ingested_at AS updated_at,
    -- Why a value is absent, the provider's own token, and the response it was
    -- read from. The fact has carried these since DB-055; serving dropped the
    -- rows entirely, so a cell Census suppressed and a geography that does not
    -- exist gave a consumer the same answer.
    --
    -- Appended rather than placed beside `estimate_value`, because
    -- `CREATE OR REPLACE VIEW` may only add columns at the end: inserting them
    -- mid-list fails on every warehouse that already has the view, which is
    -- every warehouse that matters.
    s.value_status,
    s.source_value,
    s.capture_id,
    -- The fact's own state, so the serving refresh can read one year and one
    -- state through `idx_fact_demo_year_state` (appended, as above).
    s.state_fips
FROM silver_census.fact_demographics s
JOIN gold_census.dim_acs_variable av
    ON av.dataset_code  = s.dataset
   AND av.vintage_year  = s.estimate_year
   AND av.variable_code = s.variable_code
-- `estimate_value IS NOT NULL` used to be here. It removed 31,481,530 of
-- 99,783,997 ACS fact rows from serving -- every cell the provider withheld --
-- and a consumer could not tell one from a geography that was never
-- published. They are served now, with a null value and the status that says
-- why. What is still excluded is a row with no variable to identify it, which
-- is not a withheld value but an unusable row.
WHERE s.variable_code IS NOT NULL
  AND s.variable_code <> '';

-- ============================================================
-- ACS-SCOPED SERVING TABLE (Source-First: ACS-specific columns only)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_census.rpt_acs_observations (
    source_code                TEXT NOT NULL DEFAULT 'CENSUS_ACS',
    observation_date           DATE NOT NULL,
    duration_start             DATE,
    duration_end               DATE,
    time_sk                    INTEGER,
    as_of_date                 DATE NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    geo_id                     TEXT NOT NULL,
    geo_level                  TEXT NOT NULL,
    state_fips                 TEXT,
    county_fips                TEXT,
    state_name                 TEXT,
    county_name                TEXT,
    -- Carried so the observation contract views can call
    -- gold_glossary.geo_name with the same arguments the geography
    -- catalog does (DB-038). Without it a place answered under its
    -- state's name here and its own name on /catalog/geographies.
    place_name                 TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    -- ACS-specific columns
    --
    -- `value` and `estimate_value` were `NOT NULL`, which is what made a
    -- withheld cell unservable: there was nowhere to put it. They are
    -- nullable now, and `value_status` is what says whether the absence is
    -- the provider's or this pipeline's -- constrained so a row cannot claim
    -- a published value and carry none.
    value                      NUMERIC,
    dataset_code               TEXT NOT NULL CONSTRAINT rpt_acs_observations_dataset_code_census_check
        CHECK (dataset_code IN ('acs1', 'acs5', 'dec2000sf1', 'dec2000sf3',
                                'dec1990stf1', 'dec1990stf3')),
    vintage_year               INTEGER NOT NULL,
    table_id                   TEXT NOT NULL,
    table_title                TEXT,
    variable_code              TEXT NOT NULL,
    variable_label             TEXT,
    concept                    TEXT,
    universe                   TEXT,
    denominator_hint           TEXT,
    is_publishable_default     BOOLEAN,
    estimate_value             NUMERIC,
    value_status               TEXT NOT NULL DEFAULT 'valid'
        CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid')),
    source_value               TEXT,
    capture_id                 UUID,
    margin_of_error            NUMERIC,
    margin_of_error_pct        NUMERIC,
    estimate_annotation        TEXT,
    moe_annotation             TEXT,
    value_type                 TEXT,
    units                      TEXT,
    -- Metric catalog association
    metric_code                TEXT,
    metric_display_name        TEXT,
    CONSTRAINT rpt_acs_observations_published_value_check
        CHECK (value_status <> 'valid' OR estimate_value IS NOT NULL)
) PARTITION BY RANGE (observation_date);

-- Why this relation is partitioned, when its six siblings are not.
--
-- Every ACS row's `observation_date` is `MAKE_DATE(estimate_year, 1, 1)`, so
-- the column holds one distinct value per vintage and the serving driver's
-- chunk is exactly one calendar year (`ACS_CHUNK_CONFIG`). That makes the year
-- chunk and the partition the same thing: the refresh truncates and refills
-- one partition instead of deleting from a 45 GB heap and re-inserting into
-- it, which leaves no dead tuples to vacuum and no index entries to clean up.
--
-- `BETA_RESET_REINGESTION.md` §7 measured what the old shape cost: tens of
-- millions of dead rows per year chunk, a heap that grew as it was re-served,
-- and an operator rule reading "vacuum manually; do not wait for autovacuum".
--
-- The declared range is fixed rather than derived from `CURRENT_DATE`, because
-- this DDL's output is compared against a checked-in schema snapshot (DB-051)
-- and a definition that changes when the year rolls over would turn every
-- January into a failed build nobody changed anything to cause.
-- `test_the_declared_partition_range_still_has_room` fails while there are
-- still years left, rather than on the first year there are none.

DO $$
DECLARE
    v_year  INTEGER;
    v_first CONSTANT INTEGER := 2000;  -- `control.acs_ingestion_slices` refuses an earlier year
    v_last  CONSTANT INTEGER := 2035;
BEGIN
    -- A warehouse that has not been rebuilt still holds a plain heap here;
    -- `CREATE TABLE IF NOT EXISTS` does not convert one. Attaching partitions
    -- to it is not possible and pretending otherwise would fail every run, so
    -- this says what is true and leaves the table alone. The rebuild is
    -- `BETA_RESET_REINGESTION.md` §7.
    IF (SELECT c.relkind
          FROM pg_class c
         WHERE c.oid = 'gold_census.rpt_acs_observations'::regclass) <> 'p' THEN
        RAISE WARNING '[ACS DDL] rpt_acs_observations is not partitioned; the '
                      'year refresh will delete rather than truncate. Rebuild '
                      'it per BETA_RESET_REINGESTION.md section 7.';
        RETURN;
    END IF;

    -- A row outside the declared range lands here rather than aborting the
    -- insert. The repository's fixtures use 2099 as a "this is obviously not
    -- real data" marker in a dozen files, and making that convention an error
    -- for this one relation would be a schema decision dressed up as a
    -- partition boundary. Nothing the pipeline produces reaches it: ACS
    -- `observation_date` is `MAKE_DATE(estimate_year, 1, 1)` and
    -- `control.acs_ingestion_slices` refuses a year outside
    -- 2000..CURRENT_YEAR+1, so a row here is a fixture or a defect, and
    -- either way it is countable rather than invisible.
    --
    -- The year refresh never truncates it, which is correct -- no chunk the
    -- serving driver plans covers a year outside the declared range, because
    -- the chunks come from silver's own `estimate_year`. A full re-serve
    -- truncates the parent, which does include it.
    IF to_regclass('gold_census.rpt_acs_observations_unranged') IS NULL THEN
        CREATE TABLE gold_census.rpt_acs_observations_unranged
            PARTITION OF gold_census.rpt_acs_observations DEFAULT;
        ALTER TABLE gold_census.rpt_acs_observations_unranged SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.01,
            autovacuum_vacuum_cost_limit = 2000
        );
    END IF;

    FOR v_year IN v_first..v_last LOOP
        IF to_regclass(
               format('gold_census.rpt_acs_observations_%s', v_year)
           ) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE gold_census.rpt_acs_observations_%1$s '
                'PARTITION OF gold_census.rpt_acs_observations '
                'FOR VALUES FROM (DATE %2$L) TO (DATE %3$L)',
                v_year,
                format('%s-01-01', v_year),
                format('%s-01-01', v_year + 1)
            );
        END IF;

        -- DB-048's thresholds, on the relation that actually has storage. A
        -- partitioned parent holds none, so `ALTER TABLE` on it sets nothing
        -- the autovacuum daemon will ever read.
        --
        -- The analyze threshold is the one that earns its place here: a
        -- truncate and refill leaves the planner describing rows that are
        -- gone. The vacuum thresholds are kept because they are still true of
        -- the paths that do not truncate -- a partial range falls back to a
        -- delete, and the default partition is never truncated by a year
        -- chunk -- and because one rule across every serving relation is
        -- worth more than an exemption that has to be remembered.
        EXECUTE format(
            'ALTER TABLE gold_census.rpt_acs_observations_%s SET ('
            'autovacuum_vacuum_scale_factor = 0.02, '
            'autovacuum_analyze_scale_factor = 0.01, '
            'autovacuum_vacuum_cost_limit = 2000)', v_year
        );
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_acs_observations_nk
    ON gold_census.rpt_acs_observations (
        geo_id,
        observation_date,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_source_geo_date
    ON gold_census.rpt_acs_observations (source_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_date
    ON gold_census.rpt_acs_observations (metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_dataset_vintage
    ON gold_census.rpt_acs_observations (dataset_code, vintage_year);

-- The release listing (/observations/releases) answers, per metric, the
-- distinct vintages with their newest as_of_date and row count. One metric's
-- ~52k rows sit on ~52k distinct heap pages of this table, so reached through
-- (metric_code, observation_date) that is one random read per row -- past the
-- API's statement timeout on a cold cache. Everything the listing reads is in
-- this index, so it runs as an index-only scan instead. as_of_date is a key
-- column rather than INCLUDEd because B-tree deduplication is disabled for
-- indexes with INCLUDE columns: the same index measured 3.2 GB with INCLUDE
-- and 477 MB as three key columns. On a live warehouse, build it with
-- CREATE INDEX CONCURRENTLY rather than holding a write lock for the
-- duration.
CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_vintage
    ON gold_census.rpt_acs_observations (metric_code, vintage_year, as_of_date);

-- The listing later learned to name each release's newest period
-- (newest_period_start/end, read as COALESCE(duration_*, observation_date)),
-- and those columns are not in the index above, so every row went back to the
-- heap: 16.8 s for one tract-backed metric on the internal stack in October
-- 2026, past the 15 s statement timeout. This index holds every column the
-- listing reads, so it is index-only again; the extra key columns repeat
-- within a vintage, so deduplication keeps it about the size of the one above,
-- which it makes redundant (drop that one in a quiet window once this exists).
-- This table is partitioned: on a live warehouse create it ON ONLY the parent,
-- build each partition's index CONCURRENTLY and ATTACH it, so the build never
-- holds a write lock on a load (runbook in the change that added it).
CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_release_period
    ON gold_census.rpt_acs_observations (
        metric_code, vintage_year, as_of_date,
        observation_date, duration_start, duration_end
    );

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_metric_geo_date
    ON gold_census.rpt_acs_observations (metric_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_updated_at
    ON gold_census.rpt_acs_observations (updated_at DESC);

-- A tract or place step is published one state per session, and each session
-- reads, clears and re-reads its own state's rows of the year three times. On
-- the 2024 partition (68 GB, 102M rows) each of those was a 29-second
-- sequential scan, so a state cost minutes of I/O before writing anything.
-- On the internal stack it was built per partition with CREATE INDEX
-- CONCURRENTLY, then created ON ONLY the parent and attached (2026-10-31), so
-- this statement finds it and does nothing there.
CREATE INDEX IF NOT EXISTS ix_rpt_acs_observations_level_state
    ON gold_census.rpt_acs_observations (geo_level, state_fips, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_acs_latest_selection
    ON gold_census.rpt_acs_observations (
        geo_id,
        variable_code,
        metric_code,
        observation_date DESC,
        updated_at DESC,
        (CASE dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END),
        vintage_year DESC
    );

-- ============================================================
-- ACS MATERIALIZED VIEW (Per-source latest)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_census.mv_acs_latest
    (LIKE gold_census.rpt_acs_observations INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

-- Autovacuum sized for the churn a year-chunked re-serve creates (DB-048).
--
-- The refresh is `DELETE ... WHERE observation_date BETWEEN` followed by a
-- re-insert, one year at a time. At PostgreSQL's 20% default scale factor a
-- table this size reaches its autovacuum threshold only after millions of dead
-- tuples: `BETA_RESET_REINGESTION.md` §7 recorded 54.7 million dead rows against 8.9 million live here, with the heap grown to 31 GB, and its operator
-- rule 2 was "vacuum manually; do not wait for autovacuum". These thresholds
-- are what that rule asks for, applied by the database instead of by a person
-- who has to remember. `ensure_*` re-applies this DDL, so an existing
-- warehouse picks them up on its next run without a migration.
ALTER TABLE gold_census.mv_acs_latest SET (
    autovacuum_vacuum_scale_factor = 0.02,   -- 2% dead, not 20%
    autovacuum_analyze_scale_factor = 0.01,  -- statistics stay close to the data
    autovacuum_vacuum_cost_limit = 2000      -- and it is allowed to keep up
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_acs_latest
    ON gold_census.mv_acs_latest (
        geo_id,
        dataset_code,
        vintage_year,
        variable_code,
        COALESCE(metric_code, '')
    );

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_source_metric
    ON gold_census.mv_acs_latest (source_code, metric_code);

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_vintage
    ON gold_census.mv_acs_latest (dataset_code, vintage_year);

CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_metric_geo
    ON gold_census.mv_acs_latest (metric_code, geo_id);

-- One metric at one grain, and one metric's served periods. The API filters
-- a grain with `geo_level = :geo_level` (the column is already the
-- vocabulary word), so an all-county map, a sibling ranking or a county
-- derived read reaches only that grain's rows instead of every tract row of
-- the metric. The period columns make /observations/periods an index-only
-- scan: with tract rows for many years, B17001_002's periods read every heap
-- row and hit the 15 s statement timeout in October 2026. On a live warehouse
-- build it with CREATE INDEX CONCURRENTLY.
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_metric_level_geo_period
    ON gold_census.mv_acs_latest (
        metric_code, geo_level, geo_id,
        observation_date, duration_start, duration_end
    );

-- One metric at one grain in one state: a city or tract map is drawn a state
-- at a time (web-place-maps), and with only the index above `state_fips` was
-- a filter over every row of the metric at that grain nationally. On
-- OFFICE_LAB (2026-10-31) Texas places read 32,631 rows to keep 1,863
-- (7.0 s) and Wisconsin tracts 111,538 to keep 1,542 (18.8 s); with this
-- index, 8.1 ms and 10.3 ms. About 11 GB. On a live warehouse build it with
-- CREATE INDEX CONCURRENTLY first (4 min 38 s there).
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_metric_level_state_geo_period
    ON gold_census.mv_acs_latest (
        metric_code, geo_level, state_fips, geo_id,
        observation_date, duration_start, duration_end
    );

-- ============================================================
-- ACS REFRESH PROCEDURES
-- ============================================================

DROP PROCEDURE IF EXISTS gold_census.refresh_rpt_acs_observations(DATE, DATE);
-- `p_geo_levels` scopes a chunk to those served grains (`ACS_CHUNK_CONFIG`'s
-- grain steps): the serving driver refreshes a year as one committed step per
-- grain group -- nation, state and county first, then tracts, then places --
-- so a place year that takes hours never holds the county rows of that year.
-- A grain-scoped step deletes rather than truncates: a truncate would take
-- ACCESS EXCLUSIVE on the whole year partition, and every reader of the year
-- (whatever grain it asked for) would wait behind it. NULL is every grain.
-- The three-argument form is replaced by one that also takes a state: left
-- in place, a three-argument call would match both and be refused.
DROP PROCEDURE IF EXISTS gold_census.refresh_rpt_acs_observations(DATE, DATE, TEXT[]);
CREATE OR REPLACE PROCEDURE gold_census.refresh_rpt_acs_observations(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_geo_levels TEXT[] DEFAULT NULL,
    -- One state of a grain step, so a large grain is published by several
    -- sessions at once (each its own states). Requires `p_geo_levels`.
    p_state_fips TEXT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
    v_affected_keys BIGINT;
    v_partitioned BOOLEAN;
    v_year INTEGER;
    v_partition TEXT;
    v_cleared_years INTEGER := 0;
    v_prev_nestloop TEXT;
BEGIN
    RAISE NOTICE '[ACS RPT CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;
    IF p_state_fips IS NOT NULL AND p_geo_levels IS NULL THEN
        RAISE EXCEPTION 'a state''s refresh names its grains: truncating a year '
                        'partition for one state would clear every other state';
    END IF;

    DROP TABLE IF EXISTS pg_temp.gold_acs_affected_keys;
    CREATE TEMP TABLE gold_acs_affected_keys (
        geo_id        TEXT NOT NULL,
        variable_code TEXT NOT NULL,
        metric_code   TEXT NOT NULL,
        PRIMARY KEY (geo_id, variable_code, metric_code)
    ) ON COMMIT DROP;

    -- Capture old keys as well as new keys so a source-side deletion removes a
    -- now-stale latest row. The row count comes out of the same scan: the
    -- clearing step below may be a TRUNCATE, which reports none.
    WITH scanned AS (
        SELECT d.geo_id, d.variable_code, d.metric_code
        FROM gold_census.rpt_acs_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
          AND (p_geo_levels IS NULL OR d.geo_level = ANY(p_geo_levels))
          AND (p_state_fips IS NULL OR d.state_fips = p_state_fips)
    ), recorded AS (
        INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
        SELECT DISTINCT geo_id, variable_code, metric_code FROM scanned
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_deleted_rows FROM scanned;

    -- Clear the range. On a partitioned table a whole calendar year is a
    -- partition, and truncating it leaves no dead tuples and no index entries
    -- to clean up -- which is the entire reason this relation is partitioned.
    -- A `DELETE` of a year left tens of millions of dead rows per chunk and an
    -- operator rule reading "vacuum manually; do not wait for autovacuum".
    --
    -- Truncating takes ACCESS EXCLUSIVE on the partition, where the delete
    -- took ROW EXCLUSIVE: a reader of *that year* waits for the chunk instead
    -- of seeing the pre-chunk rows. Other years are untouched, which the
    -- delete could not offer either -- it held row locks across the whole
    -- heap's index pages. A re-serve is a maintenance window in both shapes.
    SELECT c.relkind = 'p' INTO v_partitioned
    FROM pg_class c
    WHERE c.oid = 'gold_census.rpt_acs_observations'::regclass;

    IF p_geo_levels IS NOT NULL THEN
        -- A grain step: row locks only, so readers of other grains (and of
        -- this grain's previous rows) carry on.
        DELETE FROM gold_census.rpt_acs_observations
        WHERE (p_start_date IS NULL OR observation_date >= p_start_date)
          AND (p_end_date IS NULL OR observation_date <= p_end_date)
          AND geo_level = ANY(p_geo_levels)
          AND (p_state_fips IS NULL OR state_fips = p_state_fips);
    ELSIF v_partitioned AND p_start_date IS NULL AND p_end_date IS NULL THEN
        -- Every year, including the default partition.
        TRUNCATE TABLE gold_census.rpt_acs_observations;
        v_cleared_years := -1;
    ELSIF v_partitioned THEN
        FOR v_year IN
            SELECT generate_series(
                EXTRACT(YEAR FROM p_start_date)::INT,
                EXTRACT(YEAR FROM p_end_date)::INT
            )
        LOOP
            v_partition := format('gold_census.rpt_acs_observations_%s', v_year);
            -- Only a year the range covers end to end may be truncated. The
            -- serving driver's ACS chunk is always 1 January to 31 December
            -- (`ACS_CHUNK_CONFIG`), so this is the normal path; a partial
            -- range falls through to a delete rather than removing rows the
            -- caller did not ask about.
            IF to_regclass(v_partition) IS NOT NULL
               AND p_start_date <= MAKE_DATE(v_year, 1, 1)
               AND p_end_date >= MAKE_DATE(v_year, 12, 31) THEN
                EXECUTE format('TRUNCATE TABLE %s', v_partition);
                v_cleared_years := v_cleared_years + 1;
            ELSE
                DELETE FROM gold_census.rpt_acs_observations
                WHERE observation_date >= GREATEST(p_start_date, MAKE_DATE(v_year, 1, 1))
                  AND observation_date <= LEAST(p_end_date, MAKE_DATE(v_year, 12, 31));
            END IF;
        END LOOP;
    ELSE
        -- A warehouse that has not been rebuilt per BETA_RESET_REINGESTION.md
        -- section 7 still holds a plain heap here. It is still correct; it is
        -- only still slow.
        RAISE WARNING '[ACS RPT CHUNK] rpt_acs_observations is not partitioned; '
                      'deleting the range instead of truncating a partition.';
        DELETE FROM gold_census.rpt_acs_observations
        WHERE (p_start_date IS NULL OR observation_date >= p_start_date)
          AND (p_end_date IS NULL OR observation_date <= p_end_date);
    END IF;

    -- One state's read is estimated at a handful of rows (the view's computed
    -- grain defeats the statistics), so the planner joined the geography
    -- view by nested loop: for Delaware's 89,604 tract facts it compared
    -- 17.4 billion row pairs, and a large state ran for over twenty minutes.
    -- The real counts are thousands to millions; hash joins are right for
    -- every state. Restored after the insert, and with the transaction.
    IF p_state_fips IS NOT NULL THEN
        v_prev_nestloop := current_setting('enable_nestloop');
        PERFORM set_config('enable_nestloop', 'off', true);
    END IF;

    INSERT INTO gold_census.rpt_acs_observations (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        as_of_date,
        updated_at,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        place_name,
        geo_latitude,
        geo_longitude,
        metric_code,
        metric_display_name,
        value,
        dataset_code,
        vintage_year,
        table_id,
        table_title,
        variable_code,
        variable_label,
        concept,
        universe,
        denominator_hint,
        is_publishable_default,
        estimate_value,
        value_status,
        source_value,
        capture_id,
        margin_of_error,
        margin_of_error_pct,
        estimate_annotation,
        moe_annotation
    )
    SELECT
        'CENSUS_ACS',
        ao.observation_date,
        ao.duration_start,
        ao.duration_end,
        ao.time_sk,
        ao.as_of_date,
        ao.updated_at,
        ao.geo_id,
        ao.geo_level,
        gl.state_fips,
        gl.county_fips,
        gl.state_name,
        gl.county_name,
        -- A tract's own name rides in the place slot, the most specific name
        -- `gold_glossary.geo_name` reads; a tract has no place, and its
        -- `county_name` is its county's (sub-county-geography).
        COALESCE(gl.place_name, gl.area_name) AS place_name,
        gl.latitude,
        gl.longitude,
        -- The catalog is the published discovery surface, and the glossary
        -- composes every catalog code as source_code || ':' ||
        -- source_object_key. gold_census.metric_publisher publishes
        -- source_code 'CENSUS_ACS' and source_object_key
        -- '<dataset>:<variable>', so the served metric_code must be spelled
        -- the same way or a consumer following the catalog reads nothing.
        'CENSUS_ACS:' || ao.dataset_code || ':' || v.variable_code,
        v.variable_label,
        ao.estimate_value,
        ao.dataset_code,
        ao.vintage_year,
        t.table_id,
        t.table_title,
        v.variable_code,
        v.variable_label,
        COALESCE(v.concept,   t.concept),
        COALESCE(v.universe,  t.universe),
        v.denominator_hint,
        v.is_publishable_default,
        ao.estimate_value,
        ao.value_status,
        ao.source_value,
        ao.capture_id,
        ao.margin_of_error,
        ao.margin_of_error_pct,
        ao.estimate_annotation,
        ao.moe_annotation
    FROM gold_census.fact_acs_observation ao
    JOIN gold_census.dim_acs_table    t  ON t.acs_table_sk    = ao.acs_table_sk
    JOIN gold_census.dim_acs_variable v  ON v.acs_variable_sk = ao.acs_variable_sk
    -- gl supplies geography attributes only. Its geo_level vocabulary is
    -- 'us'/'state'/'county'; served rows promise NATIONAL/STATE/COUNTY.
    LEFT JOIN silver_ref.dim_geo gl ON gl.geo_id = ao.geo_id
    WHERE (p_start_date IS NULL OR ao.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR ao.observation_date <= p_end_date)
      -- The same window on the fact's own year, which the year index can
      -- read: `observation_date` is computed in the view, so the date bounds
      -- alone scanned the whole fact. A date in the window implies its year.
      AND (p_start_date IS NULL
           OR ao.vintage_year >= EXTRACT(YEAR FROM p_start_date)::INT)
      AND (p_end_date IS NULL
           OR ao.vintage_year <= EXTRACT(YEAR FROM p_end_date)::INT)
      AND (p_geo_levels IS NULL OR ao.geo_level = ANY(p_geo_levels))
      AND (p_state_fips IS NULL OR ao.state_fips = p_state_fips);
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    IF p_state_fips IS NOT NULL THEN
        PERFORM set_config('enable_nestloop', v_prev_nestloop, true);
    END IF;

    INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
    SELECT DISTINCT d.geo_id, d.variable_code, d.metric_code
    FROM gold_census.rpt_acs_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
      AND (p_geo_levels IS NULL OR d.geo_level = ANY(p_geo_levels))
      AND (p_state_fips IS NULL OR d.state_fips = p_state_fips)
    ON CONFLICT DO NOTHING;

    SELECT COUNT(*) INTO v_affected_keys FROM gold_acs_affected_keys;
    RAISE NOTICE
        '[ACS RPT CHUNK] status=COMPLETE start=% end=% grains=% cleared_partitions=% deleted_rows=% inserted_rows=% affected_keys=% duration_ms=%',
        p_start_date,
        p_end_date,
        COALESCE(array_to_string(p_geo_levels, ','), 'ALL'),
        -- -1 means the whole relation was truncated, 0 means the range was
        -- deleted rather than truncated. A reader watching a re-serve can tell
        -- a chunk that took the partitioned path from one that did not.
        v_cleared_years,
        v_deleted_rows,
        v_inserted_rows,
        v_affected_keys,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_census.refresh_mv_acs_latest();
CREATE OR REPLACE PROCEDURE gold_census.refresh_mv_acs_latest(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
    v_resolved_rows BIGINT;
    v_source RECORD;
    v_key_count BIGINT;
    v_hash_plans BOOLEAN := FALSE;
    v_prev_nestloop TEXT;
    v_prev_mergejoin TEXT;
BEGIN
    RAISE NOTICE '[ACS LATEST CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    IF to_regclass('pg_temp.gold_acs_affected_keys') IS NULL THEN
        CREATE TEMP TABLE gold_acs_affected_keys (
            geo_id        TEXT NOT NULL,
            variable_code TEXT NOT NULL,
            metric_code   TEXT NOT NULL,
            PRIMARY KEY (geo_id, variable_code, metric_code)
        ) ON COMMIT DROP;

        INSERT INTO gold_acs_affected_keys (geo_id, variable_code, metric_code)
        SELECT DISTINCT d.geo_id, d.variable_code, d.metric_code
        FROM gold_census.rpt_acs_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
        ON CONFLICT DO NOTHING;
    END IF;

    -- A key whose latest row is already newer than this chunk cannot change:
    -- nothing inside the chunk outranks a later observation, and the chunk's
    -- own report refresh never touches a later year. Backfilling 2023 places
    -- under a loaded 2024 deleted and re-inserted 63.6 million identical
    -- latest rows in about ten hours; for a backfill year this leaves only
    -- the keys that year adds. A chunk with no upper bound keeps every key.
    IF p_end_date IS NOT NULL THEN
        DELETE FROM gold_acs_affected_keys k
        USING gold_census.mv_acs_latest m
        WHERE m.geo_id = k.geo_id
          AND m.variable_code = k.variable_code
          AND m.metric_code = k.metric_code
          AND m.observation_date > p_end_date;
    END IF;

    -- PostgreSQL does not auto-analyze session-local temporary tables. Accurate
    -- key cardinality keeps the delete and per-key latest lookups predictable
    -- for large ACS annual chunks.
    ANALYZE gold_acs_affected_keys;

    -- A year's chunk carries millions of keys, and the planner reads the
    -- three-column key join as matching a few thousand rows. It then probes an
    -- index once per key, a random read each: measured on the internal stack,
    -- 749 seconds to resolve 4.4 million keys against one year partition,
    -- where a hash join over a sequential scan of the same partition took 56.
    -- Across a dozen partitions per chunk that exceeded the 90-minute chunk
    -- timeout from 2013 on. A small incremental chunk keeps the index probes,
    -- which are right for it; the planner settings revert at the end of this
    -- procedure, or with the transaction if it fails.
    SELECT count(*) INTO v_key_count FROM gold_acs_affected_keys;
    IF v_key_count > 100000 THEN
        v_hash_plans := TRUE;
        v_prev_nestloop := current_setting('enable_nestloop');
        v_prev_mergejoin := current_setting('enable_mergejoin');
        PERFORM set_config('enable_nestloop', 'off', true);
        PERFORM set_config('enable_mergejoin', 'off', true);
    END IF;

    DELETE FROM gold_census.mv_acs_latest m
    USING gold_acs_affected_keys k
    WHERE m.geo_id = k.geo_id
      AND m.variable_code = k.variable_code
      AND m.metric_code = k.metric_code;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    -- Resolve each key from the newest partition that holds it, instead of
    -- asking every partition for its candidate and ranking the answers.
    --
    -- The old shape was one `LATERAL ... ORDER BY observation_date DESC LIMIT
    -- 1` per key over the whole relation. Once the relation was partitioned by
    -- year that became a `Merge Append` across every partition -- the answer
    -- can be in any of them, so nothing prunes -- at **126 buffer hits to find
    -- one key's latest row**, against a single index scan before. Multiplied
    -- by the 4.4 million keys an ACS year chunk affects, it cost 1,294 seconds
    -- (DB-060).
    --
    -- Two facts make the cheap answer exact:
    --
    --   * one partition is one vintage year, because ACS `observation_date` is
    --     `MAKE_DATE(estimate_year, 1, 1)`;
    --   * within a partition the natural key is unique --
    --     `uq_rpt_acs_observations_nk` is `(geo_id, observation_date,
    --     dataset_code, vintage_year, variable_code, metric_code)`, and
    --     `observation_date` and `vintage_year` are both fixed inside one
    --     year, while `dataset_code` is carried in `metric_code`.
    --
    -- So the newest partition holding a key holds *exactly one* row for it,
    -- and that row is the latest. No ranking across partitions is needed; the
    -- search only has to stop.
    --
    -- The default partition is visited at both ends rather than skipped. It
    -- takes rows outside the declared 2000-2035 range, which are therefore
    -- either newer than every year partition or older than all of them, and
    -- correctness must not depend on it being empty -- the repository's
    -- fixtures put a 2099 row there.
    CREATE TEMP TABLE gold_acs_pending_keys (
        geo_id        TEXT NOT NULL,
        variable_code TEXT NOT NULL,
        metric_code   TEXT NOT NULL,
        PRIMARY KEY (geo_id, variable_code, metric_code)
    ) ON COMMIT DROP;
    INSERT INTO gold_acs_pending_keys
    SELECT geo_id, variable_code, metric_code FROM gold_acs_affected_keys;
    ANALYZE gold_acs_pending_keys;

    v_inserted_rows := 0;

    FOR v_source IN
        SELECT * FROM (
            -- Anything past the declared range outranks every year.
            SELECT 'gold_census.rpt_acs_observations_unranged' AS relation,
                   'observation_date > DATE ''2035-12-31''' AS predicate,
                   0 AS visit_order
            UNION ALL
            SELECT format('gold_census.rpt_acs_observations_%s', y), 'TRUE',
                   2036 - y
            FROM generate_series(2000, 2035) AS y
            UNION ALL
            -- Anything before it is outranked by every year.
            SELECT 'gold_census.rpt_acs_observations_unranged',
                   'observation_date < DATE ''2000-01-01''', 9999
        ) ordered
        ORDER BY visit_order
    LOOP
        EXIT WHEN NOT EXISTS (SELECT 1 FROM gold_acs_pending_keys);
        CONTINUE WHEN to_regclass(v_source.relation) IS NULL;

        EXECUTE format($resolve$
            WITH resolved AS (
                INSERT INTO gold_census.mv_acs_latest
                SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code) d.*
                FROM %1$s d
                JOIN gold_acs_pending_keys p
                  ON p.geo_id = d.geo_id
                 AND p.variable_code = d.variable_code
                 AND p.metric_code = d.metric_code
                WHERE %2$s
                ORDER BY d.geo_id, d.variable_code, d.metric_code,
                         d.observation_date DESC,
                         d.updated_at DESC,
                         CASE d.dataset_code WHEN 'acs1' THEN 1
                                             WHEN 'acs5' THEN 2 ELSE 9 END,
                         d.vintage_year DESC
                RETURNING geo_id, variable_code, metric_code
            )
            DELETE FROM gold_acs_pending_keys p
            USING resolved r
            WHERE p.geo_id = r.geo_id
              AND p.variable_code = r.variable_code
              AND p.metric_code = r.metric_code
        $resolve$, v_source.relation, v_source.predicate);

        GET DIAGNOSTICS v_resolved_rows = ROW_COUNT;
        v_inserted_rows := v_inserted_rows + v_resolved_rows;
    END LOOP;

    IF v_hash_plans THEN
        PERFORM set_config('enable_nestloop', v_prev_nestloop, true);
        PERFORM set_config('enable_mergejoin', v_prev_mergejoin, true);
    END IF;

    RAISE NOTICE
        '[ACS LATEST CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% hash_plans=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        v_hash_plans,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_census.refresh_dashboard_serving_layer_acs(DATE, DATE);
DROP PROCEDURE IF EXISTS gold_census.refresh_dashboard_serving_layer_acs(DATE, DATE, BOOLEAN);
CREATE OR REPLACE PROCEDURE gold_census.refresh_dashboard_serving_layer_acs(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_force_full BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_step_started TIMESTAMPTZ;
    v_watermark TIMESTAMPTZ;
    v_high_watermark TIMESTAMPTZ;
    v_effective_start DATE;
    v_effective_end DATE;
BEGIN
    SET LOCAL statement_timeout = '90min';
    SET LOCAL lock_timeout = '30s';

    INSERT INTO control.serving_refresh_state (
        source_code,
        last_silver_ingested_at,
        last_refresh_completed_at
    )
    SELECT
        'CENSUS_ACS',
        COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
        CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
    FROM gold_census.rpt_acs_observations r
    ON CONFLICT (source_code) DO NOTHING;

    SELECT last_silver_ingested_at
      INTO v_watermark
      FROM control.serving_refresh_state
     WHERE source_code = 'CENSUS_ACS'
     FOR UPDATE;

    UPDATE control.serving_refresh_state
       SET last_refresh_started_at = v_started_at,
           updated_at = NOW()
     WHERE source_code = 'CENSUS_ACS';

    SELECT
        MAX(s.ingested_at),
        MIN(MAKE_DATE(s.estimate_year, 1, 1)),
        MAX(MAKE_DATE(s.estimate_year, 1, 1))
      INTO v_high_watermark, v_effective_start, v_effective_end
      FROM silver_census.fact_demographics s
     WHERE s.estimate_value IS NOT NULL
       AND (p_start_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) >= p_start_date)
       AND (p_end_date IS NULL OR MAKE_DATE(s.estimate_year, 1, 1) <= p_end_date)
       AND (p_force_full OR s.ingested_at > v_watermark);

    IF v_effective_start IS NULL THEN
        UPDATE control.serving_refresh_state
           SET last_refresh_completed_at = clock_timestamp(),
               updated_at = NOW()
         WHERE source_code = 'CENSUS_ACS';
        RAISE NOTICE '[ACS DASHBOARD REFRESH] no changed silver rows after watermark=%', v_watermark;
        RETURN;
    END IF;

    RAISE NOTICE '[ACS DASHBOARD REFRESH] start window_start=% window_end=% watermark=% force_full=%',
        v_effective_start, v_effective_end, v_watermark, p_force_full;

    v_step_started := clock_timestamp();
    CALL gold_census.refresh_rpt_acs_observations(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] step=refresh_rpt_acs_observations duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_census.refresh_mv_acs_latest(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] step=refresh_mv_acs_latest duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    UPDATE control.serving_refresh_state
       SET last_silver_ingested_at = CASE
               WHEN p_force_full AND (p_start_date IS NOT NULL OR p_end_date IS NOT NULL)
                   THEN v_watermark
               ELSE GREATEST(v_watermark, v_high_watermark)
           END,
           last_refresh_completed_at = clock_timestamp(),
           last_window_start = v_effective_start,
           last_window_end = v_effective_end,
           updated_at = NOW()
     WHERE source_code = 'CENSUS_ACS';

    RAISE NOTICE
        '[ACS DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

-- A tract's counts on the 2020 Census tract it now lies in, for the years the
-- ACS published them on an earlier decade's tracts (acs-place-and-tract-
-- history, deliverable 7). DERIVED, never a published value: each earlier
-- tract's estimate is apportioned to the 2020 tracts its area became by the
-- Bureau's relationship files (`silver_ref.tract_decade_crosswalk`) -- the
-- share of its 2010 population for 2000 to 2010, the share of its land area
-- for 2010 to 2020 (the 2020 file publishes no population) -- and the shares
-- landing on one 2020 tract are summed. Only count tables are apportioned: a
-- median, a per-capita figure or a margin of error cannot be split by a
-- weight, so none appears here. A 2020 tract whose earlier tract is the same
-- area under the same code keeps that tract's published value, marked
-- `unchanged`; a 2020 tract any of whose contributing estimates was not
-- published has no value (`incomplete`), never a partial sum. The published
-- series in `rpt_acs_observations` is untouched.
CREATE OR REPLACE VIEW gold_census.acs_tract_comparable AS
WITH tract AS (
    SELECT geo_sk, geo_id FROM silver_ref.dim_geo_entity WHERE geo_type = 'tract'
),
to_2020 AS (
    SELECT 2010 AS source_decade,
           earlier.geo_id AS source_geo_id,
           later.geo_id AS geo_id,
           step.earlier_land_share AS weight,
           (step.earlier_geo_sk = step.later_geo_sk
            AND step.earlier_land_share = 1 AND step.later_land_share = 1) AS unchanged
    FROM silver_ref.tract_decade_crosswalk AS step
    JOIN tract AS earlier ON earlier.geo_sk = step.earlier_geo_sk
    JOIN tract AS later ON later.geo_sk = step.later_geo_sk
    WHERE step.earlier_vintage = 2010
    UNION ALL
    SELECT 2000,
           earlier.geo_id,
           later.geo_id,
           first_step.earlier_population_share * second_step.earlier_land_share,
           (first_step.earlier_geo_sk = second_step.later_geo_sk
            AND first_step.earlier_land_share = 1 AND first_step.later_land_share = 1
            AND second_step.earlier_land_share = 1 AND second_step.later_land_share = 1)
    FROM silver_ref.tract_decade_crosswalk AS first_step
    JOIN silver_ref.tract_decade_crosswalk AS second_step
      ON second_step.earlier_vintage = 2010
     AND second_step.earlier_geo_sk = first_step.later_geo_sk
    JOIN tract AS earlier ON earlier.geo_sk = first_step.earlier_geo_sk
    JOIN tract AS later ON later.geo_sk = second_step.later_geo_sk
    WHERE first_step.earlier_vintage = 2000
),
source AS (
    SELECT published.*,
           CASE WHEN published.vintage_year BETWEEN 2000 AND 2009 THEN 2000
                WHEN published.vintage_year BETWEEN 2010 AND 2019 THEN 2010
           END AS source_decade
    FROM gold_census.rpt_acs_observations AS published
    WHERE published.geo_level = 'TRACT'
      AND published.vintage_year BETWEEN 2000 AND 2019
      -- Count tables only; medians and per-capita figures do not apportion.
      AND published.table_id IN (
          'B01001', 'B01003', 'B02001', 'B03002', 'B08301', 'B08303', 'B11001',
          'B15003', 'B17001', 'B23025', 'B25001', 'B25002', 'B25003', 'B25070',
          'B25091', 'B27010', 'B28002'
      )
)
SELECT weight.geo_id,
       source.observation_date,
       source.dataset_code,
       source.vintage_year,
       source.table_id,
       source.variable_code,
       source.metric_code,
       source.variable_label,
       source.units,
       CASE WHEN BOOL_AND(source.estimate_value IS NOT NULL AND weight.weight IS NOT NULL)
            THEN SUM(source.estimate_value * weight.weight)
       END AS comparable_value,
       CASE WHEN BOOL_AND(weight.unchanged) AND COUNT(*) = 1 THEN 'unchanged'
            WHEN BOOL_AND(source.estimate_value IS NOT NULL AND weight.weight IS NOT NULL)
                THEN 'apportioned'
            ELSE 'incomplete'
       END AS comparable_status,
       CASE MIN(source.source_decade)
            WHEN 2000 THEN 'share of 2010 population, then share of land area'
            ELSE 'share of land area'
       END AS weight_basis,
       COUNT(*) AS source_tract_count,
       ARRAY_AGG(source.geo_id ORDER BY source.geo_id) AS source_geo_ids,
       2020 AS target_tract_vintage,
       'decade_crosswalk'::TEXT AS derivation
FROM source
JOIN to_2020 AS weight
  ON weight.source_geo_id = source.geo_id
 AND weight.source_decade = source.source_decade
WHERE weight.weight IS NULL OR weight.weight > 0
GROUP BY weight.geo_id, source.observation_date, source.dataset_code,
         source.vintage_year, source.table_id, source.variable_code,
         source.metric_code, source.variable_label, source.units;

COMMENT ON VIEW gold_census.acs_tract_comparable IS
    'DERIVED: earlier-decade ACS and Census 2000 tract counts apportioned to 2020 Census tracts by the Bureau''s relationship files. Never a published value; medians, per-capita figures and margins of error are not apportioned.';

