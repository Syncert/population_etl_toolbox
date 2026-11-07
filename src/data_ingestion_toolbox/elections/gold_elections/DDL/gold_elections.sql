-- Publication views over the elections source
-- (elections-and-economic-performance). Applied by the bootstrap manifest in
-- the `gold` phase and re-applied by the DAG's `ensure_elections_schema`
-- task; this file is the only definition of these views.
--
-- ADR-0014: seats are facts with dates and sources; a two-party margin is
-- the Republican share minus the Democratic share of their combined vote;
-- nothing here scores, ranks or adds measures together.

CREATE SCHEMA IF NOT EXISTS gold_elections;

CREATE OR REPLACE VIEW gold_elections.office_definition AS
SELECT office, office_label, seat_basis
FROM (VALUES
    ('president', 'President of the United States', 'nation'),
    ('us_senate', 'U.S. Senate', 'state and Senate class'),
    ('us_house', 'U.S. House of Representatives', 'state and district, redrawn after each census'),
    ('governor', 'Governor', 'state')
) AS office(office, office_label, seat_basis);

CREATE OR REPLACE VIEW gold_elections.party_definition AS
SELECT party_code, party_label, display_order
FROM (VALUES
    ('REP', 'Republican', 1),
    ('DEM', 'Democratic', 2),
    ('IND', 'Independent', 3),
    ('OTH', 'Other', 4)
) AS party(party_code, party_label, display_order);

-- The newest published read of each stream; an unchanged read publishes
-- nothing, so the read before it stays current.
CREATE OR REPLACE VIEW gold_elections.current_read AS
SELECT DISTINCT ON (run.stream)
       run.stream, run.run_id, run.source_file, run.published_at
FROM control.elections_run AS run
WHERE run.status = 'published'
ORDER BY run.stream, run.published_at DESC, run.run_id DESC;

-- Every seat-term span of the current reads. `end_date` NULL is serving; the
-- party is as the publisher records it, with the caucus beside it where an
-- independent sits with a party.
CREATE OR REPLACE VIEW gold_elections.seat_term AS
SELECT term.office,
       term.state_code,
       term.state_fips,
       term.geo_id,
       term.geo_sk,
       CASE term.office
           WHEN 'us_senate' THEN term.state_code || ' Senate class ' || term.senate_class
           WHEN 'us_house' THEN term.state_code || '-' || CASE WHEN term.district = 0 THEN 'AL'
                                                                ELSE LPAD(term.district::TEXT, 2, '0') END
           ELSE term.state_code || ' governor'
       END AS seat_label,
       term.district,
       term.senate_class,
       term.holder_id,
       term.holder_name,
       term.party_code,
       term.party_text,
       term.caucus_code,
       term.start_date,
       term.end_date,
       term.how,
       CASE read.stream
           WHEN 'legislators' THEN 'unitedstates/congress-legislators (CC0 1.0)'
           ELSE 'Wikidata (CC0 1.0)'
       END AS source_label,
       term.term_key,
       term.source_record_id,
       term.capture_id,
       term.run_id,
       read.published_at
FROM silver_elections.seat_term AS term
JOIN gold_elections.current_read AS read ON read.run_id = term.run_id
WHERE term.geography_status = 'resolved';

-- MEDSL and Algara & Amlani rows of every published read, one mode per
-- area: an area that publishes a TOTAL line is read from it alone; an area
-- that publishes only modes (absentee, election day, early) is their sum.
-- The area's total is counted once: MEDSL's county presidential file repeats
-- the county-wide total on every mode's rows (Georgia 2020 carries it on four
-- modes), so a mode total that already covers every mode's candidate votes is
-- the area's total; only totals that are each a part are added up.
-- Each level partitions by run_id, so a filter on one run reaches the silver
-- index instead of totalling every read (the speed gate's quadratic plan
-- came from joining two scans of a CTE the planner could not estimate).
CREATE OR REPLACE VIEW gold_elections.candidate_tally AS
WITH tagged AS (
    SELECT result.*, run.stream, run.source_file, run.published_at,
           COALESCE(result.county_fips, '') || '|' || COALESCE(result.district::TEXT, '') AS area_key
    FROM silver_elections.candidate_result AS result
    JOIN control.elections_run AS run ON run.run_id = result.run_id
    -- One provider serves each county contest (registry.HISTORY_SERVED_BEFORE):
    -- Algara & Amlani's presidential rows from 2000 are MEDSL's years and are
    -- kept in silver only for the agreement check (DQ-ELEC-007).
    WHERE NOT (run.stream = 'county_president_history' AND result.election_year >= 2000)
),
moded AS (
    SELECT tagged.*,
           BOOL_OR(tagged.mode = 'TOTAL') OVER area AS has_total,
           MAX(tagged.total_votes) FILTER (WHERE tagged.mode = 'TOTAL') OVER area AS total_line_votes,
           -- MEDSL repeats an area's total on every candidate row of a mode.
           MAX(tagged.total_votes) OVER area_mode AS mode_total_votes,
           MAX(tagged.total_votes) OVER area AS largest_mode_total_votes,
           SUM(tagged.candidate_votes) OVER area AS area_candidate_votes,
           ROW_NUMBER() OVER area_mode AS mode_row
    FROM tagged
    WINDOW area AS (PARTITION BY tagged.run_id, tagged.office, tagged.election_year, tagged.stage,
                                 tagged.special, tagged.state_code, tagged.area_key),
           area_mode AS (PARTITION BY tagged.run_id, tagged.office, tagged.election_year, tagged.stage,
                                      tagged.special, tagged.state_code, tagged.area_key, tagged.mode)
),
area_total AS (
    SELECT moded.*,
           -- The area's published total: its TOTAL line; else a mode total
           -- that covers every mode's candidate votes (the area total repeated
           -- on each mode); else the sum of its modes' totals.
           CASE WHEN moded.has_total THEN moded.total_line_votes
                WHEN moded.largest_mode_total_votes >= moded.area_candidate_votes
                    THEN moded.largest_mode_total_votes
                ELSE SUM(moded.mode_total_votes) FILTER (WHERE moded.mode_row = 1) OVER (
                    PARTITION BY moded.run_id, moded.office, moded.election_year, moded.stage,
                                 moded.special, moded.state_code, moded.area_key)
           END AS area_total_votes
    FROM moded
)
SELECT rows.run_id, rows.stream, rows.source_file, rows.published_at, rows.office,
       rows.election_year, rows.stage, rows.special, rows.state_code, rows.state_fips,
       rows.county_fips, rows.district, rows.geo_level, rows.geo_id, rows.geo_sk,
       rows.geography_status, rows.area_key, rows.candidate,
       -- A candidate on several ballot lines (New York's fusion lines) is one
       -- candidate; their party is the major party among their lines.
       CASE WHEN BOOL_OR(rows.party_code = 'REP') THEN 'REP'
            WHEN BOOL_OR(rows.party_code = 'DEM') THEN 'DEM'
            WHEN BOOL_OR(rows.party_code = 'IND') THEN 'IND'
            ELSE 'OTH' END AS party_code,
       STRING_AGG(DISTINCT rows.party_text, '; ' ORDER BY rows.party_text) AS party_text,
       BOOL_AND(rows.writein) AS writein,
       SUM(rows.candidate_votes)::BIGINT AS votes,
       MAX(rows.area_total_votes)::BIGINT AS published_total_votes,
       MIN(rows.capture_id::TEXT)::UUID AS capture_id
FROM area_total AS rows
WHERE NOT rows.has_total OR rows.mode = 'TOTAL'
GROUP BY rows.run_id, rows.stream, rows.source_file, rows.published_at, rows.office,
         rows.election_year, rows.stage, rows.special, rows.state_code, rows.state_fips,
         rows.county_fips, rows.district, rows.geo_level, rows.geo_id, rows.geo_sk,
         rows.geography_status, rows.area_key, rows.candidate;

-- The candidate totals of the current reads.
CREATE OR REPLACE VIEW gold_elections.candidate_total AS
SELECT tally.*
FROM gold_elections.candidate_tally AS tally
WHERE tally.run_id IN (SELECT read.run_id FROM gold_elections.current_read AS read);

-- One row per contest and area of any published read: each major party's
-- votes, the total, the two-party share and margin, and the party of the
-- leading candidate. A district row is a House district (not yet a shared
-- geography). Read it for one run_id; contest_result is the served copy.
CREATE OR REPLACE VIEW gold_elections.contest_tally AS
WITH ranked AS (
    SELECT total.*,
           ROW_NUMBER() OVER (
               PARTITION BY total.run_id, total.office, total.election_year, total.stage,
                            total.special, total.state_code, total.area_key
               ORDER BY total.votes DESC, total.candidate
           ) AS place
    FROM gold_elections.candidate_tally AS total
)
SELECT ranked.office, ranked.election_year, ranked.stage, ranked.special,
       ranked.state_code, ranked.state_fips, ranked.county_fips, ranked.district,
       ranked.geo_level, ranked.geo_id, ranked.geo_sk, ranked.geography_status,
       SUM(ranked.votes) FILTER (WHERE ranked.party_code = 'REP')::BIGINT AS republican_votes,
       SUM(ranked.votes) FILTER (WHERE ranked.party_code = 'DEM')::BIGINT AS democratic_votes,
       GREATEST(COALESCE(MAX(ranked.published_total_votes), 0), SUM(ranked.votes))::BIGINT AS total_votes,
       MAX(ranked.party_code) FILTER (WHERE ranked.place = 1) AS leading_party_code,
       MAX(ranked.candidate) FILTER (WHERE ranked.place = 1) AS leading_candidate,
       COUNT(*)::INTEGER AS candidate_count,
       ranked.stream, ranked.source_file, ranked.run_id, ranked.published_at,
       MIN(ranked.capture_id::TEXT)::UUID AS capture_id
FROM ranked
GROUP BY ranked.office, ranked.election_year, ranked.stage, ranked.special,
         ranked.state_code, ranked.state_fips, ranked.county_fips, ranked.district,
         ranked.geo_level, ranked.geo_id, ranked.geo_sk, ranked.geography_status,
         ranked.area_key, ranked.stream, ranked.source_file, ranked.run_id, ranked.published_at;

-- The contest rows of the current reads, written once when a read is
-- published (silver_elections.load.publish_run) so the user path and the DQ
-- rules read an indexed table instead of re-totalling every candidate line.
-- An unchanged read publishes nothing, so nothing here is rewritten (the
-- CHURN rule); a superseded read's rows are removed when its successor lands.
CREATE TABLE IF NOT EXISTS gold_elections.contest_result_store AS
SELECT * FROM gold_elections.contest_tally WITH NO DATA;

CREATE INDEX IF NOT EXISTS elections_contest_store_run_idx
    ON gold_elections.contest_result_store (run_id);
CREATE INDEX IF NOT EXISTS elections_contest_store_geo_idx
    ON gold_elections.contest_result_store (geo_id, office);
CREATE INDEX IF NOT EXISTS elections_contest_store_year_idx
    ON gold_elections.contest_result_store (office, geo_level, election_year);

-- Bring the stored contests in line with the current reads: drop a
-- superseded read's rows and total any current read not yet stored. One
-- caller at a time, so two streams publishing together cannot both store a
-- read. Returns the number of contest rows written.
CREATE OR REPLACE FUNCTION gold_elections.sync_contest_result()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    written INTEGER := 0;
    added INTEGER;
    pending RECORD;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_elections.contest_result_store'));
    DELETE FROM gold_elections.contest_result_store AS store
     WHERE NOT EXISTS (
        SELECT 1 FROM gold_elections.current_read AS read WHERE read.run_id = store.run_id
     );
    FOR pending IN
        SELECT read.run_id
          FROM gold_elections.current_read AS read
          JOIN control.elections_run AS run ON run.run_id = read.run_id
         WHERE run.stream <> ALL (ARRAY['legislators', 'governors'])
           AND NOT EXISTS (
               SELECT 1 FROM gold_elections.contest_result_store AS store
                WHERE store.run_id = read.run_id
           )
    LOOP
        INSERT INTO gold_elections.contest_result_store
        SELECT * FROM gold_elections.contest_tally AS tally WHERE tally.run_id = pending.run_id;
        GET DIAGNOSTICS added = ROW_COUNT;
        written := written + added;
    END LOOP;
    RETURN written;
END;
$$;

-- Bring the stored rows of every current read in line with a fresh total,
-- for when the totalling itself changes (this file is re-applied only when it
-- changes). A row whose fresh total is identical is left alone (the CHURN
-- rule): only a changed contest's old row is removed and its new row written.
-- Read per run, like sync_contest_result. Returns the number of rows written.
CREATE OR REPLACE FUNCTION gold_elections.reconcile_contest_result()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    written INTEGER := gold_elections.sync_contest_result();
    added INTEGER;
    pending RECORD;
BEGIN
    FOR pending IN
        SELECT read.run_id
          FROM gold_elections.current_read AS read
          JOIN control.elections_run AS run ON run.run_id = read.run_id
         WHERE run.stream <> ALL (ARRAY['legislators', 'governors'])
    LOOP
        CREATE TEMP TABLE IF NOT EXISTS reconcile_fresh (LIKE gold_elections.contest_result_store)
            ON COMMIT DROP;
        TRUNCATE reconcile_fresh;
        INSERT INTO reconcile_fresh
        SELECT * FROM gold_elections.contest_tally AS tally WHERE tally.run_id = pending.run_id;
        ANALYZE reconcile_fresh;
        -- Whole rows compared as text: NULLs compare equal and the anti-joins
        -- hash instead of looping.
        DELETE FROM gold_elections.contest_result_store AS store
         WHERE store.run_id = pending.run_id
           AND NOT EXISTS (
               SELECT 1 FROM reconcile_fresh AS fresh
                WHERE ROW(fresh.*)::TEXT = ROW(store.*)::TEXT
           );
        INSERT INTO gold_elections.contest_result_store
        SELECT fresh.* FROM reconcile_fresh AS fresh
         WHERE NOT EXISTS (
             SELECT 1 FROM gold_elections.contest_result_store AS store
              WHERE store.run_id = pending.run_id
                AND ROW(store.*)::TEXT = ROW(fresh.*)::TEXT
         );
        GET DIAGNOSTICS added = ROW_COUNT;
        written := written + added;
    END LOOP;
    RETURN written;
END;
$$;

-- Only the warehouse owner writes the store; the serving role reads it.
REVOKE EXECUTE ON FUNCTION gold_elections.sync_contest_result() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION gold_elections.reconcile_contest_result() FROM PUBLIC;

-- One row per contest and area of the current reads (see contest_tally).
CREATE OR REPLACE VIEW gold_elections.contest_result AS
SELECT store.office, store.election_year, store.stage, store.special,
       store.state_code, store.state_fips, store.county_fips, store.district,
       store.geo_level, store.geo_id, store.geo_sk, store.geography_status,
       store.republican_votes, store.democratic_votes, store.total_votes,
       store.leading_party_code, store.leading_candidate, store.candidate_count,
       -- The read's stream, file and publication time come from the read itself.
       read.stream, read.source_file, store.run_id, read.published_at, store.capture_id
FROM gold_elections.contest_result_store AS store
JOIN gold_elections.current_read AS read ON read.run_id = store.run_id;

-- The published measures: the general-election presidential vote by county
-- and state.
CREATE OR REPLACE VIEW gold_elections.measure_definition AS
SELECT measure, measure_label, unit, observation_basis
FROM (VALUES
    ('president_two_party_republican_share',
     'Presidential vote: Republican share of the two-party vote', 'percent',
     'Certified returns (MIT Election Data and Science Lab; county returns before 2000 from Algara & '
     || 'Amlani): the Republican candidate''s votes as a share of the Republican and Democratic '
     || 'candidates'' combined votes in the general election, counting a candidate''s every ballot '
     || 'line. A comparison of places, not a measure of how any voter fared.'),
    ('president_two_party_margin',
     'Presidential vote: two-party margin (Republican minus Democratic)', 'percentage points',
     'Certified returns (MIT Election Data and Science Lab; county returns before 2000 from Algara & '
     || 'Amlani): the Republican share minus the Democratic share of the two-party vote in the '
     || 'general election; positive leans Republican, negative Democratic.'),
    ('president_total_votes',
     'Presidential vote: total votes cast', 'votes',
     'Certified returns (MIT Election Data and Science Lab; county returns before 2000 from Algara & '
     || 'Amlani): every vote cast for president in the area, including candidates of every party '
     || 'and write-ins.'),
    ('senate_two_party_republican_share',
     'U.S. Senate vote: Republican share of the two-party vote', 'percent',
     'Certified returns (MIT Election Data and Science Lab; county returns to 2020 from Algara & '
     || 'Amlani): the Republican nominee''s votes as a share of the two major parties'' nominees'' '
     || 'combined votes in the regularly scheduled general election for the seat. A special election '
     || 'held the same year is kept in the contest results but not served here.'),
    ('senate_two_party_margin',
     'U.S. Senate vote: two-party margin (Republican minus Democratic)', 'percentage points',
     'Certified returns (MIT Election Data and Science Lab; county returns to 2020 from Algara & '
     || 'Amlani): the Republican share minus the Democratic share of the two-party vote in the '
     || 'regularly scheduled general election; positive leans Republican. Where a party ran no '
     || 'nominee the margin is 100 points one way, never a missing value read as zero.'),
    ('senate_total_votes',
     'U.S. Senate vote: total votes cast', 'votes',
     'Certified returns (MIT Election Data and Science Lab; county returns to 2020 from Algara & '
     || 'Amlani): every vote cast for the Senate seat in the area, including candidates of every '
     || 'party.')
) AS definition(measure, measure_label, unit, observation_basis);

-- The presidential general-election measures in the neutral observation
-- shape, dated on election day (the Tuesday after the first Monday of
-- November). The predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_elections.observation_revision AS
WITH contest AS (
    SELECT result.*,
           (MAKE_DATE(result.election_year, 11, 2)
            + ((2 - EXTRACT(DOW FROM MAKE_DATE(result.election_year, 11, 2))::INTEGER + 7) % 7))
               AS election_day,
           NULLIF(COALESCE(result.republican_votes, 0) + COALESCE(result.democratic_votes, 0), 0)
               AS two_party_votes
    FROM gold_elections.contest_result AS result
    WHERE result.office IN ('president', 'us_senate') AND result.stage = 'GEN' AND NOT result.special
      AND result.geo_level IN ('state', 'county')
),
measured AS (
    SELECT contest.*, measure.prefix || measure.suffix AS metric_key,
           CASE measure.suffix
               WHEN '_two_party_republican_share'
                   THEN ROUND(100.0 * COALESCE(contest.republican_votes, 0) / contest.two_party_votes, 2)
               WHEN '_two_party_margin'
                   THEN ROUND(100.0 * (COALESCE(contest.republican_votes, 0)
                                       - COALESCE(contest.democratic_votes, 0)) / contest.two_party_votes, 2)
               ELSE contest.total_votes::NUMERIC
           END AS value
    FROM contest
    JOIN (VALUES ('president', 'president'), ('us_senate', 'senate')) AS office_prefix(office, prefix)
      ON office_prefix.office = contest.office
    CROSS JOIN LATERAL (
        SELECT office_prefix.prefix, suffix
        FROM (VALUES ('_two_party_republican_share'), ('_two_party_margin'), ('_total_votes'))
             AS suffixes(suffix)
    ) AS measure
)
SELECT measured.metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       measured.election_year::INTEGER AS year,
       measured.election_day AS period_start,
       measured.election_day AS period_end,
       measured.geo_id,
       measured.geo_sk,
       measured.geo_level AS geo_type,
       gold_glossary.geo_grain(measured.geo_level) AS geo_level,
       measured.value,
       CASE WHEN measured.value IS NULL THEN 'missing' ELSE 'valid' END AS value_status,
       CASE WHEN measured.value IS NULL THEN 'no_major_party_votes' END AS missing_reason,
       measured.republican_votes,
       measured.democratic_votes,
       measured.total_votes,
       measured.leading_party_code,
       CASE WHEN measured.stream LIKE '%\_history' THEN 'Algara & Amlani ' ELSE 'MEDSL ' END
           || COALESCE(measured.source_file, measured.stream) AS release_key,
       measured.published_at,
       md5(measured.metric_key || '|' || measured.geo_id || '|' || measured.election_year::TEXT)
           AS source_record_id,
       measured.capture_id,
       measured.run_id
FROM measured
JOIN gold_elections.measure_definition AS definition ON definition.measure = measured.metric_key
WHERE measured.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped', 'district');

-- The newest release of each observation.
CREATE OR REPLACE VIEW gold_elections.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_elections.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_elections.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'elections_files:v1'::TEXT AS schema_version
FROM gold_elections.measure_definition;

COMMENT ON SCHEMA gold_elections IS
    'Policy-free publication views for congressional and governor seat terms and MEDSL election results.';

-- Store any current read published before contest_result_store existed,
-- and rewrite only the stored contests whose totalling changed.
SELECT gold_elections.reconcile_contest_result();
