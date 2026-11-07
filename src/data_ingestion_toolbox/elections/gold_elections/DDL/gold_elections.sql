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

-- MEDSL rows of the current reads, one mode per area: an area that
-- publishes a TOTAL line is read from it alone; an area that publishes only
-- modes (absentee, election day, early) is their sum.
CREATE OR REPLACE VIEW gold_elections.candidate_total AS
WITH current_rows AS (
    SELECT result.*, read.stream, read.source_file, read.published_at,
           COALESCE(result.county_fips, '') || '|' || COALESCE(result.district::TEXT, '') AS area_key
    FROM silver_elections.candidate_result AS result
    JOIN gold_elections.current_read AS read ON read.run_id = result.run_id
    -- One provider serves each county contest (registry.HISTORY_SERVED_BEFORE):
    -- Algara & Amlani's presidential rows from 2000 are MEDSL's years and are
    -- kept in silver only for the agreement check (DQ-ELEC-007).
    WHERE NOT (read.stream = 'county_president_history' AND result.election_year >= 2000)
),
mode_total AS (
    -- MEDSL repeats an area's total on every candidate row of a mode.
    SELECT run_id, office, election_year, stage, special, state_code, area_key, mode,
           MAX(total_votes) AS total_votes
    FROM current_rows
    GROUP BY run_id, office, election_year, stage, special, state_code, area_key, mode
),
area AS (
    SELECT run_id, office, election_year, stage, special, state_code, area_key,
           BOOL_OR(mode = 'TOTAL') AS has_total,
           -- The area's published total: its TOTAL line, or the sum of its modes.
           CASE WHEN BOOL_OR(mode = 'TOTAL') THEN MAX(total_votes) FILTER (WHERE mode = 'TOTAL')
                ELSE SUM(total_votes) END AS area_total_votes
    FROM mode_total
    GROUP BY run_id, office, election_year, stage, special, state_code, area_key
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
       MAX(area.area_total_votes)::BIGINT AS published_total_votes,
       MIN(rows.capture_id::TEXT)::UUID AS capture_id
FROM current_rows AS rows
JOIN area USING (run_id, office, election_year, stage, special, state_code, area_key)
WHERE NOT area.has_total OR rows.mode = 'TOTAL'
GROUP BY rows.run_id, rows.stream, rows.source_file, rows.published_at, rows.office,
         rows.election_year, rows.stage, rows.special, rows.state_code, rows.state_fips,
         rows.county_fips, rows.district, rows.geo_level, rows.geo_id, rows.geo_sk,
         rows.geography_status, rows.area_key, rows.candidate;

-- One row per contest and area: each major party's votes, the total, the
-- two-party share and margin, and the party of the leading candidate.
-- A district row is a House district (not yet a shared geography).
CREATE OR REPLACE VIEW gold_elections.contest_result AS
WITH ranked AS (
    SELECT total.*,
           ROW_NUMBER() OVER (
               PARTITION BY total.run_id, total.office, total.election_year, total.stage,
                            total.special, total.state_code, total.area_key
               ORDER BY total.votes DESC, total.candidate
           ) AS place
    FROM gold_elections.candidate_total AS total
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
