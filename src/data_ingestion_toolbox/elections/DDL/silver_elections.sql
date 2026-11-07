-- Control and silver relations for the elections source
-- (elections-and-economic-performance): congressional seat terms,
-- governors, and MEDSL election results.

CREATE SCHEMA IF NOT EXISTS silver_elections;

-- One run per read of a stream. A read whose files are byte-for-byte the last
-- published read's is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.elections_run (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    stream TEXT NOT NULL CHECK (
        stream IN ('legislators', 'governors', 'county_president', 'state_president', 'senate', 'house',
                   'county_president_history', 'county_senate_history', 'county_senate')
    ),
    status TEXT NOT NULL CHECK (
        status IN ('capturing', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')
    ),
    run_checksum TEXT CHECK (run_checksum IS NULL OR run_checksum ~ '^[0-9a-f]{64}$'),
    -- The Dataverse file a results read chose from its dataset's list.
    source_file TEXT,
    file_count INTEGER NOT NULL DEFAULT 0 CHECK (file_count >= 0),
    record_count INTEGER NOT NULL DEFAULT 0 CHECK (record_count >= 0),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT elections_run_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

-- The county history streams (plan election-results-back-to-1990) widen the
-- stream list of a table created before them. Widening a list invalidates
-- nothing stored; a CHECK that already names the new streams is left alone,
-- so a rerun changes nothing.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.elections_run'::regclass
          AND conname = 'elections_run_stream_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%county_senate_history%'
    ) THEN
        ALTER TABLE control.elections_run DROP CONSTRAINT elections_run_stream_check;
        ALTER TABLE control.elections_run ADD CONSTRAINT elections_run_stream_check CHECK (
            stream IN ('legislators', 'governors', 'county_president', 'state_president', 'senate', 'house',
                       'county_president_history', 'county_senate_history', 'county_senate')
        );
    END IF;
END
$$;

-- One row per committed file of a run: a results read's Dataverse list, then
-- its data file; the two legislators files; the Wikidata answer.
CREATE TABLE IF NOT EXISTS control.elections_file (
    run_id UUID NOT NULL REFERENCES control.elections_run(run_id),
    file_index INTEGER NOT NULL CHECK (file_index >= 0),
    file_key TEXT NOT NULL,
    file_kind TEXT NOT NULL CHECK (file_kind IN ('legislators', 'sparql', 'listing', 'results')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    PRIMARY KEY (run_id, file_index)
);

CREATE TABLE IF NOT EXISTS silver_elections.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, record_index, error_code)
);

-- One span of one person in one seat with one party. A congressional term
-- whose party changed is one span per affiliation. A seat is a state's
-- governorship, a state's Senate class, or a state's House district (0 is
-- at-large) in the Congress the term began in. `end_date` NULL is serving.
CREATE TABLE IF NOT EXISTS silver_elections.seat_term (
    run_id UUID NOT NULL REFERENCES control.elections_run(run_id),
    term_key TEXT NOT NULL CHECK (term_key ~ '^[0-9a-f]{64}$'),
    office TEXT NOT NULL CHECK (office IN ('us_senate', 'us_house', 'governor')),
    state_code TEXT NOT NULL CHECK (state_code ~ '^[A-Z]{2}$'),
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    district INTEGER CHECK (district IS NULL OR district >= 0),
    senate_class SMALLINT CHECK (senate_class IS NULL OR senate_class IN (1, 2, 3)),
    holder_id TEXT NOT NULL,
    holder_name TEXT NOT NULL,
    party_text TEXT,
    party_code TEXT NOT NULL CHECK (party_code IN ('REP', 'DEM', 'IND', 'OTH')),
    caucus_code TEXT CHECK (caucus_code IS NULL OR caucus_code IN ('REP', 'DEM')),
    start_date DATE NOT NULL,
    end_date DATE,
    how TEXT,
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, term_key),
    CONSTRAINT elections_term_dates_ordered CHECK (end_date IS NULL OR end_date >= start_date),
    CONSTRAINT elections_term_seat_shape CHECK (
        (office = 'us_senate' AND senate_class IS NOT NULL AND district IS NULL)
        OR (office = 'us_house' AND district IS NOT NULL AND senate_class IS NULL)
        OR (office = 'governor' AND district IS NULL AND senate_class IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS elections_seat_term_state_idx
    ON silver_elections.seat_term (state_fips, office, start_date);

-- One candidate's votes on one ballot line, in one mode, in one contest, in
-- one area, exactly as MEDSL publishes it. A House row's area is its
-- district (`geography_status = 'district'`): districts are not yet a shared
-- geography, so they are kept and never served as a place.
CREATE TABLE IF NOT EXISTS silver_elections.candidate_result (
    run_id UUID NOT NULL REFERENCES control.elections_run(run_id),
    result_key TEXT NOT NULL CHECK (result_key ~ '^[0-9a-f]{64}$'),
    office TEXT NOT NULL CHECK (office IN ('president', 'us_senate', 'us_house')),
    election_year SMALLINT NOT NULL CHECK (election_year BETWEEN 1789 AND 2100),
    stage TEXT NOT NULL,
    special BOOLEAN NOT NULL,
    state_code TEXT NOT NULL CHECK (state_code ~ '^[A-Z]{2}$'),
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    county_fips TEXT CHECK (county_fips IS NULL OR county_fips ~ '^[0-9]{5}$'),
    district INTEGER CHECK (district IS NULL OR district >= 0),
    candidate TEXT NOT NULL,
    party_text TEXT,
    party_code TEXT NOT NULL CHECK (party_code IN ('REP', 'DEM', 'IND', 'OTH')),
    writein BOOLEAN NOT NULL,
    mode TEXT NOT NULL,
    candidate_votes BIGINT NOT NULL CHECK (candidate_votes >= 0),
    total_votes BIGINT CHECK (total_votes IS NULL OR total_votes >= 0),
    geo_level TEXT NOT NULL CHECK (geo_level IN ('state', 'county', 'district')),
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'district')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, result_key),
    CONSTRAINT elections_result_level_shape CHECK (
        (geo_level = 'county' AND county_fips IS NOT NULL)
        OR (geo_level = 'district' AND district IS NOT NULL AND geography_status = 'district')
        OR (geo_level = 'state' AND county_fips IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS elections_result_contest_idx
    ON silver_elections.candidate_result (run_id, office, election_year, geo_id);
