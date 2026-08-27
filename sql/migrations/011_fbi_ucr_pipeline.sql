-- Capture-first FBI Uniform Crime Reporting (UCR) warehouse contract.
-- Fresh-bootstrap and idempotent rerun DDL for the disposable beta warehouse.
--
-- Three source distinctions are enforced structurally rather than by
-- convention: a provider-published national/state observation is a different
-- geography basis from an agency observation, an absolute total is a different
-- measure form from a rate, and a month with no report is never a zero value.

CREATE SCHEMA IF NOT EXISTS silver_fbi;
CREATE SCHEMA IF NOT EXISTS gold_fbi;

CREATE TABLE IF NOT EXISTS control.fbi_ucr_release (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL CHECK (BTRIM(product_id) <> ''),
    ucr_program TEXT NOT NULL CHECK (BTRIM(ucr_program) <> ''),
    offense_code TEXT NOT NULL CHECK (offense_code ~ '^[A-Z]{1,3}$'),
    period_start TEXT NOT NULL CHECK (period_start ~ '^(0[1-9]|1[0-2])-[0-9]{4}$'),
    period_end TEXT NOT NULL CHECK (period_end ~ '^(0[1-9]|1[0-2])-[0-9]{4}$'),
    refresh_date DATE,
    max_data_month TEXT CHECK (max_data_month ~ '^(0[1-9]|1[0-2])/[0-9]{4}$'),
    parser_contract_version TEXT NOT NULL,
    subject_scope JSONB NOT NULL,
    release_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    decision TEXT NOT NULL CHECK (decision IN (
        'unchanged', 'ingest', 'missing_release_quarantine',
        'backward_refresh_quarantine', 'period_unavailable_quarantine'
    )),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'quarantined', 'silver_ready', 'published'
    )),
    directory_slice_count INTEGER NOT NULL DEFAULT 0
        CHECK (directory_slice_count >= 0),
    observation_slice_count INTEGER NOT NULL DEFAULT 0
        CHECK (observation_slice_count >= 0),
    complete BOOLEAN NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (status <> 'published' OR published_at IS NOT NULL),
    CHECK (decision <> 'ingest' OR status = 'quarantined' OR complete),
    CHECK (decision = 'missing_release_quarantine' OR refresh_date IS NOT NULL),
    UNIQUE (product_id, refresh_date, run_id)
);
CREATE INDEX IF NOT EXISTS fbi_ucr_release_latest_idx
    ON control.fbi_ucr_release (product_id, refresh_date DESC, created_at DESC);

-- ---------------------------------------------------------------------------
-- Capture-scoped source revisions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_fbi.agency_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    ori TEXT NOT NULL CHECK (ori ~ '^[A-Z]{2}[A-Z0-9]{7}$'),
    agency_name TEXT NOT NULL CHECK (BTRIM(agency_name) <> ''),
    agency_type TEXT NOT NULL CHECK (BTRIM(agency_type) <> ''),
    state_code TEXT NOT NULL CHECK (state_code ~ '^[A-Z]{2}$'),
    state_name TEXT,
    county_labels TEXT[] NOT NULL,
    is_nibrs BOOLEAN,
    nibrs_start_date TEXT,
    latitude DOUBLE PRECISION CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION
        CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    source_record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CHECK (NOT ('NOT SPECIFIED' = ANY (county_labels)))
);
CREATE INDEX IF NOT EXISTS fbi_agency_revision_run_idx
    ON silver_fbi.agency_revision (run_id, product_id, release_key);

CREATE TABLE IF NOT EXISTS silver_fbi.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    ucr_program TEXT NOT NULL,
    offense_code TEXT NOT NULL,
    offense_label TEXT NOT NULL,
    measure_id TEXT NOT NULL,
    measure_form TEXT NOT NULL CHECK (measure_form IN (
        'absolute_total', 'rate', 'percentage', 'trend', 'category_count'
    )),
    counted_entity_basis TEXT NOT NULL CHECK (counted_entity_basis IN (
        'offense', 'clearance', 'arrest', 'incident', 'victim'
    )),
    unit TEXT NOT NULL,
    reported_status TEXT NOT NULL CHECK (reported_status IN (
        'reported', 'estimated', 'mixed'
    )),
    subject_type TEXT NOT NULL CHECK (
        subject_type IN ('national', 'state', 'agency')
    ),
    subject_code TEXT NOT NULL,
    subject_label TEXT NOT NULL,
    source_geo_level TEXT NOT NULL,
    period TEXT NOT NULL CHECK (period ~ '^(0[1-9]|1[0-2])-[0-9]{4}$'),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL CHECK (period_end >= period_start),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (
        value_status IN ('reported', 'not_reported')
    ),
    population_denominator NUMERIC
        CHECK (population_denominator IS NULL OR population_denominator >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    -- A published zero stays a value; an absent month never becomes one.
    CHECK (value_status <> 'reported' OR value IS NOT NULL),
    CHECK (value_status <> 'not_reported' OR value IS NULL)
);
CREATE INDEX IF NOT EXISTS fbi_observation_revision_run_idx
    ON silver_fbi.observation_revision (run_id, product_id, release_key);

CREATE TABLE IF NOT EXISTS silver_fbi.participation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    ucr_program TEXT NOT NULL,
    subject_type TEXT NOT NULL CHECK (
        subject_type IN ('national', 'state', 'agency')
    ),
    subject_code TEXT NOT NULL,
    subject_label TEXT NOT NULL,
    source_geo_level TEXT NOT NULL,
    period TEXT NOT NULL CHECK (period ~ '^(0[1-9]|1[0-2])-[0-9]{4}$'),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL CHECK (period_end >= period_start),
    population NUMERIC CHECK (population IS NULL OR population >= 0),
    participated_population NUMERIC
        CHECK (participated_population IS NULL OR participated_population >= 0),
    coverage_percent NUMERIC
        CHECK (coverage_percent IS NULL OR coverage_percent BETWEEN 0 AND 100),
    coverage_basis TEXT NOT NULL CHECK (coverage_basis IN (
        'provider_population_coverage_percent', 'provider_population_only'
    )),
    participation_status TEXT NOT NULL CHECK (participation_status IN (
        'full_participation', 'partial_participation', 'no_participation',
        'unknown'
    )),
    source_record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CHECK (
        population IS NULL OR participated_population IS NULL
        OR participated_population <= population
    )
);
CREATE INDEX IF NOT EXISTS fbi_participation_revision_run_idx
    ON silver_fbi.participation_revision (run_id, product_id, release_key);

CREATE TABLE IF NOT EXISTS silver_fbi.slice_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    slice_key TEXT NOT NULL,
    source_row_index BIGINT NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        run_id, product_id, release_key, slice_key, source_row_index, error_code
    )
);

-- ---------------------------------------------------------------------------
-- Conformed silver model
-- ---------------------------------------------------------------------------

-- Frozen provider state contract. ``state_fips`` is NULL for documented
-- provider codes that designate no canonical Census state (federal agencies
-- and other non-state groupings); those never resolve to a guessed code.
CREATE TABLE IF NOT EXISTS silver_fbi.dim_state_code (
    state_code TEXT PRIMARY KEY CHECK (state_code ~ '^[A-Z]{2}$'),
    state_label TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    is_supported BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (is_supported = (state_fips IS NOT NULL))
);

-- Reviewed, effective-dated ORI-to-place crosswalk. Content is owned by
-- checked-in Python so every entry is reviewed in code; it is materialized
-- here only so silver conformance can join against it.
CREATE TABLE IF NOT EXISTS silver_fbi.reviewed_place_crosswalk (
    ori TEXT NOT NULL CHECK (ori ~ '^[A-Z]{2}[A-Z0-9]{7}$'),
    place_geo_id TEXT NOT NULL,
    place_name TEXT NOT NULL,
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    place_fips TEXT NOT NULL CHECK (place_fips ~ '^[0-9]{5}$'),
    geography_vintage INTEGER NOT NULL,
    effective_start DATE NOT NULL,
    effective_end DATE,
    crosswalk_version TEXT NOT NULL,
    evidence_url TEXT NOT NULL,
    review_note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ori, place_geo_id, effective_start),
    CHECK (effective_end IS NULL OR effective_end >= effective_start),
    CHECK (place_geo_id = 'state:' || state_fips || '|place:' || place_fips)
);

CREATE TABLE IF NOT EXISTS silver_fbi.dim_ucr_dataset_release (
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL CHECK (release_key ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'),
    refresh_date DATE NOT NULL,
    max_data_month TEXT NOT NULL,
    ucr_program TEXT NOT NULL,
    offense_code TEXT NOT NULL,
    offense_label TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    documentation_url TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    reported_status TEXT NOT NULL,
    counted_entity_note TEXT NOT NULL,
    release_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    source_record_count BIGINT NOT NULL CHECK (source_record_count >= 0),
    quarantine_count BIGINT NOT NULL CHECK (quarantine_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('replaying', 'silver_ready', 'published')),
    reconciled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, release_key),
    CHECK (status = 'replaying' OR reconciled_at IS NOT NULL),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS silver_fbi.dim_offense_measure (
    product_id TEXT NOT NULL,
    measure_id TEXT NOT NULL,
    ucr_program TEXT NOT NULL,
    offense_code TEXT NOT NULL,
    offense_label TEXT NOT NULL,
    measure_form TEXT NOT NULL,
    counted_entity_basis TEXT NOT NULL,
    unit TEXT NOT NULL,
    reported_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, measure_id)
);

CREATE TABLE IF NOT EXISTS silver_fbi.dim_agency (
    ori TEXT PRIMARY KEY CHECK (ori ~ '^[A-Z]{2}[A-Z0-9]{7}$'),
    state_code TEXT NOT NULL CHECK (state_code ~ '^[A-Z]{2}$'),
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    first_seen_release TEXT NOT NULL,
    last_seen_release TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_fbi.dim_agency_version (
    ori TEXT NOT NULL REFERENCES silver_fbi.dim_agency(ori),
    release_key TEXT NOT NULL,
    agency_name TEXT NOT NULL,
    agency_type TEXT NOT NULL,
    state_name TEXT,
    county_labels TEXT[] NOT NULL,
    is_nibrs BOOLEAN,
    nibrs_start_date TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    attribute_checksum TEXT NOT NULL CHECK (attribute_checksum ~ '^[0-9a-f]{64}$'),
    evidence_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ori, release_key)
);

CREATE TABLE IF NOT EXISTS silver_fbi.agency_geography_relationship (
    relationship_sk BIGSERIAL PRIMARY KEY,
    ori TEXT NOT NULL REFERENCES silver_fbi.dim_agency(ori),
    relationship_type TEXT NOT NULL CHECK (
        relationship_type IN ('state', 'county', 'place')
    ),
    source_label TEXT NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    resolution_method TEXT CHECK (resolution_method IS NULL OR resolution_method IN (
        'exact_state_code', 'reviewed_county_name_crosswalk',
        'reviewed_place_crosswalk'
    )),
    resolution_status TEXT NOT NULL CHECK (resolution_status IN (
        'resolved', 'unresolved', 'ambiguous', 'unsupported'
    )),
    confidence_class TEXT NOT NULL CHECK (
        confidence_class IN ('exact', 'reviewed', 'unresolved')
    ),
    reason_code TEXT,
    effective_start DATE NOT NULL,
    effective_end DATE NOT NULL CHECK (effective_end >= effective_start),
    geography_vintage INTEGER NOT NULL,
    evidence_source TEXT NOT NULL,
    evidence_capture_id UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ori, relationship_type, source_label, geography_vintage, effective_start),
    CHECK (resolution_status <> 'resolved' OR geo_id IS NOT NULL),
    CHECK (resolution_status = 'resolved' OR geo_sk IS NULL)
);
CREATE INDEX IF NOT EXISTS fbi_agency_relationship_geo_idx
    ON silver_fbi.agency_geography_relationship (relationship_type, geo_id)
    WHERE resolution_status = 'resolved';

CREATE TABLE IF NOT EXISTS silver_fbi.fact_reporting_participation (
    participation_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    ucr_program TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_code TEXT NOT NULL,
    subject_label TEXT NOT NULL,
    source_geo_level TEXT NOT NULL,
    period TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geography_status TEXT NOT NULL CHECK (geography_status IN (
        'provider_geo_exact', 'agency_only', 'agency_county_bridged',
        'agency_place_bridged', 'ambiguous', 'unsupported'
    )),
    population NUMERIC,
    participated_population NUMERIC,
    coverage_percent NUMERIC,
    coverage_basis TEXT NOT NULL,
    participation_status TEXT NOT NULL,
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL,
    transformation_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (product_id, release_key)
        REFERENCES silver_fbi.dim_ucr_dataset_release(product_id, release_key),
    UNIQUE (product_id, release_key, subject_type, subject_code, period)
);

CREATE TABLE IF NOT EXISTS silver_fbi.fact_crime_observation (
    observation_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    release_key TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    measure_id TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_code TEXT NOT NULL,
    subject_label TEXT NOT NULL,
    source_geo_level TEXT NOT NULL,
    period TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geography_status TEXT NOT NULL CHECK (geography_status IN (
        'provider_geo_exact', 'agency_only', 'agency_county_bridged',
        'agency_place_bridged', 'ambiguous', 'unsupported'
    )),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (
        value_status IN ('reported', 'not_reported')
    ),
    population_denominator NUMERIC,
    source_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index BIGINT NOT NULL,
    transformation_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (product_id, release_key)
        REFERENCES silver_fbi.dim_ucr_dataset_release(product_id, release_key),
    FOREIGN KEY (product_id, measure_id)
        REFERENCES silver_fbi.dim_offense_measure(product_id, measure_id),
    -- Every observation must resolve to a coverage interpretation; a value
    -- without its participation companion cannot be published.
    FOREIGN KEY (product_id, release_key, subject_type, subject_code, period)
        REFERENCES silver_fbi.fact_reporting_participation(
            product_id, release_key, subject_type, subject_code, period
        ),
    UNIQUE (product_id, release_key, source_record_id),
    CHECK (value_status <> 'reported' OR value IS NOT NULL),
    CHECK (value_status <> 'not_reported' OR value IS NULL)
);
CREATE INDEX IF NOT EXISTS fbi_fact_measure_subject_period_idx
    ON silver_fbi.fact_crime_observation (
        product_id, measure_id, subject_type, subject_code, period
    );
CREATE INDEX IF NOT EXISTS fbi_fact_capture_idx
    ON silver_fbi.fact_crime_observation (capture_id, source_row_index);

-- ---------------------------------------------------------------------------
-- Gold publication views
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW gold_fbi.crime_observation AS
SELECT fact.observation_sk, fact.product_id, release.offense_code,
       release.offense_label, release.ucr_program, fact.release_key,
       release.refresh_date, release.max_data_month, fact.measure_id,
       measure.measure_form, measure.counted_entity_basis, measure.unit,
       measure.reported_status, fact.subject_type, fact.subject_code,
       fact.subject_label, fact.source_geo_level, fact.period,
       fact.period_start, fact.period_end, fact.geo_id, fact.geo_sk,
       fact.geography_status,
       CASE fact.subject_type
           WHEN 'national' THEN 'provider-published national total'
           WHEN 'state' THEN 'provider-published state total'
           ELSE 'agency-reported for one law-enforcement agency'
       END AS geography_basis,
       fact.value_source, fact.value, fact.value_status,
       fact.population_denominator,
       coverage.population, coverage.participated_population,
       coverage.coverage_percent, coverage.coverage_basis,
       coverage.participation_status, release.counted_entity_note,
       release.methodology_url, release.documentation_url,
       fact.source_record_id, fact.capture_id
FROM silver_fbi.fact_crime_observation AS fact
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = fact.product_id
 AND release.release_key = fact.release_key
JOIN silver_fbi.dim_offense_measure AS measure
  ON measure.product_id = fact.product_id
 AND measure.measure_id = fact.measure_id
JOIN silver_fbi.fact_reporting_participation AS coverage
  ON coverage.product_id = fact.product_id
 AND coverage.release_key = fact.release_key
 AND coverage.subject_type = fact.subject_type
 AND coverage.subject_code = fact.subject_code
 AND coverage.period = fact.period
WHERE release.status = 'published'
  AND fact.geography_status NOT IN ('ambiguous', 'unsupported');

CREATE OR REPLACE VIEW gold_fbi.reporting_coverage AS
SELECT coverage.participation_sk, coverage.product_id, coverage.release_key,
       coverage.ucr_program, coverage.subject_type, coverage.subject_code,
       coverage.subject_label, coverage.source_geo_level, coverage.period,
       coverage.period_start, coverage.period_end, coverage.geo_id,
       coverage.geo_sk, coverage.geography_status, coverage.population,
       coverage.participated_population, coverage.coverage_percent,
       coverage.coverage_basis, coverage.participation_status,
       release.refresh_date, release.methodology_url, coverage.capture_id
FROM silver_fbi.fact_reporting_participation AS coverage
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = coverage.product_id
 AND release.release_key = coverage.release_key
WHERE release.status = 'published';

CREATE OR REPLACE VIEW gold_fbi.agency_geography AS
SELECT relationship.relationship_sk, relationship.ori, agency.state_code,
       version.agency_name, version.agency_type, version.county_labels,
       relationship.relationship_type, relationship.source_label,
       relationship.geo_id, relationship.geo_sk,
       relationship.resolution_method, relationship.resolution_status,
       relationship.confidence_class, relationship.reason_code,
       relationship.effective_start, relationship.effective_end,
       relationship.geography_vintage, relationship.evidence_source,
       relationship.evidence_capture_id, relationship.release_key,
       agency.geo_sk AS agency_geo_sk
FROM silver_fbi.agency_geography_relationship AS relationship
JOIN silver_fbi.dim_agency AS agency USING (ori)
JOIN silver_fbi.dim_agency_version AS version
  ON version.ori = relationship.ori
 AND version.release_key = relationship.release_key
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = relationship.product_id
 AND release.release_key = relationship.release_key
WHERE release.status = 'published';

-- A county or place filter selects agency observations; it never sums them
-- into an area total. The agency observation identity is carried through so a
-- multi-county agency deduplicates by observation rather than multiplying.
CREATE OR REPLACE VIEW gold_fbi.agency_observation_area_filter AS
SELECT observation.observation_sk, observation.product_id,
       observation.release_key, observation.measure_id,
       observation.measure_form, observation.counted_entity_basis,
       observation.unit, observation.subject_code AS ori,
       observation.subject_label AS agency_name, observation.period,
       observation.period_start, observation.period_end, observation.value,
       observation.value_status, observation.participation_status,
       observation.coverage_percent,
       relationship.relationship_type AS filter_geography_type,
       relationship.geo_id AS filter_geo_id,
       relationship.geo_sk AS filter_geo_sk,
       relationship.source_label AS filter_source_label,
       relationship.confidence_class AS filter_confidence_class,
       CASE relationship.relationship_type
           WHEN 'county' THEN
               'agency-reported for agencies associated with this county'
           WHEN 'place' THEN
               'agency-reported for agencies mapped to this place'
           ELSE 'agency-reported for agencies associated with this state'
       END AS result_label,
       'agency' AS observation_grain
FROM gold_fbi.crime_observation AS observation
JOIN silver_fbi.agency_geography_relationship AS relationship
  ON relationship.ori = observation.subject_code
 AND relationship.product_id = observation.product_id
 -- The relationship is effective-dated, so a filter follows the observation's
 -- period rather than the release it was last confirmed in.
 AND observation.period_start >= relationship.effective_start
 AND observation.period_end <= relationship.effective_end
WHERE observation.subject_type = 'agency'
  AND relationship.resolution_status = 'resolved';

CREATE OR REPLACE VIEW gold_fbi.latest_release_observation AS
SELECT observation.*
FROM gold_fbi.crime_observation AS observation
JOIN (
    SELECT product_id, MAX(refresh_date) AS refresh_date
    FROM silver_fbi.dim_ucr_dataset_release
    WHERE status = 'published'
    GROUP BY product_id
) AS latest
  ON latest.product_id = observation.product_id
 AND latest.refresh_date = observation.refresh_date;

CREATE OR REPLACE VIEW gold_fbi.measure_export AS
SELECT measure.product_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.offense_code AS source_offense_code,
       measure.offense_label AS display_name,
       measure.ucr_program, measure.measure_form,
       measure.counted_entity_basis, measure.unit, measure.reported_status,
       release.release_key AS source_watermark,
       release.methodology_url, release.counted_entity_note,
       release.parser_contract_version AS schema_version
FROM silver_fbi.dim_offense_measure AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_fbi.dim_ucr_dataset_release AS candidate
    WHERE candidate.product_id = measure.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.refresh_date DESC
    LIMIT 1
) AS release ON TRUE;

CREATE OR REPLACE VIEW gold_fbi.metric_publisher AS
SELECT 'FBI_UCR'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.product_id || ':' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.offense_label || ' ' || measure.counted_entity_basis || ' ('
        || measure.measure_form || ')')::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       ARRAY_AGG(DISTINCT UPPER(fact.subject_type)
                 ORDER BY UPPER(fact.subject_type))::TEXT[] AS valid_geo_grains,
       ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
       CASE WHEN measure.measure_form = 'absolute_total'
            THEN 'additive_within_subject'
            ELSE 'non_additive' END::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_fbi',
           'relation', 'crime_observation',
           'product_id', measure.product_id,
           'measure_id', measure.measure_id
       ) AS physical_lineage,
       release.release_key::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Federal Bureau of Investigation Uniform Crime Reporting Program'::TEXT
           AS source_name,
       'government-law-enforcement'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM silver_fbi.dim_offense_measure AS measure
JOIN silver_fbi.fact_crime_observation AS fact
  ON fact.product_id = measure.product_id
 AND fact.measure_id = measure.measure_id
JOIN silver_fbi.dim_ucr_dataset_release AS release
  ON release.product_id = fact.product_id
 AND release.release_key = fact.release_key
WHERE release.status = 'published'
GROUP BY measure.product_id, measure.measure_id, measure.offense_label,
         measure.counted_entity_basis, measure.measure_form, measure.unit,
         release.release_key, release.source_run_id, release.published_at,
         release.methodology_url;

COMMENT ON SCHEMA silver_fbi IS
    'Source-faithful FBI UCR release history, agency reference, and reconciliation.';
COMMENT ON SCHEMA gold_fbi IS
    'Policy-free publication views for validated FBI UCR observations and coverage.';
COMMENT ON COLUMN silver_fbi.observation_revision.source_record IS
    'Exact provider series, period, and value text; a missing month is never a zero.';
COMMENT ON COLUMN silver_fbi.agency_revision.county_labels IS
    'Provider county-name evidence only; never a canonical county code or a rollup key.';
COMMENT ON VIEW gold_fbi.agency_observation_area_filter IS
    'County/place filters over agency-grain observations; never a county or city total.';
COMMENT ON VIEW gold_fbi.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';
