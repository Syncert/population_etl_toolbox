-- Seed for the frontend live-stack smoke tier: one measure per served source.
--
-- What the tier does with this seed. `live-stack.smoke.test.js` discovers the
-- deployment's sources from `/catalog/capabilities`, asks each one's catalog
-- for its current metrics, and requires every one of them to answer rows
-- through whichever access shape the explorer's own
-- `buildLatestObservationRequest` selects. Against this seed every source
-- resolves to the neutral `/observations` resource, because every source
-- declares it; a source falling back to its source-scoped `latest` pair is
-- legitimate but is now the exception, and a silent drift back to it is one
-- of the things the tier watches for. What it is checking is the client's
-- reading of a real deployment, which no fixture can establish.
--
-- Why this file grew. It used to publish exactly one ACS measure for one
-- county, so that loop iterated one metric of one source and the tier's own
-- summary line -- "every active catalog metric answers" -- described a check
-- over a seventh of the surface it names. Six of the seven registered sources
-- could have stopped answering entirely with this tier green: the neutral
-- dispatch's three identity strategies, the stratified envelopes, and the
-- grain vocabularies of CDC, FBI UCR and USDA NASS were all unexercised.
--
-- Three rules this file keeps, each of which has a way of going wrong:
--
-- 1. THE CATALOG ROWS ARE NOT WRITTEN HERE. Every one is selected from the
--    source's own `gold_<source>.metric_publisher` view at the bottom of this
--    file, composed exactly as `glossary/harvest.py` composes it
--    (`source_code || ':' || source_object_key`). A hand-written catalog row
--    carries a hand-written `physical_lineage`, and that is the field the
--    neutral resource resolves a metric's serving rows through: a fixture
--    spelling it independently of the publisher keeps testing whatever shape
--    was true the day it was written, which is the class of drift ARC-005 and
--    DB-034 both ended. Seed the data; let the publisher say what it means.
--
-- 2. ONLY A SOURCE WITH NO COUNTY MAY DECLARE THE `STATE` GRAIN. `spatialGrains` returns
--    ['STATE', 'COUNTY'] in that order -- the tile layer publishes both
--    attribution fields -- and the tier's tile-join test walks those grains
--    and picks the first source publishing any metric at one. The Martin seed
--    draws a single county polygon (Dane County, `state:55|county:025`) and no
--    state, so a measure advertising STATE would be chosen, decode zero
--    features, and fail a test about geography joins for a reason that is
--    purely about this seed. County-grain measures here all use that county's
--    `geo_id` for the same reason: the join must be on a real shared
--    geography, not on two fixtures agreeing.
--    The one exception is EIA, which publishes no county at all: Wisconsin is
--    seeded as a state with a polygon around the county (below), so its
--    state row joins a real STATE feature.
--
-- 3. EVERY VALUE IS DETERMINISTIC. Fixed uuids, fixed far-future dates, no
--    `NOW()`. DB-039 records why: a `NOW()` in this file made the seed encode
--    a release date unrelated to the row's ingestion, a state the refresh can
--    no longer produce, and made two runs of the same seed differ.
--
-- The far-future period (2094-2098) keeps these rows sorting after anything a
-- real ingestion could publish into the same relations.

-- ---------------------------------------------------------------------------
-- Shared: the geography identity, and one capture graph per source.
-- ---------------------------------------------------------------------------

-- The Martin seed publishes Dane County into `gold_glossary.dim_geo_latest`
-- but not into the silver identity, and Census PEP's fact table requires a
-- resolved row to carry a real `geo_sk`. Seeded here so the one county this
-- deployment can draw is a geography every layer agrees exists.
INSERT INTO silver_ref.dim_geo_entity (
    geo_id, geo_type, census_geoid, state_fips, county_fips,
    first_seen_version, last_seen_version
) VALUES (
    'state:55|county:025', 'county', '55025', '55', '025', 2098, 2098
) ON CONFLICT (geo_id) DO NOTHING;

-- Wisconsin itself, for a source that publishes no county (EIA's weekly
-- gasoline prices are by state, PADD, city and nation). The polygon is the
-- county's box grown a little, so the state contains the county and the tile
-- layer has a STATE feature to join a state-grain measure against.
INSERT INTO silver_ref.dim_geo_entity (
    geo_id, geo_type, census_geoid, state_fips, first_seen_version,
    last_seen_version
) VALUES (
    'state:55', 'state', '55', '55', 2098, 2098
) ON CONFLICT (geo_id) DO NOTHING;

INSERT INTO gold_glossary.dim_geo_latest (
    geo_id, geo_level, state_fips, state_name, latitude, longitude, geo_geom
) VALUES (
    'state:55', 'STATE', '55', 'Wisconsin', 44.5, -89.5,
    ST_Multi(ST_GeomFromText(
        'POLYGON((-90.00 42.50,-88.80 42.50,-88.80 43.60,-90.00 43.60,-90.00 42.50))',
        4326
    ))
) ON CONFLICT (geo_id) DO NOTHING;

-- One run/request/capture chain per source that records provenance on its
-- facts. The uuids are fixed and obviously synthetic: every capture-first
-- relation below carries a foreign key into this graph, and a fixture that
-- invented a run id would be citing an ingestion that never happened.
INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES
    ('00000000-0000-4000-8000-000000000cdc', 'CDC', 'success'),
    ('00000000-0000-4000-8000-000000000fb1', 'FBI_UCR', 'success'),
    ('00000000-0000-4000-8000-000000000a55', 'USDA_NASS', 'success'),
    ('00000000-0000-4000-8000-000000000e70', 'CENSUS_PEP', 'success'),
    ('00000000-0000-4000-8000-000000000bea', 'BEA', 'success'),
    ('00000000-0000-4000-8000-000000000b1c', 'BLS_QCEW', 'success'),
    ('00000000-0000-4000-8000-000000000b95', 'CENSUS_BPS', 'success'),
    ('00000000-0000-4000-8000-0000000005ae', 'CENSUS_SAIPE_SAHIE', 'success'),
    ('00000000-0000-4000-8000-00000000015c', 'IRS_MIGRATION', 'success'),
    ('00000000-0000-4000-8000-000000000e1a', 'EIA', 'success'),
    ('00000000-0000-4000-8000-000000000cb9', 'CENSUS_CBP', 'success'),
    ('00000000-0000-4000-8000-00000000010d', 'CENSUS_LODES', 'success'),
    ('00000000-0000-4000-8000-000000000e9a', 'EPA_AQS', 'success'),
    ('00000000-0000-4000-8000-0000000000aa', 'NOAA_NORMALS', 'success'),
    ('00000000-0000-4000-8000-000000000fcc', 'FCC_BDC', 'success'),
    ('00000000-0000-4000-8000-000000000fe1', 'FEMA_NRI', 'success'),
    ('00000000-0000-4000-8000-000000000f4f', 'FHFA_HPI', 'success'),
    ('00000000-0000-4000-8000-000000000d0d', 'HUD_FMR_IL', 'success')
ON CONFLICT (run_id) DO NOTHING;

INSERT INTO raw_capture.payload_blob (payload_checksum, payload, payload_size)
VALUES ('44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', '\x7b7d'::BYTEA, 2)
ON CONFLICT (payload_checksum) DO NOTHING;

INSERT INTO control.ingestion_request (
    request_id, run_id, source_code, endpoint, request_parameters,
    request_fingerprint, status
) VALUES
    ('00000000-0000-4000-9000-000000000cdc', '00000000-0000-4000-8000-000000000cdc',
     'CDC', 'smoke://seed', '{}'::JSONB, 'a563afccf114b55950d2e24f6b196d55d0e63fb8937625f6f87c4e29e07a1d24', 'captured'),
    ('00000000-0000-4000-9000-000000000fb1', '00000000-0000-4000-8000-000000000fb1',
     'FBI_UCR', 'smoke://seed', '{}'::JSONB, '07735be126dc5d37c8091f510a48748544218be74b194484d721a4869d23654c', 'captured'),
    ('00000000-0000-4000-9000-000000000a55', '00000000-0000-4000-8000-000000000a55',
     'USDA_NASS', 'smoke://seed', '{}'::JSONB, 'd3210f3ccecd2dd1a0473c07b25b34fb0715c0654b7fc12a96768d3017719090', 'captured'),
    ('00000000-0000-4000-9000-000000000e70', '00000000-0000-4000-8000-000000000e70',
     'CENSUS_PEP', 'smoke://seed', '{}'::JSONB, '3f45d5d8b3eb1261ea67453de9821d7207c2c93db3965bb54a9f853a0073015a', 'captured'),
    ('00000000-0000-4000-9000-000000000bea', '00000000-0000-4000-8000-000000000bea',
     'BEA', 'smoke://seed', '{}'::JSONB, '70021e6443a882267a777f4fcb288121072a28e3c3fb1c2d11b25b4921ae167f', 'captured'),
    ('00000000-0000-4000-9000-000000000b1c', '00000000-0000-4000-8000-000000000b1c',
     'BLS_QCEW', 'smoke://seed', '{}'::JSONB, '172bff3668e0d4c3a201e34b7ea1d7340e9fb3d6c6135fef6410bdf9093d7afc', 'captured'),
    ('00000000-0000-4000-9000-000000000b95', '00000000-0000-4000-8000-000000000b95',
     'CENSUS_BPS', 'smoke://seed', '{}'::JSONB, '30627877c2451639215808defa3a3b0c4cd694c585d689849cb4043a7f6d9ed6', 'captured'),
    ('00000000-0000-4000-9000-0000000005ae', '00000000-0000-4000-8000-0000000005ae',
     'CENSUS_SAIPE_SAHIE', 'smoke://seed', '{}'::JSONB, 'd18e86b829892741b9042e06f5760490a423e7d1e3e447a145d821a0405f9d9c', 'captured'),
    ('00000000-0000-4000-9000-00000000015c', '00000000-0000-4000-8000-00000000015c',
     'IRS_MIGRATION', 'smoke://seed', '{}'::JSONB, 'aedef3c58968656b3f72cff6b2b82c475e7a2b68115b0693f74b09ef1eebb376', 'captured'),
    ('00000000-0000-4000-9000-000000000e1a', '00000000-0000-4000-8000-000000000e1a',
     'EIA', 'smoke://seed', '{}'::JSONB, '8447b6d38eee7a283fe6b223ff8dcb0db2edfab3a68f1ad98688d41cb6b5761c', 'captured'),
    ('00000000-0000-4000-9000-000000000cb9', '00000000-0000-4000-8000-000000000cb9',
     'CENSUS_CBP', 'smoke://seed', '{}'::JSONB, '144b212f4b341dba2dbac560053bbe034400e22d73b0645bf57ba10c2412bfd1', 'captured'),
    ('00000000-0000-4000-9000-00000000010d', '00000000-0000-4000-8000-00000000010d',
     'CENSUS_LODES', 'smoke://seed', '{}'::JSONB, '233c3cf1bc8f809c7efc13a8effbc7e6731dbb1ed773de6f7b20cfb7a7a6734e', 'captured'),
    ('00000000-0000-4000-9000-000000000e9a', '00000000-0000-4000-8000-000000000e9a',
     'EPA_AQS', 'smoke://seed', '{}'::JSONB, '867caaa54cc939c28f650c85e03bac20cd781d68037f65698b6061b0377a927f', 'captured'),
    ('00000000-0000-4000-9000-0000000000aa', '00000000-0000-4000-8000-0000000000aa',
     'NOAA_NORMALS', 'smoke://seed', '{}'::JSONB, '704e1b270076137d346303215f65e90f673f32938b0996e07c4b80f3f8d4acc4', 'captured'),
    ('00000000-0000-4000-9000-000000000fcc', '00000000-0000-4000-8000-000000000fcc',
     'FCC_BDC', 'smoke://seed', '{}'::JSONB, '8639ff5d5f7518dea854d19fc48d45e0cc1ced0bb63e230b2bf29fe6af0add6a', 'captured'),
    ('00000000-0000-4000-9000-000000000fe1', '00000000-0000-4000-8000-000000000fe1',
     'FEMA_NRI', 'smoke://seed', '{}'::JSONB, '3905b3189883473e55eb8e3576335c6c9f28854bb5b3ac4123168dcb3ec070c7', 'captured'),
    ('00000000-0000-4000-9000-000000000f4f', '00000000-0000-4000-8000-000000000f4f',
     'FHFA_HPI', 'smoke://seed', '{}'::JSONB, 'f2e7ee8ee51eca3509f4cfeb8fe40010dfd40869385e7de053b889d337e0c3cb', 'captured'),
    ('00000000-0000-4000-9000-000000000d0d', '00000000-0000-4000-8000-000000000d0d',
     'HUD_FMR_IL', 'smoke://seed', '{}'::JSONB, '866992fcfcf7a06d7c71f9bce906dfd29eb07ffecdaa0ffad9632968d455b120', 'captured')
ON CONFLICT (request_id) DO NOTHING;

INSERT INTO raw_capture.response_capture (
    capture_id, request_id, run_id, source_code, endpoint, request_parameters,
    request_fingerprint, retrieved_at, http_status, response_headers,
    media_type, payload_checksum
) VALUES
    ('00000000-0000-4000-a000-000000000cdc', '00000000-0000-4000-9000-000000000cdc',
     '00000000-0000-4000-8000-000000000cdc', 'CDC', 'smoke://seed', '{}'::JSONB,
     'a563afccf114b55950d2e24f6b196d55d0e63fb8937625f6f87c4e29e07a1d24', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000fb1', '00000000-0000-4000-9000-000000000fb1',
     '00000000-0000-4000-8000-000000000fb1', 'FBI_UCR', 'smoke://seed', '{}'::JSONB,
     '07735be126dc5d37c8091f510a48748544218be74b194484d721a4869d23654c', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000a55', '00000000-0000-4000-9000-000000000a55',
     '00000000-0000-4000-8000-000000000a55', 'USDA_NASS', 'smoke://seed', '{}'::JSONB,
     'd3210f3ccecd2dd1a0473c07b25b34fb0715c0654b7fc12a96768d3017719090', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000e70', '00000000-0000-4000-9000-000000000e70',
     '00000000-0000-4000-8000-000000000e70', 'CENSUS_PEP', 'smoke://seed', '{}'::JSONB,
     '3f45d5d8b3eb1261ea67453de9821d7207c2c93db3965bb54a9f853a0073015a', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000bea', '00000000-0000-4000-9000-000000000bea',
     '00000000-0000-4000-8000-000000000bea', 'BEA', 'smoke://seed', '{}'::JSONB,
     '70021e6443a882267a777f4fcb288121072a28e3c3fb1c2d11b25b4921ae167f', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000b1c', '00000000-0000-4000-9000-000000000b1c',
     '00000000-0000-4000-8000-000000000b1c', 'BLS_QCEW', 'smoke://seed', '{}'::JSONB,
     '172bff3668e0d4c3a201e34b7ea1d7340e9fb3d6c6135fef6410bdf9093d7afc', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000b95', '00000000-0000-4000-9000-000000000b95',
     '00000000-0000-4000-8000-000000000b95', 'CENSUS_BPS', 'smoke://seed', '{}'::JSONB,
     '30627877c2451639215808defa3a3b0c4cd694c585d689849cb4043a7f6d9ed6', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-0000000005ae', '00000000-0000-4000-9000-0000000005ae',
     '00000000-0000-4000-8000-0000000005ae', 'CENSUS_SAIPE_SAHIE', 'smoke://seed', '{}'::JSONB,
     'd18e86b829892741b9042e06f5760490a423e7d1e3e447a145d821a0405f9d9c', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-00000000015c', '00000000-0000-4000-9000-00000000015c',
     '00000000-0000-4000-8000-00000000015c', 'IRS_MIGRATION', 'smoke://seed', '{}'::JSONB,
     'aedef3c58968656b3f72cff6b2b82c475e7a2b68115b0693f74b09ef1eebb376', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000e1a', '00000000-0000-4000-9000-000000000e1a',
     '00000000-0000-4000-8000-000000000e1a', 'EIA', 'smoke://seed', '{}'::JSONB,
     '8447b6d38eee7a283fe6b223ff8dcb0db2edfab3a68f1ad98688d41cb6b5761c', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000cb9', '00000000-0000-4000-9000-000000000cb9',
     '00000000-0000-4000-8000-000000000cb9', 'CENSUS_CBP', 'smoke://seed', '{}'::JSONB,
     '144b212f4b341dba2dbac560053bbe034400e22d73b0645bf57ba10c2412bfd1', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-00000000010d', '00000000-0000-4000-9000-00000000010d',
     '00000000-0000-4000-8000-00000000010d', 'CENSUS_LODES', 'smoke://seed', '{}'::JSONB,
     '233c3cf1bc8f809c7efc13a8effbc7e6731dbb1ed773de6f7b20cfb7a7a6734e', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000e9a', '00000000-0000-4000-9000-000000000e9a',
     '00000000-0000-4000-8000-000000000e9a', 'EPA_AQS', 'smoke://seed', '{}'::JSONB,
     '867caaa54cc939c28f650c85e03bac20cd781d68037f65698b6061b0377a927f', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-0000000000aa', '00000000-0000-4000-9000-0000000000aa',
     '00000000-0000-4000-8000-0000000000aa', 'NOAA_NORMALS', 'smoke://seed', '{}'::JSONB,
     '704e1b270076137d346303215f65e90f673f32938b0996e07c4b80f3f8d4acc4', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000fcc', '00000000-0000-4000-9000-000000000fcc',
     '00000000-0000-4000-8000-000000000fcc', 'FCC_BDC', 'smoke://seed', '{}'::JSONB,
     '8639ff5d5f7518dea854d19fc48d45e0cc1ced0bb63e230b2bf29fe6af0add6a', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000fe1', '00000000-0000-4000-9000-000000000fe1',
     '00000000-0000-4000-8000-000000000fe1', 'FEMA_NRI', 'smoke://seed', '{}'::JSONB,
     '3905b3189883473e55eb8e3576335c6c9f28854bb5b3ac4123168dcb3ec070c7', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000f4f', '00000000-0000-4000-9000-000000000f4f',
     '00000000-0000-4000-8000-000000000f4f', 'FHFA_HPI', 'smoke://seed', '{}'::JSONB,
     'f2e7ee8ee51eca3509f4cfeb8fe40010dfd40869385e7de053b889d337e0c3cb', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000d0d', '00000000-0000-4000-9000-000000000d0d',
     '00000000-0000-4000-8000-000000000d0d', 'HUD_FMR_IL', 'smoke://seed', '{}'::JSONB,
     '866992fcfcf7a06d7c71f9bce906dfd29eb07ffecdaa0ffad9632968d455b120', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')
ON CONFLICT (capture_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census ACS -- the survey family: dataset/vintage identity and a margin of error.
-- ---------------------------------------------------------------------------

INSERT INTO gold_census.dim_acs_table (
    dataset_code, vintage_year, table_id, table_title, survey_span_years
) VALUES (
    'acs5', 2098, 'B01003', 'Total population (smoke fixture)', 5
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.dim_acs_variable (
    acs_table_sk, dataset_code, vintage_year, variable_code, variable_label,
    value_role
)
SELECT acs_table_sk, 'acs5', 2098, 'B01003_001_SMOKE',
       'Total population (smoke fixture)', 'ESTIMATE'
FROM gold_census.dim_acs_table
WHERE dataset_code = 'acs5' AND vintage_year = 2098 AND table_id = 'B01003'
ON CONFLICT DO NOTHING;

-- The serving row, keyed on the code the publisher composes for the variable
-- above. The geography is the county the Martin seed draws, so a discovered
-- tile layer and a served observation join on a real shared geo_id.
INSERT INTO gold_census.rpt_acs_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude, value,
    dataset_code, vintage_year, table_id, variable_code, estimate_value,
    value_type, units, metric_code, metric_display_name
) VALUES (
    'CENSUS_ACS', '2098-01-01', '2094-01-01', '2098-12-31', 20980101,
    -- `as_of_date` and `updated_at` are one fact about this row, not
    -- two: the serving refresh derives the release date from the silver
    -- row's `ingested_at`, which is what `updated_at` publishes (DB-039).
    '2098-12-31', '2098-12-31 00:00:00+00', 'state:55|county:025', 'COUNTY', '55', '025',
    'Wisconsin', 'Dane County', 43.0667, -89.4000, 561504,
    'acs5', 2098, 'B01003', 'B01003_001_SMOKE', 561504,
    'ESTIMATE', 'people', 'CENSUS_ACS:acs5:B01003_001_SMOKE',
    'Total population (smoke fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.mv_acs_latest
SELECT * FROM gold_census.rpt_acs_observations
WHERE metric_code = 'CENSUS_ACS:acs5:B01003_001_SMOKE'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FRED -- the national macro family: one series, no geography dimension.
-- ---------------------------------------------------------------------------

INSERT INTO gold_fred.dim_fred_series (
    series_id, series_title, units, frequency, seasonal_adjustment,
    reference_url, updated_at
) VALUES (
    'SMOKE_UNRATE', 'Unemployment rate (smoke fixture)', 'Percent', 'Monthly',
    'Seasonally Adjusted', 'https://fred.stlouisfed.org/series/SMOKE_UNRATE',
    '2098-12-31 00:00:00+00'
) ON CONFLICT (series_id) DO NOTHING;

INSERT INTO gold_fred.rpt_fred_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, series_id, series_title,
    value, units, frequency, seasonal_adjustment_status, metric_code,
    metric_display_name
) VALUES (
    'FRED', '2098-01-01', '2098-01-01', '2098-01-31', 20980101,
    '2098-12-31', '2098-12-31 00:00:00+00', 'us:1', 'NATIONAL',
    'SMOKE_UNRATE', 'Unemployment rate (smoke fixture)', 4.2, 'Percent',
    'Monthly', 'Seasonally Adjusted', 'FRED:SMOKE_UNRATE',
    'Unemployment rate (smoke fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_fred.mv_fred_latest
SELECT * FROM gold_fred.rpt_fred_observations
WHERE metric_code = 'FRED:SMOKE_UNRATE'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- BLS -- the labour family: a survey/series identity carrying seasonal adjustment.
--
-- `gold_bls.metric_publisher` publishes a series only where no measure
-- identity claims its program (the LAUS measure-identity migration, ETL-047).
-- `gold_bls.dim_bls_measure` is empty in this deployment, so the series
-- branch is the one that publishes here -- which is also the branch the
-- source-scoped `/bls/observations/latest` pair reads.
-- ---------------------------------------------------------------------------

INSERT INTO gold_bls.dim_bls_survey (
    program_code, survey_name, observation_basis
) VALUES (
    'SM', 'Smoke survey (fixture)', 'JOBS'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.dim_bls_series (
    bls_survey_sk, program_code, series_id, series_title, measure_category,
    value_type, unit_of_measure, seasonal_adjustment_status
)
SELECT bls_survey_sk, 'SM', 'SMOKE_SERIES_0001',
       'Smoke employment level (fixture)', 'EMPLOYMENT', 'LEVEL', 'persons',
       'Seasonally Adjusted'
FROM gold_bls.dim_bls_survey WHERE program_code = 'SM'
ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.rpt_bls_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, series_id, program_code, series_title, value,
    units, seasonal_adjustment_status, metric_code, metric_display_name
) VALUES (
    'BLS', '2098-01-01', '2098-01-01', '2098-01-31', 20980101,
    '2098-12-31', '2098-12-31 00:00:00+00', 'state:55|county:025', 'COUNTY',
    '55', '025', 'Wisconsin', 'Dane County', 'SMOKE_SERIES_0001', 'SM',
    'Smoke employment level (fixture)', 375000, 'persons',
    'Seasonally Adjusted', 'BLS:SMOKE_SERIES_0001',
    'Smoke employment level (fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.mv_bls_latest
SELECT * FROM gold_bls.rpt_bls_observations
WHERE metric_code = 'BLS:SMOKE_SERIES_0001'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census PEP -- the vintage family, and the `lineage_key_column` identity
-- strategy: the serving revision keys rows by the bare measure code while the
-- catalog publishes a composed one. DB-034 is the defect that gets exercised
-- here -- matching only the lineage key made the route refuse the identity it
-- had just published.
-- ---------------------------------------------------------------------------

INSERT INTO silver_pep.dim_measure (
    metric_code, display_name, unit, is_component, allows_negative
) VALUES (
    'SMOKEPOP', 'Resident population estimate (smoke fixture)', 'persons',
    FALSE, FALSE
) ON CONFLICT (metric_code) DO NOTHING;

INSERT INTO silver_pep.pep_dataset (
    dataset_code, title, transport, geography_levels, summary_levels,
    variable_families, parser_version, release_page_url, decennial_base
) VALUES (
    'pep_smoke', 'Population estimates (smoke fixture)', 'bulk_csv',
    ARRAY['county'], ARRAY['050'], ARRAY['POP'], '1',
    'https://www.census.gov/programs-surveys/popest.html', 2090
) ON CONFLICT (dataset_code) DO NOTHING;

INSERT INTO silver_pep.pep_release (
    dataset_code, vintage_year, product_code, data_url, layout_url,
    release_date, observation_start_year, observation_end_year,
    geography_basis_date, schema_version, status
) VALUES (
    'pep_smoke', 2098, 'alldata',
    'https://www2.census.gov/programs-surveys/popest/smoke/alldata.csv',
    'https://www2.census.gov/programs-surveys/popest/smoke/layout.txt',
    '2098-12-31', 2098, 2098, '2098-01-01', '1', 'published'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_pep.release_load (
    capture_id, dataset_code, release_vintage, product_code,
    source_record_count, observation_count, completeness_status
) VALUES (
    '00000000-0000-4000-a000-000000000e70', 'pep_smoke', 2098, 'alldata',
    1, 1, 'complete'
) ON CONFLICT DO NOTHING;

-- The parsed revision the fact row is a resolved projection of: PEP's
-- as-released surface is the revision, and `gold_pep.population_estimate_revision`
-- reads it, so the fact table keys into it rather than standing alone.
INSERT INTO silver_pep.observation_revision (
    capture_id, source_row_index, source_column_index, source_header,
    dataset_code, release_vintage, product_code, observation_year,
    metric_code, unit, summary_level, state_fips_source, county_fips_source,
    name_source, state_name_source, value_source, value, value_status
) VALUES (
    '00000000-0000-4000-a000-000000000e70', 0, 0, 'POPESTIMATE2098',
    'pep_smoke', 2098, 'alldata', 2098, 'SMOKEPOP', 'persons', '050',
    '55', '025', 'Dane County', 'Wisconsin', '561504', 561504, 'valid'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_pep.fact_population_estimate (
    capture_id, source_row_index, source_column_index, dataset_code,
    release_vintage, product_code, metric_code, observation_year,
    estimate_date, geo_id, geo_sk, geo_type, geography_basis_date,
    resolution_status, summary_level, source_geo_code, value_source, value,
    unit
)
SELECT '00000000-0000-4000-a000-000000000e70', 0, 0, 'pep_smoke', 2098,
       'alldata', 'SMOKEPOP', 2098, '2098-07-01', 'state:55|county:025',
       geo_sk, 'county', '2098-01-01', 'resolved', '050', '55025',
       '561504', 561504, 'persons'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- CDC -- the `identity_columns` strategy (asset/measure/value-type), and the
-- first of the stratified envelopes: rows carry a stratum and an adjustment
-- status that an aligned single-value analysis would collapse.
-- ---------------------------------------------------------------------------

INSERT INTO silver_cdc.dim_dataset_release (
    asset_id, release_watermark, socrata_id, title, methodology_url,
    geography_basis, parser_contract_version, estimate_method,
    population_basis, metadata_capture_id, source_run_id, source_record_count,
    quarantine_count, status, reconciled_at, published_at
) VALUES (
    'cdi', '20980101', 'smok-0001', 'Chronic disease indicators (smoke fixture)',
    'https://www.cdc.gov/cdi', 'county', '1', 'model-based', 'adults',
    '00000000-0000-4000-a000-000000000cdc', '00000000-0000-4000-8000-000000000cdc',
    1, 0, 'published', '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_cdc.dim_measure (
    asset_id, measure_id, value_type_id, measure_label, topic,
    value_type_label, unit, adjustment_status, estimate_method,
    population_basis
) VALUES (
    'cdi', 'SMOKE_CDI_01', 'crude', 'Smoke health indicator (fixture)',
    'Smoke topic', 'Crude prevalence', 'percent', 'crude', 'model-based',
    'adults'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_cdc.dim_stratum (stratum_id, strata) VALUES (
    '23deed5745968020bb282f7742ddab6a9bed872684e14f8983d89b02dc203ad7', '[["OVERALL", "Overall", "OVR", "Overall"]]'::JSONB
) ON CONFLICT (stratum_id) DO NOTHING;

INSERT INTO silver_cdc.fact_health_observation (
    asset_id, release_watermark, source_record_id, source_run_id, capture_id,
    source_row_index, measure_id, value_type_id, stratum_id, period_start,
    period_end, geo_id, geo_sk, geo_type, geography_status, value_source,
    value, value_status, unit, adjustment_status, estimate_method,
    population_basis, transformation_version
)
SELECT 'cdi', '20980101', 'e542fa20911eb14e6a74ce7bc00bc84ee2834abff9d2ccf4ef0a5d99e6aa1069',
       '00000000-0000-4000-8000-000000000cdc',
       '00000000-0000-4000-a000-000000000cdc', 0, 'SMOKE_CDI_01', 'crude',
       '23deed5745968020bb282f7742ddab6a9bed872684e14f8983d89b02dc203ad7', 2098, 2098, 'state:55|county:025',
       geo_sk, 'county', 'resolved', '12.5', 12.5, 'valid', 'percent',
       'crude', 'model-based', 'adults', '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FBI UCR -- the participation family. Its rows carry a reporting basis this
-- API deliberately does not flatten into the source-scoped row shape, so the
-- neutral `/observations` resource is its only observation surface: exactly
-- the access-shape choice this tier exists to check the client makes.
--
-- `subject_type` is the grain expression (`UPPER(subject_type)`), so `county`
-- here publishes the COUNTY grain and joins the drawn boundary.
-- ---------------------------------------------------------------------------

INSERT INTO silver_fbi.dim_ucr_dataset_release (
    product_id, release_key, refresh_date, max_data_month, ucr_program,
    offense_code, offense_label, period_start, period_end, documentation_url,
    methodology_url, parser_contract_version, reported_status,
    counted_entity_note, release_capture_id, source_run_id,
    source_record_count, quarantine_count, status, reconciled_at, published_at
) VALUES (
    'estimates', '2098-12-31', '2098-12-31', '2098-12', 'summary',
    'V', 'Violent crime (smoke fixture)', '2098-01-01', '2098-12-31',
    'https://cde.ucr.cjis.gov/', 'https://cde.ucr.cjis.gov/methodology', '1',
    'reported', 'Counted offences, smoke fixture',
    '00000000-0000-4000-a000-000000000fb1',
    '00000000-0000-4000-8000-000000000fb1', 1, 0, 'published',
    '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_fbi.dim_offense_measure (
    product_id, measure_id, ucr_program, offense_code, offense_label,
    measure_form, counted_entity_basis, unit, reported_status
) VALUES (
    'estimates', 'SMOKE_V_COUNT', 'summary', 'V',
    'Violent crime (smoke fixture)', 'count', 'offense', 'offenses',
    'reported'
) ON CONFLICT DO NOTHING;

-- The participation basis every crime observation is keyed to. FBI UCR
-- counts what reporting agencies submitted, so a value without its coverage
-- is not a fact this warehouse will store: the fact table's foreign key says
-- so, and the neutral envelope publishes the coverage beside the value.
INSERT INTO silver_fbi.fact_reporting_participation (
    product_id, release_key, ucr_program, subject_type, subject_code,
    subject_label, source_geo_level, period, period_start, period_end,
    geo_id, geo_sk, geography_status, population, participated_population,
    coverage_percent, coverage_basis, participation_status, source_run_id,
    capture_id, source_row_index, transformation_version
)
SELECT 'estimates', '2098-12-31', 'summary', 'county', '55025',
       'Dane County', 'county', '2098', '2098-01-01', '2098-12-31',
       'state:55|county:025', geo_sk, 'provider_geo_exact', 561504, 561504,
       100.0, 'agency_reported_months', 'full_year',
       '00000000-0000-4000-8000-000000000fb1',
       '00000000-0000-4000-a000-000000000fb1', 0, '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

INSERT INTO silver_fbi.fact_crime_observation (
    product_id, release_key, source_record_id, measure_id, subject_type,
    subject_code, subject_label, source_geo_level, geo_id, geo_sk, period,
    period_start, period_end, geography_status, value_source, value,
    value_status, source_run_id, capture_id, source_row_index,
    transformation_version
)
SELECT 'estimates', '2098-12-31', 'ccf93d8d198bb4d68f2264e0624fefe83b6511b82169556899eb68b7eb049690', 'SMOKE_V_COUNT',
       'county', '55025', 'Dane County', 'county', 'state:55|county:025',
       geo_sk, '2098', '2098-01-01', '2098-12-31', 'provider_geo_exact',
       '1234', 1234, 'reported', '00000000-0000-4000-8000-000000000fb1',
       '00000000-0000-4000-a000-000000000fb1', 0, '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- USDA NASS -- the widest `identity_columns` tuple and its own grain
-- vocabulary (`UPPER(agg_level_desc)`), which published NATION from one column
-- while filtering on another whose word is NATIONAL (DB-028). COUNTY here, so
-- the grain the catalog advertises is one the tier can send straight back.
-- ---------------------------------------------------------------------------

INSERT INTO silver_nass.dim_dataset_release (
    product_id, release_watermark, label, source_desc, slice_mode,
    methodology_url, parser_contract_version, incremental_field,
    release_expectation, registered_years, source_run_id, source_record_count,
    quarantine_count, slice_count, status, reconciled_at, published_at
) VALUES (
    'crops', '2098', 'Crops (smoke fixture)', 'SURVEY', 'full',
    'https://www.nass.usda.gov/', '1', 'year', 'annual',
    '[2098]'::JSONB, '00000000-0000-4000-8000-000000000a55', 1, 0, 1,
    'published', '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_commodity (
    commodity_sk, sector_desc, group_desc, commodity_desc, class_desc,
    prodn_practice_desc, util_practice_desc
) VALUES (
    'c5084f7c89d9d1128b54fd91f0d8ac2b0aca74ff2cb2c41c56e4fc7609a15cc4', 'CROPS', 'FIELD CROPS', 'CORN', 'ALL CLASSES',
    'ALL PRODUCTION PRACTICES', 'ALL UTILIZATION PRACTICES'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_domain (domain_sk, domain_desc, domaincat_desc)
VALUES ('f2912ead6f4ce0d029af5662376d793d691cdf6906a4161a103e4a4a983cfd26', 'TOTAL', 'NOT SPECIFIED')
ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_statistic (
    statistic_sk, source_desc, statisticcat_desc, short_desc, unit_desc,
    freq_desc, value_kind, calculation_basis, additive_behavior,
    additive_behavior_known
) VALUES (
    'f41a8794e3844f67adf4c587312401ece4d8538b8f791fba5bdfc2b7fbf0ebeb', 'SURVEY', 'YIELD',
    'CORN, GRAIN - YIELD, MEASURED IN BU / ACRE (SMOKE FIXTURE)',
    'BU / ACRE', 'ANNUAL', 'ratio', 'per-acre', 'non_additive', TRUE
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.fact_crop_observation (
    product_id, release_watermark, source_record_id, source_run_id,
    capture_id, source_row_index, slice_key, commodity_sk, statistic_sk,
    domain_sk, geo_id, geo_sk, geo_type, geography_status, geo_source_code,
    agg_level_desc, location_desc, state_fips, county_fips, year, freq_desc, begin_code, end_code,
    reference_period_desc, value_source, value, value_status, unit_desc,
    cv_source, cv_status, source_desc, transformation_version
)
SELECT 'crops', '2098', 'f65bd325f32b0a4b12726509ce34ce28cdaeb93e0c92d57bba9318c57d1be559',
       '00000000-0000-4000-8000-000000000a55',
       '00000000-0000-4000-a000-000000000a55', 0, '2098', 'c5084f7c89d9d1128b54fd91f0d8ac2b0aca74ff2cb2c41c56e4fc7609a15cc4',
       'f41a8794e3844f67adf4c587312401ece4d8538b8f791fba5bdfc2b7fbf0ebeb', 'f2912ead6f4ce0d029af5662376d793d691cdf6906a4161a103e4a4a983cfd26', 'state:55|county:025', geo_sk, 'county',
       'resolved', '55025', 'COUNTY', 'DANE', '55', '025', 2098, 'ANNUAL', '00', '00',
       'YEAR', '185.4', 185.4, 'valid', 'BU / ACRE', '', 'not_available',
       'SURVEY', '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- BEA regional accounts -- one table line for one county and year, served
-- from views over a published table capture. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.bea_table_capture (
    run_id, table_code, capture_id, member_name, release_date,
    captured_row_count, in_scope_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000bea', 'CAINC1', '00000000-0000-4000-a000-000000000bea', 'CAINC1__ALL_AREAS_SMOKE.csv', '2098-11-15',
    1, 1, 'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_bea.dim_line (
    table_code, line_code, table_title, description, unit, dollar_basis,
    observation_basis, methodology_url, parser_contract_version
) VALUES (
    'CAINC1', '3', 'Personal income summary (smoke fixture)',
    'Per capita personal income', 'Dollars', 'per_capita_current_dollars',
    'place of residence (smoke fixture)',
    'https://www.bea.gov/resources/methodologies/local-area-personal-income',
    'smoke:v1'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_bea.fact_observation (
    table_code, line_code, geo_id, year, capture_id, run_id, release_date,
    retrieved_at, geo_sk, geo_type, geography_status, value_source, value,
    value_status, source_record_id
)
SELECT 'CAINC1', '3', 'state:55|county:025', 2098, '00000000-0000-4000-a000-000000000bea', '00000000-0000-4000-8000-000000000bea',
       '2098-11-15', '2098-12-31 00:00:00+00', geo_sk, 'county', 'resolved',
       '71234', 71234, 'valid',
       '9bae0c4c5a6f7e8d9caabecfd0e1f203142536475869708192a3b4c5d6e7f809'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- BLS QCEW -- the establishment-count family: jobs located in the county,
-- keyed by (measure, industry, ownership), served from views over a
-- published slice. COUNTY only, per rule 2.
-- ---------------------------------------------------------------------------

INSERT INTO control.bls_qcew_slice (
    run_id, year, period, industry_code, capture_id, captured_row_count,
    in_scope_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000b1c', 2098, 'a', '10', '00000000-0000-4000-a000-000000000b1c', 1, 1, 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_bls_qcew.dim_measure (
    measure_id, measure_label, unit, period_kind, observation_basis,
    methodology_url, parser_contract_version
) VALUES (
    'annual_avg_employment', 'Employment, annual average', 'jobs', 'year',
    'establishment-based: jobs located in the area (smoke fixture)',
    'https://www.bls.gov/opub/hom/cew/home.htm', 'bls_qcew_csv:v1'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_bls_qcew.dim_industry (industry_code, industry_title, industry_level)
VALUES ('10', 'Total, all industries', 'total')
ON CONFLICT DO NOTHING;

INSERT INTO silver_bls_qcew.fact_observation (
    measure_id, industry_code, own_code, geo_id, period_start, capture_id,
    run_id, retrieved_at, period_end, year, period, geo_sk, geo_type,
    geography_status, value_source, value, value_status, source_record_id
)
SELECT 'annual_avg_employment', '10', '0', 'state:55|county:025', '2098-01-01',
       '00000000-0000-4000-a000-000000000b1c', '00000000-0000-4000-8000-000000000b1c', '2098-12-31 00:00:00+00', '2098-12-31', 2098, 'a',
       geo_sk, 'county', 'resolved', '345678', 345678, 'valid',
       '5d6a7f0e1c2b3a4958677a8b9cadbecf0d1e2f30415263748596a7b8c9dadbec'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census BPS -- housing units authorized, keyed by (measure, structure type,
-- frequency) and served from views over a published slice. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.census_bps_slice (
    run_id, slice_key, frequency, year, month, capture_id,
    captured_row_count, in_scope_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000b95', 'county', 'annual', 2098, 12, '00000000-0000-4000-a000-000000000b95', 1, 1, 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_bps.dim_measure (
    measure_id, structure_type, measure_label, structure_label, unit,
    observation_basis, methodology_url, parser_contract_version
) VALUES (
    'units', '1_unit', 'Housing units authorized', '1-unit structures',
    'housing units', 'authorized by building permit (smoke fixture)',
    'https://www.census.gov/construction/bps/methodology.html', 'smoke:v1'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_bps.fact_observation (
    measure_id, structure_type, frequency, geo_id, period_start, capture_id,
    run_id, retrieved_at, period_end, geo_sk, geo_type, geography_status,
    value_source, value, reported_value, value_status, months_reported,
    source_record_id
)
SELECT 'units', '1_unit', 'annual', 'state:55|county:025', '2098-01-01',
       '00000000-0000-4000-a000-000000000b95', '00000000-0000-4000-8000-000000000b95', '2098-12-31 00:00:00+00', '2098-12-31', geo_sk,
       'county', 'resolved', '1234', 1234, 1234, 'valid', 12,
       '6e7b8f1f2d3c4b5a69788b9cadbecfd01e2f3041526374859607a8b9cadbecfd'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census SAIPE/SAHIE -- model-based annual estimates with published
-- confidence bounds, served from views over a published slice. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.census_sae_slice (
    run_id, dataset_id, estimate_year, geo_level, capture_id,
    captured_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-0000000005ae', 'saipe', 2098, 'county', '00000000-0000-4000-a000-0000000005ae', 1, 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_sae.dim_measure (
    dataset_id, measure_id, measure_label, unit, universe, estimate_method,
    methodology_url, parser_contract_version
) VALUES (
    'saipe', 'SAEPOVRTALL_PT', 'Poverty rate, all ages (smoke fixture)',
    'percent', 'all ages', 'model-based small-area estimate',
    'https://www.census.gov/programs-surveys/saipe/technical-documentation/methodology.html',
    'smoke:v1'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_sae.fact_estimate (
    dataset_id, measure_id, estimate_year, geo_id, capture_id, run_id,
    retrieved_at, geo_sk, geo_type, geography_status, value_source, value,
    value_status, confidence_lower, confidence_upper, source_record_id
)
SELECT 'saipe', 'SAEPOVRTALL_PT', 2098, 'state:55|county:025', '00000000-0000-4000-a000-0000000005ae',
       '00000000-0000-4000-8000-0000000005ae', '2098-12-31 00:00:00+00', geo_sk, 'county', 'resolved',
       '9.8', 9.8, 'valid', 8.9, 10.7,
       '7f8c9a2a3e4d5c6b7a899cadbecfd0e12f30415263748596a7b8c9dadbecfd0e'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- IRS SOI migration -- one county's total US inflow for a year pair, which
-- the total views publish as returns, individuals and AGI. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.irs_migration_file (
    run_id, direction, year_pair, year1, year2, capture_id,
    captured_row_count, parsed_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-00000000015c', 'inflow', '2097-2098', 2097, 2098, '00000000-0000-4000-a000-00000000015c', 1, 1, 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_irs_migration.fact_flow (
    direction, year_pair, subject_geo_id, counterpart_code, capture_id,
    run_id, year1, year2, retrieved_at, category, counterpart_label,
    subject_geo_sk, returns, individuals, agi, value_status, value_source,
    source_record_id
)
SELECT 'inflow', '2097-2098', 'state:55|county:025', '97:000', '00000000-0000-4000-a000-00000000015c',
       '00000000-0000-4000-8000-00000000015c', 2097, 2098, '2098-12-31 00:00:00+00', 'total_us',
       'Total Migration-US (smoke fixture)', geo_sk, 12345, 23456, 987654,
       'valid', '12345,23456,987654',
       '8a9dab3b4f5e6d7c8b9aadbecfd0e1f2304152637485960718293a4b5c6d7e8f'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- EIA weekly retail gasoline -- a state's regular-grade price for one week.
-- EIA publishes no county, so this is the STATE exception to rule 2. EIA
-- does not publish Wisconsin; the state is a fixture, like every value here.
-- ---------------------------------------------------------------------------

INSERT INTO control.eia_read (
    run_id, start_week, end_week, page_count, row_total, payload_checksum,
    parsed_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000e1a', '2098-12-29', '2098-12-29', 1, 1,
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 1,
    'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO control.eia_page (run_id, page_index, capture_id, payload_checksum)
VALUES (
    '00000000-0000-4000-8000-000000000e1a', 0, '00000000-0000-4000-a000-000000000e1a',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_eia.fact_retail_price (
    series_id, week_start, capture_id, run_id, retrieved_at, product, duoarea,
    area_name, geo_type, geo_id, geo_sk, geography_status, value_source, value,
    value_status, source_record_id
)
SELECT 'EMM_EPMR_PTE_SWI_DPG', '2098-12-29', '00000000-0000-4000-a000-000000000e1a', '00000000-0000-4000-8000-000000000e1a',
       '2098-12-31 00:00:00+00', 'EPMR', 'SWI', 'Wisconsin (smoke fixture)',
       'state', 'state:55', geo_sk, 'resolved', '3.215', 3.215, 'valid',
       'a226939d214efafafb13b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f809'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census CBP -- establishments for one county and year, with the noise
-- flag CBP publishes, served from views over a published file. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.census_cbp_file (
    run_id, kind, year, capture_id, captured_row_count, in_scope_row_count,
    status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000cb9', 'county', 2098, '00000000-0000-4000-a000-000000000cb9', 1, 1, 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_cbp.fact_observation (
    measure, naics_key, geo_id, year, capture_id, run_id, naics_code,
    retrieved_at, geo_sk, geo_type, geography_status, value_source, value,
    value_status, noise_flag, source_record_id
)
SELECT 'est', 'total', 'state:55|county:025', 2098, '00000000-0000-4000-a000-000000000cb9', '00000000-0000-4000-8000-000000000cb9', '------',
       '2098-12-31 00:00:00+00', geo_sk, 'county', 'resolved', '12345', 12345,
       'valid', 'G',
       'acbf1d5d6b7a8f9eadbbcfd0e1f2031425364758697a8192a3b4c5d6e7f8091a'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census LEHD LODES -- workplace jobs summed from census blocks to one
-- county, served from views over a published state slice.
-- ---------------------------------------------------------------------------

INSERT INTO control.census_lodes_slice (
    run_id, state, year, data_vintage, format_version, version_capture_id,
    checksum_capture_id, status, published_at
) VALUES (
    '00000000-0000-4000-8000-00000000010d', 'wi', 2098, '20981231', 'LODES8', '00000000-0000-4000-a000-00000000010d', '00000000-0000-4000-a000-00000000010d', 'published',
    '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO control.census_lodes_file (
    run_id, family, file_name, capture_id, listed_sha256, status, row_count
) VALUES (
    '00000000-0000-4000-8000-00000000010d', 'wac', 'wi_wac_S000_JT00_2098.csv.gz', '00000000-0000-4000-a000-00000000010d',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
    'captured', 1
) ON CONFLICT DO NOTHING;

INSERT INTO silver_census_lodes.fact_area (
    run_id, family, column_code, geo_id, year, capture_id, geo_sk,
    geography_status, block_count, value_source, value, value_status
)
SELECT '00000000-0000-4000-8000-00000000010d', 'wac', 'C000', 'state:55|county:025', 2098, '00000000-0000-4000-a000-00000000010d', geo_sk,
       'resolved', 1, '345678', 345678, 'valid'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- EPA AirData -- one complete monitor's PM2.5 annual mean, which the county
-- view keeps as the county's highest complete monitor. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.epa_aqs_file (
    run_id, year, capture_id, payload_checksum, row_count, in_scope_row_count,
    status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000e9a', 2098, '00000000-0000-4000-a000-000000000e9a',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 1, 1,
    'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_epa_aqs.monitor_fact (
    run_id, monitor_id, sample_duration, pollutant_standard, event_type, year,
    capture_id, source_row_index, measure, parameter_code, poc, site_number,
    geo_id, geo_sk, geography_status, completeness, certification,
    observation_count, units, value_source, value, value_status,
    source_record_id
)
SELECT '00000000-0000-4000-8000-000000000e9a', '55025-0041-88101-1', '24 HOUR', 'PM25 Annual 2024',
       'No Events', 2098, '00000000-0000-4000-a000-000000000e9a', 1, 'pm25_annual_mean', '88101', 1, '0041',
       'state:55|county:025', geo_sk, 'resolved', 'Y', 'Certified', 120,
       'Micrograms/cubic meter (LC)', '7.4', 7.4, 'valid',
       'bdc02e6e7c8b9a0fbecbdfe1f2031425364758697a8b9c0d1e2f3a4b5c6d7e8f'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- NOAA Climate Normals -- one standard-flagged station inside the county,
-- whose 1991-2020 normal the county view averages. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.noaa_normals_file (
    run_id, archive_version, capture_id, payload_checksum, station_file_count,
    station_count, boundary_vintage, status, published_at
) VALUES (
    '00000000-0000-4000-8000-0000000000aa', 'smoke-2098', '00000000-0000-4000-a000-0000000000aa',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 1, 1,
    2098, 'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_noaa_normals.station (
    run_id, station_id, capture_id, source_row_index, latitude, longitude,
    elevation_m, station_name, geo_id, geo_sk, boundary_vintage,
    geography_status
)
SELECT '00000000-0000-4000-8000-0000000000aa', 'USW00014837', '00000000-0000-4000-a000-0000000000aa', 1, 43.14, -89.35, 262.1,
       'MADISON DANE CO RGNL AP (smoke fixture)', 'state:55|county:025', geo_sk,
       2098, 'resolved'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

INSERT INTO silver_noaa_normals.station_normal (
    run_id, station_id, variable, measure, capture_id, value_source, value,
    value_status, completeness_flag, years, source_record_id
) VALUES (
    '00000000-0000-4000-8000-0000000000aa', 'USW00014837', 'ANN-TAVG-NORMAL', 'annual_mean_temperature',
    '00000000-0000-4000-a000-0000000000aa', '46.6', 46.6, 'valid', 'S', 30, 'ced13f7f8d9cab1acfdce0f2031425360'
) ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FCC Broadband Data Collection -- one county's availability shares for
-- one technology, served from views over a published read. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.fcc_bdc_read (
    run_id, as_of_date, listing_capture_id, payload_checksum, file_count,
    row_count, kept_row_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000fcc', '2098-06-30', '00000000-0000-4000-a000-000000000fcc',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 1, 1,
    1, 'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO control.fcc_bdc_file (
    run_id, slice_key, subcategory, file_id, file_name, revision, capture_id,
    payload_checksum
) VALUES (
    '00000000-0000-4000-8000-000000000fcc', 'other_geographies', 'other_geographies', 1,
    'bdc_us_fixed_broadband_summary_by_geography_J98.csv', '1', '00000000-0000-4000-a000-000000000fcc',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_fcc_bdc.availability_row (
    run_id, geo_id, technology, capture_id, source_row_index, geography_type,
    geography_id, geo_sk, geography_status, total_units, speed_02_02,
    speed_10_1, speed_25_3, speed_100_20, speed_250_25, speed_1000_100,
    value_source, value_status
)
SELECT '00000000-0000-4000-8000-000000000fcc', 'state:55|county:025', 'Any Technology', '00000000-0000-4000-a000-000000000fcc', 1, 'county',
       '55025', geo_sk, 'resolved', 250000, 0.999, 0.998, 0.995, 0.962, 0.901,
       0.655, 'smoke fixture', 'valid'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FEMA National Risk Index -- one county's composite expected annual loss
-- from a published NRI run. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.fema_nri_run (
    run_id, stream, status, run_checksum, nri_version, page_count,
    record_count, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000fe1', 'nri', 'published',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
    'November 2098', 1, 1, '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO control.fema_nri_page (run_id, page_index, capture_id, record_count)
VALUES ('00000000-0000-4000-8000-000000000fe1', 0, '00000000-0000-4000-a000-000000000fe1', 1)
ON CONFLICT DO NOTHING;

INSERT INTO silver_fema_nri.nri_fact (
    run_id, geo_id, field, measure, capture_id, stcofips, county_type,
    nri_version, geo_sk, geography_status, value_source, value, value_status,
    rating, source_record_id
)
SELECT '00000000-0000-4000-8000-000000000fe1', 'state:55|county:025', 'EAL_VALT', 'expected_annual_loss',
       '00000000-0000-4000-a000-000000000fe1', '55025', 'County', 'November 2098', geo_sk, 'resolved',
       '12345678.9', 12345678.9, 'valid', 'Relatively Low',
       'cee25f8f9d0cabcbdcfe0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FHFA House Price Index -- one county's annual index from a published
-- county workbook. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.fhfa_hpi_file (
    run_id, kind, capture_id, payload_checksum, provider_vintage, row_count,
    county_count, status, published_at
) VALUES (
    '00000000-0000-4000-8000-000000000f4f', 'county', '00000000-0000-4000-a000-000000000f4f',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
    '2098-11-30', 1, 1, 'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_fhfa_hpi.fact_observation (
    measure, geo_id, year, capture_id, run_id, provider_vintage, retrieved_at,
    fips_code, geo_sk, geography_status, value_source, value, value_status,
    source_record_id
)
SELECT 'hpi', 'state:55|county:025', 2098, '00000000-0000-4000-a000-000000000f4f', '00000000-0000-4000-8000-000000000f4f', '2098-11-30',
       '2098-12-31 00:00:00+00', '55025', geo_sk, 'resolved', '412.34', 412.34,
       'valid',
       'dff3609fae1bdcdcedf0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- HUD Fair Market Rents -- one county's two-bedroom FMR from a published
-- workbook edition. COUNTY only.
-- ---------------------------------------------------------------------------

INSERT INTO control.hud_fmr_il_file (
    run_id, channel, dataset, fiscal_year, edition, effective_date,
    capture_id, payload_checksum, row_count, county_row_count, status,
    published_at
) VALUES (
    '00000000-0000-4000-8000-000000000d0d', 'workbook', 'fmr', 2098, 'original', '2097-10-01', '00000000-0000-4000-a000-000000000d0d',
    '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', 1, 1,
    'published', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_hud_fmr_il.fact_observation (
    measure, geo_id, fiscal_year, capture_id, run_id, dataset, edition,
    effective_date, retrieved_at, fips_code, geo_type, geo_sk,
    geography_status, hud_area_code, hud_area_name, metro, value_source,
    value, value_status, source_record_id
)
SELECT 'fmr_2br', 'state:55|county:025', 2098, '00000000-0000-4000-a000-000000000d0d', '00000000-0000-4000-8000-000000000d0d', 'fmr',
       'original', '2097-10-01', '2098-12-31 00:00:00+00', '5502599999',
       'county', geo_sk, 'resolved', 'METRO31540M31540',
       'Madison, WI MSA (smoke fixture)', TRUE, '1650', 1650, 'valid',
       'e00471a0bf2cedededf1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Publish the catalog, exactly as the glossary harvest would.
--
-- `glossary/harvest.py` reads each `gold_<source>.metric_publisher` view and
-- writes `source_code || ':' || source_object_key` as the metric code. That
-- rule, and the publisher's own `physical_lineage`, are what the neutral
-- resource resolves a metric's serving rows through -- so they are read from
-- the publishers here rather than restated. A source whose publisher stops
-- yielding a row for the data above publishes no catalog row, and the tier
-- then reports a source with no metrics instead of passing on a metric whose
-- lineage this file invented.
-- ---------------------------------------------------------------------------

CREATE TEMP VIEW smoke_published AS
SELECT * FROM gold_census.metric_publisher
UNION ALL SELECT * FROM gold_fred.metric_publisher
UNION ALL SELECT * FROM gold_bls.metric_publisher
UNION ALL SELECT * FROM gold_pep.metric_publisher
UNION ALL SELECT * FROM gold_cdc.metric_publisher
UNION ALL SELECT * FROM gold_fbi.metric_publisher
UNION ALL SELECT * FROM gold_nass.metric_publisher
UNION ALL SELECT * FROM gold_bea.metric_publisher
UNION ALL SELECT * FROM gold_bls_qcew.metric_publisher
UNION ALL SELECT * FROM gold_census_bps.metric_publisher
UNION ALL SELECT * FROM gold_census_sae.metric_publisher
UNION ALL SELECT * FROM gold_irs_migration.metric_publisher
UNION ALL SELECT * FROM gold_eia.metric_publisher
UNION ALL SELECT * FROM gold_census_cbp.metric_publisher
UNION ALL SELECT * FROM gold_census_lodes.metric_publisher
UNION ALL SELECT * FROM gold_epa_aqs.metric_publisher
UNION ALL SELECT * FROM gold_noaa_normals.metric_publisher
UNION ALL SELECT * FROM gold_fcc_bdc.metric_publisher
UNION ALL SELECT * FROM gold_fema_nri.metric_publisher
UNION ALL SELECT * FROM gold_fhfa_hpi.metric_publisher
UNION ALL SELECT * FROM gold_hud_fmr_il.metric_publisher;

INSERT INTO gold_glossary.dim_source_system (
    source_code, source_name, source_type, reference_url
)
SELECT DISTINCT ON (source_code)
       source_code, source_name, source_type, reference_url
FROM smoke_published
ORDER BY source_code
ON CONFLICT (source_code) DO UPDATE SET
    source_name = EXCLUDED.source_name,
    source_type = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url;

INSERT INTO gold_glossary.dim_metric_catalog (
    metric_code, source_code, source_object_type, source_object_key,
    metric_display_name, units, measure_kind, valid_geo_grains,
    valid_time_grains, aggregation_characteristic, physical_lineage,
    publisher_contract_version, source_watermark, source_run_id,
    publication_time
)
SELECT source_code || ':' || source_object_key, source_code,
       source_object_type, source_object_key, metric_display_name, units,
       measure_kind, valid_geo_grains, valid_time_grains,
       aggregation_characteristic, physical_lineage,
       publisher_contract_version, source_watermark, source_run_id,
       publication_time
FROM smoke_published
ON CONFLICT (metric_code) DO UPDATE SET
    source_object_type = EXCLUDED.source_object_type,
    source_object_key = EXCLUDED.source_object_key,
    metric_display_name = EXCLUDED.metric_display_name,
    units = EXCLUDED.units,
    measure_kind = EXCLUDED.measure_kind,
    valid_geo_grains = EXCLUDED.valid_geo_grains,
    valid_time_grains = EXCLUDED.valid_time_grains,
    aggregation_characteristic = EXCLUDED.aggregation_characteristic,
    physical_lineage = EXCLUDED.physical_lineage;

DROP VIEW smoke_published;
