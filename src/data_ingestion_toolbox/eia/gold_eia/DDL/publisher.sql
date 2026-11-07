-- Provider-neutral glossary publisher contract for EIA retail gasoline
-- (grocery-and-gasoline-prices) and home energy prices
-- (home-heating-fuel-prices). One row per measure, across every area EIA
-- publishes it for; grains are aggregated from the rows the latest view
-- serves with a value. The display name carries EIA's citation.
CREATE OR REPLACE VIEW gold_eia.metric_publisher AS
SELECT 'EIA'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       served.metric_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (CASE served.metric_key
            WHEN 'ELEC_RES' THEN 'Residential electricity price, monthly (EIA-861M, cents per kilowatt-hour)'
            WHEN 'NG_RES' THEN 'Residential natural gas price, monthly (EIA Natural Gas Monthly, U.S. dollars per thousand cubic feet)'
            WHEN 'EPD2F' THEN 'Residential heating oil price, weekly October to March (EIA-877, U.S. dollars per gallon)'
            WHEN 'EPLLPA' THEN 'Residential propane price, weekly October to March (EIA-877, U.S. dollars per gallon)'
            WHEN 'EPD2D' THEN 'On-highway diesel fuel retail price, weekly (EIA-888, U.S. dollars per gallon)'
            ELSE served.grade || ' retail price, weekly (EIA-878, U.S. dollars per gallon)'
        END)::TEXT AS metric_display_name,
       served.unit AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY[UPPER(served.frequency)]::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_eia',
           'relation', 'observation_revision',
           'key', served.metric_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       (CASE served.route
            WHEN 'res_electricity' THEN 'U.S. Energy Information Administration, Electric Power Monthly (EIA-861M)'
            WHEN 'res_natural_gas' THEN 'U.S. Energy Information Administration, Natural Gas Monthly'
            WHEN 'heating_fuels' THEN 'U.S. Energy Information Administration, State Heating Oil and Propane Program (EIA-877)'
            WHEN 'diesel' THEN 'U.S. Energy Information Administration, Gasoline and Diesel Fuel Update (EIA-888)'
            ELSE 'U.S. Energy Information Administration, Gasoline and Diesel Fuel Update (EIA-878)'
        END)::TEXT AS source_name,
       'government-energy-statistics'::TEXT AS source_type,
       (CASE served.route
            WHEN 'res_electricity' THEN 'https://www.eia.gov/electricity/monthly/'
            WHEN 'res_natural_gas' THEN 'https://www.eia.gov/naturalgas/monthly/'
            WHEN 'heating_fuels' THEN 'https://www.eia.gov/petroleum/heatingoilpropane/'
            ELSE 'https://www.eia.gov/petroleum/gasdiesel/'
        END)::TEXT AS reference_url,
       -- The newest week any area is served with a value, for the harvest's
       -- `ended` rule (migration 042). Optional; appended at the end.
       served.last_value_date
FROM (
    SELECT latest.metric_key,
           MIN(latest.grade) AS grade,
           MIN(latest.unit) AS unit,
           MIN(latest.route) AS route,
           MIN(latest.frequency) AS frequency,
           MAX(latest.week_start)::TEXT AS source_watermark,
           -- A grain is one a value is published at.
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(read.published_at) AS publication_time,
           MAX(latest.week_start) FILTER (WHERE latest.value IS NOT NULL)::DATE
               AS last_value_date
    FROM gold_eia.observation_latest AS latest
    JOIN control.eia_read AS read ON read.run_id = latest.run_id
    GROUP BY latest.metric_key
) AS served
WHERE served.publication_time IS NOT NULL;
