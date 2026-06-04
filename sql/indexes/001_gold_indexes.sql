CREATE INDEX IF NOT EXISTS ix_gold_fact_observation_metric_geo_period
    ON gold.rpt_observation_dashboard (metric_code, geo_level, observation_date);

CREATE INDEX IF NOT EXISTS ix_gold_fact_observation_geo_metric_period
    ON gold.rpt_observation_dashboard (geo_id, metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_gold_fact_observation_geo
    ON gold.rpt_observation_dashboard (geo_level, geo_id);

CREATE INDEX IF NOT EXISTS ix_gold_fact_observation_source_dataset
    ON gold.rpt_observation_dashboard (source_code, dataset_code);

CREATE INDEX IF NOT EXISTS ix_gold_fact_observation_period
    ON gold.rpt_observation_dashboard (observation_date);

CREATE INDEX IF NOT EXISTS ix_silver_ref_dim_geo_geom
    ON silver_ref.dim_geo USING GIST (geom);
