-- A stratum is a JSON array, and the warehouse now says so.
--
-- `/api/v1/cdc/observations` publishes a stratum with every value, and the
-- response model declares `strata: list[Any]`. The CDC parser produces one: a
-- tuple of (category, category_label, value, value_label) tuples that psycopg2
-- stores as a JSON array of arrays. Nothing recorded that anywhere the
-- database could check. `jsonb` accepts an object, a string, a number, or
-- `null` just as happily as an array, and the serving path hands each row
-- straight to `CdcObservation.model_validate`.
--
-- So a stratum stored as an object was accepted by every write path and then
-- crashed the read path -- a pydantic ValidationError the caller sees as
-- `500 The API failed to complete this request`, with no way to tell which row
-- is unserveable, and the row stays in the warehouse answering 500 for every
-- page that includes it.
--
-- The shape is decided where it is written, not coerced where it is read: an
-- empty list standing in for a stratum would be exactly the missing-presented-
-- as-a-value this warehouse refuses. Both relations that store one are
-- checked -- `observation_revision` is where a replay lands it, `dim_stratum`
-- is what `gold_cdc.health_observation` joins and the API therefore reads.
--
-- Rerun-safe: each constraint is dropped before it is added. Existing rows are
-- validated by ADD CONSTRAINT, so a warehouse already holding a non-array
-- stratum fails this step loudly rather than serving it.

ALTER TABLE silver_cdc.dim_stratum
    DROP CONSTRAINT IF EXISTS dim_stratum_strata_is_array_check,
    ADD CONSTRAINT dim_stratum_strata_is_array_check
        CHECK (jsonb_typeof(strata) = 'array');

ALTER TABLE silver_cdc.observation_revision
    DROP CONSTRAINT IF EXISTS observation_revision_strata_is_array_check,
    ADD CONSTRAINT observation_revision_strata_is_array_check
        CHECK (jsonb_typeof(strata) = 'array');

COMMENT ON COLUMN silver_cdc.dim_stratum.strata IS
    'The stratum as the parser produced it: a JSON array of '
    '[category, category_label, value, value_label] arrays. The API publishes '
    'it as a list, so a non-array shape is refused here rather than surfacing '
    'as an unhandled 500 on every page that includes the row.';

COMMENT ON COLUMN silver_cdc.observation_revision.strata IS
    'The stratum as the parser produced it, before dim_stratum distinct-loads '
    'it: a JSON array of [category, category_label, value, value_label] arrays.';
