-- 042: a measure whose provider stopped publishing values is `ended`.
--
-- `gold_glossary.dim_metric_catalog.freshness_state` said only whether the
-- publisher still *emits* a key: `current` while it does, `stale` and then
-- `retired` once it stops. A series the provider keeps listing but stopped
-- pricing stayed `current` for ever. On the internal stack on 2026-11-06 the
-- Midwest whole-milk Average Price (BLS:APU0200709112) was `current` with its
-- newest price in April 2020, alongside 7 more Average Price and 16 LAUS
-- series whose newest value was over a year old.
--
-- The harvest now publishes `ended` for such a key when its publisher reports
-- the date of its newest real value and that date is older than the
-- threshold for the key's time grain (`glossary/harvest.py`). `ended` is not
-- `retired`: the history is still served, and the observation routes still
-- answer it. It is not `stale` either, which stays the early warning that a
-- publisher stopped emitting a key. This widens the CHECK to admit it.
-- Widening invalidates nothing stored, a CHECK already naming `ended` is left
-- alone, and on a fresh bootstrap `002_gold_glossary_schema.sql` declares the
-- same list. Idempotent.

DO $$
DECLARE
    v_constraint TEXT;
BEGIN
    IF to_regclass('gold_glossary.dim_metric_catalog') IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'gold_glossary.dim_metric_catalog'::regclass
          AND conname = 'dim_metric_catalog_freshness_state_check'
          AND pg_get_constraintdef(oid) LIKE '%ended%'
    ) THEN
        RETURN;
    END IF;
    -- Whatever the existing CHECK on the column is called: migration 002's
    -- inline declaration and the glossary schema's are both unnamed.
    FOR v_constraint IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'gold_glossary.dim_metric_catalog'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%freshness_state%'
    LOOP
        EXECUTE format(
            'ALTER TABLE gold_glossary.dim_metric_catalog DROP CONSTRAINT %I',
            v_constraint
        );
    END LOOP;
    ALTER TABLE gold_glossary.dim_metric_catalog
        ADD CONSTRAINT dim_metric_catalog_freshness_state_check
        CHECK (freshness_state IN ('current', 'ended', 'stale', 'retired'));
END
$$;
