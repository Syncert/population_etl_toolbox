-- Agency directories are captured and conformed per offense product.
-- The legacy key omitted product_id: later products updated the first
-- product's evidence and could not publish their own county roll-ups.
-- Widen the key without changing or deleting evidence. Replaying captured
-- product releases then restores each product's missing relationships and
-- geography statuses, using its own directory bytes rather than copied links.
DO $relationship_key$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_fbi.agency_geography_relationship'::REGCLASS
          AND conname = 'fbi_agency_relationship_product_key'
    ) THEN
        ALTER TABLE silver_fbi.agency_geography_relationship
            ADD CONSTRAINT fbi_agency_relationship_product_key UNIQUE (
                product_id, ori, relationship_type, source_label,
                geography_vintage, effective_start
            );
    END IF;
    ALTER TABLE silver_fbi.agency_geography_relationship
        DROP CONSTRAINT IF EXISTS
            agency_geography_relationship_ori_relationship_type_source__key;
END;
$relationship_key$;
