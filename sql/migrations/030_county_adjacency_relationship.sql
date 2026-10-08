-- 030: counties that share a boundary are a relationship the reference keeps.
--
-- `silver_ref.bridge_geo_relationship_version` recorded containment (state to
-- county, county to place by code) and boundary intersection (county to
-- place), and nothing about which counties neighbour each other, so a place
-- page had nowhere to send a reader next and the compare page no sensible
-- default (nearby-and-related-places). Adjacency is a property of one
-- geometry vintage, so it belongs in the same versioned bridge, computed once
-- per vintage by `GeographyRepository.reconcile_relationships` with the
-- evidence source `census_boundary_adjacency`, never per request.
--
-- `silver_ref.sql` declares the widened check for a fresh warehouse; this
-- swaps it on one that already holds the table. No row changes.
--
-- Rerunnable: the constraint is dropped by name and recreated identically.

ALTER TABLE silver_ref.bridge_geo_relationship_version
    DROP CONSTRAINT IF EXISTS bridge_geo_relationship_version_relationship_type_check;

ALTER TABLE silver_ref.bridge_geo_relationship_version
    ADD CONSTRAINT bridge_geo_relationship_version_relationship_type_check CHECK (
        relationship_type IN ('contains', 'intersects', 'adjacent', 'serves', 'provider_crosswalk')
    );
