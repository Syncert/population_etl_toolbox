-- Migration 035: one FBI agency relationship per agency, area and window.
--
-- `silver_fbi.transform._supersede_contained_relationships` removed a
-- relationship row only when another row of the same product, agency, label
-- and resolved geography had a *strictly* wider window. A reviewed place
-- mapping effective from 2023 bridges 2023's months whatever the product
-- window, so a release over 2023 and one over 1990-2023 recorded the same
-- place window under two geography vintages, both rows stayed, and
-- `gold_fbi.agency_observation_area_filter` served each of those observations
-- twice (DQ-FBI-004: 167,296 rows against 167,200 agency grains on the local
-- warehouse). The transform now also supersedes an equal window, keeping the
-- row written last; it runs per product only when a release is replayed, so
-- this applies the same statement to every product once. Safe to rerun: a
-- second run finds nothing to delete. No evidence is lost: the kept row
-- carries the same agency, label, geography and window, and the release's
-- captures are untouched.

DELETE FROM silver_fbi.agency_geography_relationship AS narrower
USING silver_fbi.agency_geography_relationship AS wider
WHERE wider.product_id = narrower.product_id
  AND wider.ori = narrower.ori
  AND wider.relationship_type = narrower.relationship_type
  AND wider.source_label = narrower.source_label
  AND wider.geo_id IS NOT DISTINCT FROM narrower.geo_id
  AND wider.relationship_sk <> narrower.relationship_sk
  AND wider.effective_start <= narrower.effective_start
  AND wider.effective_end >= narrower.effective_end
  AND (
      (wider.effective_start, wider.effective_end)
          <> (narrower.effective_start, narrower.effective_end)
      OR (wider.updated_at, wider.relationship_sk)
          > (narrower.updated_at, narrower.relationship_sk)
  );
