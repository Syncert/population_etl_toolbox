-- 041: serve each BLS key's newest *published* value as its latest.
--
-- `gold_bls.refresh_mv_bls_latest` took each key's newest row, whatever it
-- held. BLS answers the October 2025 month of most series with `-` and the
-- footnote "Data unavailable due to the 2025 lapse in appropriations", and
-- for a series with no later month that withheld row became the series'
-- latest: on the internal stack on 2026-11-06, 8 Average Price keys served an
-- empty 2025-10-31 row (Midwest whole milk, APU0200709112, whose newest price
-- is April 2020, among them) although every one has an older real value.
--
-- The procedure now sorts withheld rows last (`gold_bls.sql`). The rows it
-- already wrote change only when their key is next refreshed, and a key whose
-- series has ended may never be refreshed again, so this step replaces those
-- rows once. It does not call the procedure: a step must not depend on the
-- procedure body a deploy happens to hold. The withheld rows stay in
-- `rpt_bls_observations` with their status and footnote-derived token; only
-- the latest projection changes. A key whose every row is withheld keeps its
-- withheld row. Rerunnable: the second run finds no key to replace.

DROP TABLE IF EXISTS pg_temp.gold_bls_withheld_latest_keys;
CREATE TEMP TABLE gold_bls_withheld_latest_keys AS
SELECT m.geo_id, m.series_id, m.metric_code
FROM gold_bls.mv_bls_latest AS m
WHERE m.value IS NULL
  AND m.metric_code IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM gold_bls.rpt_bls_observations AS r
      WHERE r.metric_code = m.metric_code
        AND r.geo_id = m.geo_id
        AND r.series_id = m.series_id
        AND r.value IS NOT NULL
  );

DELETE FROM gold_bls.mv_bls_latest AS m
USING gold_bls_withheld_latest_keys AS k
WHERE m.geo_id = k.geo_id
  AND m.series_id = k.series_id
  AND m.metric_code = k.metric_code;

INSERT INTO gold_bls.mv_bls_latest
SELECT DISTINCT ON (r.geo_id, r.series_id, r.metric_code)
    r.*
FROM gold_bls.rpt_bls_observations AS r
JOIN gold_bls_withheld_latest_keys AS k
  ON k.geo_id = r.geo_id
 AND k.series_id = r.series_id
 AND k.metric_code = r.metric_code
WHERE r.value IS NOT NULL
ORDER BY r.geo_id, r.series_id, r.metric_code,
         r.observation_date DESC, r.updated_at DESC;

DROP TABLE gold_bls_withheld_latest_keys;
