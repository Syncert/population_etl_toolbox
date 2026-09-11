-- 016_publisher_harvest_fingerprint.sql
--
-- The glossary harvest skipped whenever a publisher's publication_time had not
-- advanced. That time is derived from the *facts* every publisher reads, so a
-- change to what the publisher SAYS -- a metric's identity, display name,
-- units, grains, lineage, or the set of keys it emits -- moved nothing the
-- guard could see. The harvest returned zero rows, recorded success, and left
-- the catalog serving identities the warehouse no longer published, with no
-- way back except an operator editing this table by hand.
--
-- The fix gives the guard a second input: a digest of the content the harvest
-- would write. A NULL fingerprint means "never recorded", which must harvest
-- rather than skip, so every existing row re-harvests once after this
-- migration and is fingerprinted from then on.

ALTER TABLE gold_glossary.publisher_harvest_state
    ADD COLUMN IF NOT EXISTS last_content_fingerprint TEXT;

COMMENT ON COLUMN gold_glossary.publisher_harvest_state.last_content_fingerprint IS
    'SHA-256 over the catalog content the last successful harvest wrote. NULL means never recorded, which harvests rather than skips.';

-- Whether that harvest ran because an operator forced it, so the reason a
-- catalog moved is inspectable after the fact rather than only in a task log.
ALTER TABLE gold_glossary.publisher_harvest_state
    ADD COLUMN IF NOT EXISTS last_harvest_forced BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN gold_glossary.publisher_harvest_state.last_harvest_forced IS
    'TRUE when the last harvest bypassed the publication-time and fingerprint guards on an explicit operator request.';
