-- 017_serving_full_reserve_run.sql
--
-- A forced full re-serve rewrites every calendar year regardless of its
-- watermark, because the change that motivates one -- a metric identity, a
-- geography vocabulary, units -- moves no watermark at all. That leaves the
-- driver with no way to tell "this year is already done" from "this year looks
-- done because nothing changed", so it needs a different marker for progress.
--
-- The marker is when the current forced run began. A chunk that completed at
-- or after that instant is done for this run; everything else is outstanding.
-- Keeping it here rather than in memory is what makes an interrupted re-serve
-- resume at the year it stopped on: an Airflow retry is a new process, and
-- re-running two hours of completed ACS chunks because the third hour failed
-- is the behaviour this column exists to avoid.

ALTER TABLE control.serving_refresh_state
    ADD COLUMN IF NOT EXISTS last_full_reserve_started_at TIMESTAMPTZ;

COMMENT ON COLUMN control.serving_refresh_state.last_full_reserve_started_at IS
    'When the current (or most recent) forced full re-serve began. Chunks completing at or after it are done for that run; a retry resumes rather than restarts.';
