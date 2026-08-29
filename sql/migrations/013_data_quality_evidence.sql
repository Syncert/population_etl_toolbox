-- Warehouse data-quality evidence store.
-- Fresh-bootstrap and idempotent rerun DDL for the disposable beta warehouse.
--
-- Ownership boundaries: this migration owns only the shared quality-evidence
-- relations in control. Quality definitions stay version-controlled in Python
-- (data_ingestion_toolbox.quality); these tables persist operational evidence
-- only. Results are append-only: re-running a rule creates new evidence rather
-- than rewriting history, and only a warning's review status may change after
-- the fact.

CREATE TABLE IF NOT EXISTS control.data_quality_run (
    quality_run_id       UUID PRIMARY KEY,
    source_code          TEXT NOT NULL CHECK (BTRIM(source_code) <> ''),
    ingestion_run_id     UUID REFERENCES control.ingestion_run(run_id),
    publication_event_id UUID REFERENCES control.publisher_ready_event(event_id),
    assessment_type      TEXT NOT NULL CHECK (
        assessment_type IN ('inline', 'scheduled', 'release', 'manual')
    ),
    code_commit_sha      TEXT NOT NULL CHECK (code_commit_sha ~ '^[0-9a-f]{40}$'),
    rule_set_version     TEXT NOT NULL CHECK (BTRIM(rule_set_version) <> ''),
    evaluated_scope      JSONB NOT NULL DEFAULT '{}'::JSONB,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at          TIMESTAMPTZ,
    overall_status       TEXT NOT NULL DEFAULT 'running' CHECK (
        overall_status IN ('running', 'pass', 'warn', 'fail', 'error')
    ),
    failure_summary      TEXT NOT NULL DEFAULT '' CHECK (
        CHAR_LENGTH(failure_summary) <= 2000
    ),
    CONSTRAINT data_quality_run_finished_after_start CHECK (
        finished_at IS NULL OR finished_at >= started_at
    ),
    CONSTRAINT data_quality_run_terminal_has_finish CHECK (
        overall_status = 'running' OR finished_at IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS ix_data_quality_run_source_started
    ON control.data_quality_run (source_code, started_at DESC);

CREATE TABLE IF NOT EXISTS control.data_quality_result (
    result_id         BIGSERIAL PRIMARY KEY,
    quality_run_id    UUID NOT NULL
        REFERENCES control.data_quality_run(quality_run_id),
    rule_id           TEXT NOT NULL CHECK (rule_id ~ '^DQ-[A-Z]+-[0-9]{3}$'),
    severity          TEXT NOT NULL CHECK (
        severity IN ('BLOCK', 'QUARANTINE', 'WARN', 'INFO')
    ),
    layer             TEXT NOT NULL CHECK (
        layer IN (
            'raw', 'control', 'silver', 'reference',
            'gold', 'publisher', 'serving', 'glossary'
        )
    ),
    object_name       TEXT NOT NULL CHECK (object_name ~ '^[a-z_]+\.[a-z_0-9]+$'),
    source_code       TEXT NOT NULL CHECK (BTRIM(source_code) <> ''),
    partition_key     TEXT NOT NULL DEFAULT '',
    partition_detail  JSONB NOT NULL DEFAULT '{}'::JSONB,
    result            TEXT NOT NULL CHECK (
        result IN ('pass', 'fail', 'warn', 'not_applicable')
    ),
    observed_count    BIGINT,
    expected_count    BIGINT,
    observed_measure  NUMERIC,
    source_watermark  TEXT,
    latest_capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    -- Bounded sample identifiers only: never credentials, secrets, or payloads.
    evidence          JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (
        PG_COLUMN_SIZE(evidence) <= 8192
    ),
    evaluated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms       INTEGER NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    review_status     TEXT CHECK (
        review_status IN ('open', 'acknowledged', 'accepted', 'escalated')
    ),
    CONSTRAINT data_quality_result_review_is_for_warnings CHECK (
        review_status IS NULL OR result = 'warn'
    ),
    CONSTRAINT data_quality_result_one_per_rule_object_partition UNIQUE (
        quality_run_id, rule_id, object_name, partition_key
    )
);

CREATE INDEX IF NOT EXISTS ix_data_quality_result_rule_evaluated
    ON control.data_quality_result (rule_id, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS ix_data_quality_result_object
    ON control.data_quality_result (object_name, evaluated_at DESC);

-- Evidence is append-only. The only mutation history may absorb is a warning's
-- review status; every other change must arrive as a new run's evidence.
CREATE OR REPLACE FUNCTION control.data_quality_result_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'control.data_quality_result is append-only evidence; DELETE is not allowed';
    END IF;
    IF ROW(NEW.quality_run_id, NEW.rule_id, NEW.severity, NEW.layer,
           NEW.object_name, NEW.source_code, NEW.partition_key,
           NEW.partition_detail, NEW.result, NEW.observed_count,
           NEW.expected_count, NEW.observed_measure, NEW.source_watermark,
           NEW.latest_capture_id, NEW.evidence, NEW.evaluated_at,
           NEW.duration_ms)
       IS DISTINCT FROM
       ROW(OLD.quality_run_id, OLD.rule_id, OLD.severity, OLD.layer,
           OLD.object_name, OLD.source_code, OLD.partition_key,
           OLD.partition_detail, OLD.result, OLD.observed_count,
           OLD.expected_count, OLD.observed_measure, OLD.source_watermark,
           OLD.latest_capture_id, OLD.evidence, OLD.evaluated_at,
           OLD.duration_ms)
    THEN
        RAISE EXCEPTION
            'control.data_quality_result is append-only evidence; only review_status may change';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS data_quality_result_append_only
    ON control.data_quality_result;
CREATE TRIGGER data_quality_result_append_only
    BEFORE UPDATE OR DELETE ON control.data_quality_result
    FOR EACH ROW EXECUTE FUNCTION control.data_quality_result_guard();
