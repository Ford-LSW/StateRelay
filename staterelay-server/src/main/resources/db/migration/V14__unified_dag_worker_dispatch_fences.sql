-- Unified sr_worker DAG dispatch fences. All nullable fence columns intentionally preserve
-- compatibility with rows created before this migration.
ALTER TABLE sr_dag_node_instance
    ADD COLUMN IF NOT EXISTS current_attempt_id BIGINT,
    ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT,
    ADD COLUMN IF NOT EXISTS dispatch_token VARCHAR(128);

ALTER TABLE sr_dag_node_instance
    ADD CONSTRAINT fk_sr_dag_node_current_attempt
    FOREIGN KEY (current_attempt_id) REFERENCES sr_dag_node_attempt(id);

ALTER TABLE sr_dag_node_instance
    ADD CONSTRAINT ck_sr_dag_node_dispatch_generation
    CHECK (dispatch_generation IS NULL OR dispatch_generation > 0);

ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT,
    ADD COLUMN IF NOT EXISTS dispatch_token VARCHAR(128),
    ADD COLUMN IF NOT EXISTS capacity_released_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS transport_generation BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS transport_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_dispatch_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_dispatch_error TEXT;

ALTER TABLE sr_dag_node_attempt
    ADD CONSTRAINT ck_sr_dag_attempt_dispatch_generation
        CHECK (dispatch_generation IS NULL OR dispatch_generation > 0),
    ADD CONSTRAINT ck_sr_dag_attempt_transport_generation
        CHECK (transport_generation >= 0),
    ADD CONSTRAINT ck_sr_dag_attempt_transport_attempts
        CHECK (transport_attempts >= 0);

CREATE INDEX IF NOT EXISTS idx_sr_dag_attempt_transport_due
    ON sr_dag_node_attempt(next_dispatch_at, id)
    WHERE status = 10 AND next_dispatch_at IS NOT NULL;

COMMENT ON COLUMN sr_dag_node_instance.current_attempt_id IS
    'Current authoritative physical attempt; nullable for pre-V14 rows';
COMMENT ON COLUMN sr_dag_node_instance.dispatch_generation IS
    'Logical dispatch generation copied to the current attempt';
COMMENT ON COLUMN sr_dag_node_instance.dispatch_token IS
    'Opaque logical dispatch token copied to the current attempt';
COMMENT ON COLUMN sr_dag_node_attempt.capacity_released_at IS
    'Set once when this attempt reservation is released from sr_worker';
