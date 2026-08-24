-- Artifact 必须绑定到创建它的完整执行围栏，旧数据允许为空。
ALTER TABLE sr_dag_artifact
    ADD COLUMN IF NOT EXISTS request_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS request_checksum VARCHAR(128),
    ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT,
    ADD COLUMN IF NOT EXISTS dispatch_token VARCHAR(128),
    ADD COLUMN IF NOT EXISTS attempt_lease_version BIGINT,
    ADD COLUMN IF NOT EXISTS worker_id VARCHAR(160),
    ADD COLUMN IF NOT EXISTS worker_epoch VARCHAR(64);

-- 新流程以物理 Attempt 和输出端口作为 Artifact 的幂等身份。
ALTER TABLE sr_dag_artifact
    DROP CONSTRAINT IF EXISTS uk_sr_dag_artifact_per_attempt;

CREATE UNIQUE INDEX IF NOT EXISTS uk_sr_dag_artifact_attempt_output
    ON sr_dag_artifact (attempt_id, output_name)
    WHERE attempt_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sr_dag_artifact_attempt_status
    ON sr_dag_artifact (attempt_id, status);

COMMENT ON INDEX uk_sr_dag_artifact_attempt_output IS
    '新协议按 attempt_id + output_name 幂等登记，同一节点的不同 Attempt 相互隔离';
