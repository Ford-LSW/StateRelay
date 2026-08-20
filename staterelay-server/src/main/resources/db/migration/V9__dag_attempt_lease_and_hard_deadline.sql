-- =============================================================================
-- V9: NodeAttempt lease 与硬截止时间
--
-- 对齐 DAG_Engine_Scheduler_Executor_设计总结_第一版封版修订.md：
--   §43 / §29.4 / §29.5 / §19.1 / §27.2 / §44 / §47 / §50
--
-- 核心变更：
--   1. 新增 execution_deadline_at（不可续约硬截止，T8 创建时由 DB 时钟固化）
--   2. 新增 attempt_lease_version（lease 围栏版本，单调递增 CAS）
--   3. 新增 attempt_lease_expire_time（Worker lease 续约时间，与硬截止分离）
--   4. 新增 worker_epoch（Worker 重启周期，用于跨 epoch 恢复校验）
--   5. 新增基于 execution_deadline_at 的超时索引（替代原 started_at 索引）
-- =============================================================================
-- -----------------------------------------------------------------------------
-- 1. NodeAttempt 新增字段
-- -----------------------------------------------------------------------------
ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS execution_deadline_at TIMESTAMPTZ;

ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS attempt_lease_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS attempt_lease_expire_time TIMESTAMPTZ;

ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS worker_epoch VARCHAR(64);

COMMENT ON COLUMN sr_dag_node_attempt.execution_deadline_at IS
    '执行硬截止时间（不可续约，T8 创建 Attempt 时由数据库 NOW() + timeout_seconds 一次性固化）；'
    'heartbeat 不能延长；到期无论 lease 是否仍有效，都进入 TIMEOUT 处理；对齐文档 §43/§29.4';

COMMENT ON COLUMN sr_dag_node_attempt.attempt_lease_version IS
    'Attempt lease 围栏版本号（单调递增）；Scheduler CAS 升级后通过 rebindFence 同步给 Worker；'
    '对齐文档 §19.1/§44/§47.1';

COMMENT ON COLUMN sr_dag_node_attempt.attempt_lease_expire_time IS
    'Attempt lease 到期时间（Worker heartbeat 可续约）；'
    '到期只触发 UNKNOWN / 接管恢复，不直接等于 TIMEOUT；对齐文档 §43';

COMMENT ON COLUMN sr_dag_node_attempt.worker_epoch IS
    'Worker 重启周期标识；旧 epoch 的 heartbeat / 取消响应 / 执行结果一律不能覆盖新 epoch 状态；'
    '对齐文档 §23';

-- -----------------------------------------------------------------------------
-- 2. 索引：基于 execution_deadline_at 的超时扫描（§29.4 / §29.5）
-- -----------------------------------------------------------------------------
-- 取代原基于 started_at 的 RUNNING 超时索引；现在 DISPATCHING / RUNNING / UNKNOWN 都参与硬截止扫描
CREATE INDEX IF NOT EXISTS idx_sr_dag_attempt_hard_deadline
    ON sr_dag_node_attempt (execution_deadline_at, id)
    WHERE status IN (10, 30, 80)
      AND execution_deadline_at IS NOT NULL;

COMMENT ON INDEX idx_sr_dag_attempt_hard_deadline IS
    '超时扫描索引（DISPATCHING / RUNNING / UNKNOWN 中 execution_deadline_at 已到期的 Attempt）；对齐文档 §29.4';

-- lease 到期扫描索引（UNKNOWN 接管恢复）
CREATE INDEX IF NOT EXISTS idx_sr_dag_attempt_lease_expired
    ON sr_dag_node_attempt (attempt_lease_expire_time, id)
    WHERE status IN (10, 30, 80)
      AND attempt_lease_expire_time IS NOT NULL;

COMMENT ON INDEX idx_sr_dag_attempt_lease_expired IS
    'Attempt lease 到期扫描索引；lease 到期且硬截止未到则进入 UNKNOWN 接管恢复；对齐文档 §43/§19.1';
