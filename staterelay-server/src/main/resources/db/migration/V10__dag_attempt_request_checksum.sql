-- =============================================================================
-- V10: NodeAttempt 新增 request_checksum 字段
--
-- 对齐 DAG_Engine_Scheduler_Executor_设计总结_第一版封版修订.md §44 / §47.1：
--   rebindFence 协议围栏校验需要 requestChecksum 字段，
--   用于跨进程 lease 接管时 fail-closed 校验"相同 requestId + 相同业务参数"。
--
-- checksum 语义（§44 协议规则第 4 条）：
--   algorithmCode + requestJson 的 SHA-256 摘要；
--   相同 requestId 但 checksum 不同时，Worker Store 必须拒绝执行并告警。
-- =============================================================================
ALTER TABLE sr_dag_node_attempt
    ADD COLUMN IF NOT EXISTS request_checksum VARCHAR(64) NOT NULL DEFAULT '';

COMMENT ON COLUMN sr_dag_node_attempt.request_checksum IS
    '业务参数摘要（algorithmCode + requestJson 的 SHA-256）；'
    '§44 rebindFence 围栏校验字段，相同 requestId + 不同 checksum 必须 fail-closed 拒绝';
