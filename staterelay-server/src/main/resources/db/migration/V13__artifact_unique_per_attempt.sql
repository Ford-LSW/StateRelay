-- =============================================================================
-- V13: sr_dag_artifact 唯一约束扩展为按 Attempt 隔离
--
-- 对齐 GIS-Worker与节点数据传递设计.md §16.2 / §16.3：
--   - 同一个 NodeInstance 可以有多次 Attempt（重试 / 晚到）
--   - 每次 Attempt 都会产出自己的 STAGED Artifact，output_name 可能相同
--   - 只有当前权威 Attempt 的 Artifact → AVAILABLE，晚到的 → ORPHANED
--
-- 因此 (dag_instance_id, node_id, output_name) 不再是全局唯一，
-- 需要加入 attempt_no 形成 (dag_instance_id, node_id, output_name, attempt_no) 唯一。
--
-- 注意：attempt_no 可空（V11 前旧记录），PostgreSQL 中多个 NULL 视为不同值，不会冲突。
-- =============================================================================

-- 删除旧的唯一约束（V5 创建）
ALTER TABLE sr_dag_artifact
    DROP CONSTRAINT IF EXISTS uk_sr_dag_artifact;

-- 新建按 Attempt 隔离的唯一约束
ALTER TABLE sr_dag_artifact
    ADD CONSTRAINT uk_sr_dag_artifact_per_attempt
    UNIQUE (dag_instance_id, node_id, output_name, attempt_no);

COMMENT ON CONSTRAINT uk_sr_dag_artifact_per_attempt ON sr_dag_artifact IS
    '按 (dag_instance_id, node_id, output_name, attempt_no) 唯一；'
    '允许多次 Attempt 产出同名 output（§16.2 / §16.3）';
