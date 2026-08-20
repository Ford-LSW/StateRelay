-- =============================================================================
-- V8: DAG 状态机封版修订
--
-- 对齐 DAG_Engine_Scheduler_Executor_设计总结_第一版封版修订.md：
--   1. DagInstance 状态扩展：新增 CANCELLING(15) / FAILING(18) 中间态
--   2. DagInstance 新增 finished_node_count / cancel_reason 字段
--   3. NodeInstance 新增 schedule_fail_count / last_schedule_error_* / cancel_reason 字段
--   4. 替换 RUNNING 单态索引为活跃态索引（RUNNING/CANCELLING/FAILING）
--   5. 新增 CANCELLING / FAILING 部分索引，方便 scanner 扫描
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. DagInstance 状态机扩展：放宽 CHECK 约束到 7 态
-- -----------------------------------------------------------------------------
ALTER TABLE sr_dag_instance
    DROP CONSTRAINT IF EXISTS ck_sr_dag_instance_status;

ALTER TABLE sr_dag_instance
    ADD CONSTRAINT ck_sr_dag_instance_status
        CHECK (status IN (0, 10, 15, 18, 20, 30, 40));

COMMENT ON COLUMN sr_dag_instance.status IS
    '0=INIT, 10=RUNNING, 15=CANCELLING, 18=FAILING, 20=SUCCESS, 30=FAILED, 40=CANCELLED';

-- -----------------------------------------------------------------------------
-- 2. DagInstance 新增 finished_node_count / cancel_reason 字段
-- -----------------------------------------------------------------------------
ALTER TABLE sr_dag_instance
    ADD COLUMN IF NOT EXISTS finished_node_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sr_dag_instance
    ADD COLUMN IF NOT EXISTS cancel_reason VARCHAR(64);

COMMENT ON COLUMN sr_dag_instance.finished_node_count IS
    '已完成节点数（任何终态都 +1，重试不 +1）；对齐文档 §39';
COMMENT ON COLUMN sr_dag_instance.cancel_reason IS
    '取消原因（USER_CANCELLED），仅在 CANCELLING / CANCELLED 状态下有值';

-- -----------------------------------------------------------------------------
-- 3. NodeInstance 新增调度失败统计 + 取消原因字段
-- -----------------------------------------------------------------------------
ALTER TABLE sr_dag_node_instance
    ADD COLUMN IF NOT EXISTS schedule_fail_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sr_dag_node_instance
    ADD COLUMN IF NOT EXISTS last_schedule_error_code VARCHAR(64);

ALTER TABLE sr_dag_node_instance
    ADD COLUMN IF NOT EXISTS last_schedule_error_message VARCHAR(512);

ALTER TABLE sr_dag_node_instance
    ADD COLUMN IF NOT EXISTS cancel_reason VARCHAR(64);

COMMENT ON COLUMN sr_dag_node_instance.schedule_fail_count IS
    'Scheduler 调度失败次数（无可用 Worker），与 retry_count 严格分开；对齐文档 §9.1';
COMMENT ON COLUMN sr_dag_node_instance.last_schedule_error_code IS
    '最近一次调度失败错误码（如 NO_AVAILABLE_WORKER）';
COMMENT ON COLUMN sr_dag_node_instance.last_schedule_error_message IS
    '最近一次调度失败错误描述';
COMMENT ON COLUMN sr_dag_node_instance.cancel_reason IS
    '取消原因（USER_CANCELLED / DAG_FAILED），仅在 CANCELLED 状态下有值';

-- -----------------------------------------------------------------------------
-- 4. 索引调整：替换原 RUNNING 单态索引为活跃态索引
-- -----------------------------------------------------------------------------
-- 原 idx_sr_dag_instance_running 只扫 status=10；现在 scanner 需要扫 RUNNING/CANCELLING/FAILING
DROP INDEX IF EXISTS idx_sr_dag_instance_running;

CREATE INDEX idx_sr_dag_instance_active
    ON sr_dag_instance (last_progress_time, id)
    WHERE status IN (10, 15, 18);

COMMENT ON INDEX idx_sr_dag_instance_active IS
    'DAG Engine Scanner 扫描活跃实例（RUNNING/CANCELLING/FAILING）；对齐文档 §35.1';

-- -----------------------------------------------------------------------------
-- 5. CANCELLING / FAILING 部分索引，便于诊断与单独扫描
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sr_dag_instance_cancelling
    ON sr_dag_instance (last_progress_time, id)
    WHERE status = 15;

CREATE INDEX IF NOT EXISTS idx_sr_dag_instance_failing
    ON sr_dag_instance (last_progress_time, id)
    WHERE status = 18;

-- -----------------------------------------------------------------------------
-- 6. NodeInstance 新增 next_schedule_time 复合索引（READY 节点调度领取）
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sr_dag_node_ready_schedule
    ON sr_dag_node_instance (next_schedule_time, id)
    WHERE status = 10;

COMMENT ON INDEX idx_sr_dag_node_ready_schedule IS
    'Scheduler 领取 READY Node 的扫描索引（含 next_schedule_time 退避）';
