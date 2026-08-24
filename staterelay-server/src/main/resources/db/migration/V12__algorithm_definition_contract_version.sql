-- =============================================================================
-- V12: sr_algorithm_definition 新增 contract_version / contract_checksum 字段
--
-- 对齐 GIS-Worker与节点数据传递设计.md §3.2、§3.1：
--   1. AlgorithmContract 需要稳定的 contractVersion 与 contractChecksum，
--      用于 Worker 心跳上报后调度中心校验契约一致性；
--   2. 不一致时该 Worker 不得接收该算法的新任务 + 记录 ALGORITHM_CONTRACT_MISMATCH 告警；
--   3. 现有 input_schema_json / output_schema_json 继续承载结构化 inputs/outputs Map。
--
-- 兼容性：
--   - 新字段均可空，旧算法定义默认 contract_version = '1.0'、contract_checksum = NULL。
--   - 调度侧校验时若 contract_checksum 为空，仅校验 contractVersion 一致。
-- =============================================================================

ALTER TABLE sr_algorithm_definition
    ADD COLUMN IF NOT EXISTS contract_version   VARCHAR(32) NOT NULL DEFAULT '1.0',
    ADD COLUMN IF NOT EXISTS contract_checksum   VARCHAR(128);

COMMENT ON COLUMN sr_algorithm_definition.contract_version IS
    '契约版本号，例如 1.0；§3.2 Worker 心跳上报 contractVersion 供校验，不一致不得接收任务';
COMMENT ON COLUMN sr_algorithm_definition.contract_checksum IS
    '契约 checksum（contractVersion + inputs + outputs 序列化哈希），可空；'
    '§3.2 防止 Worker 假冒实现；空时校验降级为版本号匹配';
