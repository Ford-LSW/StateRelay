-- =============================================================================
-- V11: sr_dag_artifact 扩展空间 Artifact 状态机与元数据字段
--
-- 对齐 GIS-Worker与节点数据传递设计.md §16.1、§16.2、§4.2、§15.1：
--   1. 新增 status 字段，承载 SpatialArtifactState 状态机
--      CREATING=10 / STAGED=20 / AVAILABLE=30 / ORPHANED=40 / DELETING=50 / DELETED=60
--      —— Worker 不能直接提升为 AVAILABLE；只有通过当前 Attempt 完整围栏
--         校验通过后才能 STAGED → AVAILABLE，避免晚到 Attempt 覆盖当前权威结果。
--   2. 新增 format / object_key / checksum 字段，承载 VectorDatasetRef 容器属性
--      （调度侧不下传 object_key 给 Worker，统一以 artifactId 查询，§15.1 末段）。
--   3. 新增 attempt_id / attempt_no 字段，标识产生该 Artifact 的 Attempt
--      （用于 STAGED→AVAILABLE 推进时校验 current_attempt_id 完整围栏，§16.2）。
--   4. 新增 updated_at 字段，记录状态推进时间。
--
-- 兼容性：
--   - status 默认值 30 (AVAILABLE)，使旧记录自动视为可读（向后兼容）。
--   - 新字段均可空，旧记录无需回填。
-- =============================================================================

ALTER TABLE sr_dag_artifact
    ADD COLUMN IF NOT EXISTS status       INTEGER      NOT NULL DEFAULT 30,
    ADD COLUMN IF NOT EXISTS format       VARCHAR(32),
    ADD COLUMN IF NOT EXISTS object_key   TEXT,
    ADD COLUMN IF NOT EXISTS checksum     VARCHAR(128),
    ADD COLUMN IF NOT EXISTS attempt_id   VARCHAR(64),
    ADD COLUMN IF NOT EXISTS attempt_no   INTEGER,
    ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW();

-- status: 10=CREATING, 20=STAGED, 30=AVAILABLE, 40=ORPHANED, 50=DELETING, 60=DELETED
ALTER TABLE sr_dag_artifact
    DROP CONSTRAINT IF EXISTS ck_sr_dag_artifact_status;
ALTER TABLE sr_dag_artifact
    ADD CONSTRAINT ck_sr_dag_artifact_status CHECK (status IN (10, 20, 30, 40, 50, 60));

-- 清理扫描：按 status 查找 ORPHANED 待回收
CREATE INDEX IF NOT EXISTS idx_sr_dag_artifact_status
    ON sr_dag_artifact (status, dag_instance_id);

-- 按 artifactId 单点查询元数据（ArtifactMetadataClient 使用）
CREATE INDEX IF NOT EXISTS idx_sr_dag_artifact_id_status
    ON sr_dag_artifact (id, status);

COMMENT ON COLUMN sr_dag_artifact.status IS
    '10=CREATING, 20=STAGED, 30=AVAILABLE, 40=ORPHANED, 50=DELETING, 60=DELETED；'
    '§16.1/§16.2 状态机：STAGED 经 Attempt 围栏校验通过后才能 AVAILABLE；'
    '默认 30 (AVAILABLE) 保证旧记录可读';
COMMENT ON COLUMN sr_dag_artifact.format IS
    '容器物理格式，例如 FILE_GDB / SHAPEFILE；§4.2 VectorDatasetRef.format';
COMMENT ON COLUMN sr_dag_artifact.object_key IS
    '对象存储 Key，例如 dag/50001/A/attempt-1/source.gdb.zip；'
    '§15.1 调度侧不下传，Worker 通过 ArtifactMetadataClient 按 artifactId 查询';
COMMENT ON COLUMN sr_dag_artifact.checksum IS
    '校验和，例如 sha256:xxx；下载后完整性校验';
COMMENT ON COLUMN sr_dag_artifact.attempt_id IS
    '产生该 Artifact 的 Attempt ID；§16.2 STAGED→AVAILABLE 推进时校验 current_attempt_id';
COMMENT ON COLUMN sr_dag_artifact.attempt_no IS
    '产生该 Artifact 的 Attempt 序号；用于按 Attempt 精确清理';
COMMENT ON COLUMN sr_dag_artifact.updated_at IS
    '状态推进时间，用于 ORPHANED 清理调度';
