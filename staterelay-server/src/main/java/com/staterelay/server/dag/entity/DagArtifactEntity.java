package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.artifact.SpatialArtifactState;
import com.staterelay.contract.dag.enums.DagArtifactType;
import com.staterelay.server.dag.converter.DagArtifactTypeConverter;
import com.staterelay.server.dag.converter.SpatialArtifactStateConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.Instant;

/**
 * DAG Artifact 引用（{@code sr_dag_artifact}）。
 *
 * <p>由 JPA Repository 处理简单 CRUD；查询按节点查所有产物也走 JPA。
 *
 * <p>对齐 GIS-Worker 设计文档 §4.2、§16.1、§16.2：扩展空间 Artifact 状态机与
 * 容器元数据字段。新字段可空，向后兼容旧业务 ArtifactRef。
 */
@Data
@Entity
@Table(name = "sr_dag_artifact")
public class DagArtifactEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_instance_id", nullable = false)
    private Long dagInstanceId;

    @Column(name = "node_id", nullable = false, length = 128)
    private String nodeId;

    @Column(name = "output_name", nullable = false, length = 128)
    private String outputName;

    @Column(name = "artifact_type", nullable = false)
    @Convert(converter = DagArtifactTypeConverter.class)
    private DagArtifactType artifactType;

    @Column(name = "storage_type", length = 32)
    private String storageType;

    @Column(name = "uri", columnDefinition = "TEXT")
    private String uri;

    @Column(name = "value_json", columnDefinition = "jsonb")
    private String valueJson;

    @Column(name = "metadata", columnDefinition = "jsonb")
    private String metadata;

    @Column(name = "parent_artifact_ids", columnDefinition = "BIGINT[]")
    private Long[] parentArtifactIds;

    // ===== V11 新增：空间 Artifact 状态机与容器元数据（对齐 §16.1、§4.2、§15.1） =====

    /**
     * Artifact 状态机（§16.1）。
     * <ul>
     *   <li>CREATING —— Worker 正在生成或上传</li>
     *   <li>STAGED —— 文件已生成并登记，但当前 Attempt 结果尚未被 Scheduler 确认</li>
     *   <li>AVAILABLE —— 当前权威 Attempt 已被接受，可被下游读取</li>
     *   <li>ORPHANED —— 旧 Attempt / 晚到 Attempt / 失败 Attempt 产生的非权威成果</li>
     *   <li>DELETING / DELETED —— 清理流程</li>
     * </ul>
     * 默认 AVAILABLE，保证旧记录可读。
     */
    @Column(name = "status", nullable = false)
    @Convert(converter = SpatialArtifactStateConverter.class)
    private SpatialArtifactState status = SpatialArtifactState.AVAILABLE;

    /** 容器物理格式，例如 FILE_GDB / SHAPEFILE（§4.2 VectorDatasetRef.format） */
    @Column(name = "format", length = 32)
    private String format;

    /**
     * 对象存储 Key，例如 {@code dag/50001/A/attempt-1/source.gdb.zip}（§15.1）。
     * 调度侧不下传给 Worker，由 Worker 通过 {@code ArtifactMetadataClient} 按 id 查询。
     */
    @Column(name = "object_key", columnDefinition = "TEXT")
    private String objectKey;

    /** 校验和，例如 {@code sha256:xxx}，下载后完整性校验 */
    @Column(name = "checksum", length = 128)
    private String checksum;

    /**
     * 产生该 Artifact 的 Attempt ID（§16.2）。
     * STAGED → AVAILABLE 推进时校验 current_attempt_id 完整围栏。
     */
    @Column(name = "attempt_id", length = 64)
    private String attemptId;

    /** 产生该 Artifact 的 Attempt 序号，用于按 Attempt 精确清理 */
    @Column(name = "attempt_no")
    private Integer attemptNo;

    /** 登记 Artifact 时使用的请求标识。 */
    @Column(name = "request_id", length = 128)
    private String requestId;

    /** 请求业务参数摘要。 */
    @Column(name = "request_checksum", length = 128)
    private String requestChecksum;

    /** 调度代次。 */
    @Column(name = "dispatch_generation")
    private Long dispatchGeneration;

    /** 调度令牌。 */
    @Column(name = "dispatch_token", length = 128)
    private String dispatchToken;

    /** Attempt 租约版本。 */
    @Column(name = "attempt_lease_version")
    private Long attemptLeaseVersion;

    /** Worker 标识。 */
    @Column(name = "worker_id", length = 160)
    private String workerId;

    /** Worker 启动代次。 */
    @Column(name = "worker_epoch", length = 64)
    private String workerEpoch;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** 状态推进时间，用于 ORPHANED 清理调度 */
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
