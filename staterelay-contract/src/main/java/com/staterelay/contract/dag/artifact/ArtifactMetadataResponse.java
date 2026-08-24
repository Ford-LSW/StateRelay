package com.staterelay.contract.dag.artifact;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.staterelay.contract.dag.spatial.VectorStorageType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Objects;

/**
 * Artifact 元数据查询响应（对齐文档 §15.1、§4.4）。
 *
 * <p>Worker 通过 {@link com.staterelay.contract.dag.artifact.ArtifactMetadataQuery} 调用
 * {@code ArtifactMetadataClient}（HTTP 端点）按 {@code artifactId} 查询容器的元数据。
 *
 * <p><b>设计原则（文档 §15.1 末段）：</b>
 * 调度侧不下传 {@code objectKey} 给 Worker（避免 URL 失效时引用不一致），
 * 统一以 {@code artifactId} 作为稳定 ID 查询。
 *
 * <p>典型字段对应 §4.2 VectorDatasetRef 的容器属性：
 * <pre>{@code
 * {
 *   "artifactId": 20001,
 *   "status": "AVAILABLE",
 *   "storageType": "OBJECT_STORAGE",
 *   "format": "FILE_GDB",
 *   "objectKey": "dag/50001/A/attempt-1/source.gdb.zip",
 *   "checksum": "sha256:xxx",
 *   "dagInstanceId": 50001,
 *   "createdAt": "2026-08-21T10:00:00Z"
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ArtifactMetadataResponse {

    /** 容器 Artifact ID */
    private Long artifactId;

    /** Artifact 当前状态，下游读取前必须为 {@link SpatialArtifactState#AVAILABLE} */
    private SpatialArtifactState status;

    /** 存储类型，决定下载方式 */
    private VectorStorageType storageType;

    /** 容器物理格式，例如 {@code FILE_GDB} */
    private String format;

    /** 对象存储 Key（{@link VectorStorageType#OBJECT_STORAGE} 时必填） */
    private String objectKey;

    /** 校验和，例如 {@code sha256:xxx}，下载后用于完整性校验 */
    private String checksum;

    /** 所属 DAG 实例 ID，用于 scoping 和清理 */
    private Long dagInstanceId;

    /** 所属节点编码，用于 scoping 和清理 */
    private String nodeId;

    /** 所属 Attempt 编号，用于 scoping 和清理 */
    private Integer attemptNo;

    /** 创建时间 */
    private Instant createdAt;

    public ArtifactMetadataResponse(Long artifactId, SpatialArtifactState status,
                                     VectorStorageType storageType) {
        this.artifactId = Objects.requireNonNull(artifactId, "artifactId");
        this.status = Objects.requireNonNull(status, "status");
        this.storageType = Objects.requireNonNull(storageType, "storageType");
    }
}
