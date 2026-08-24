package com.staterelay.contract.dag.artifact;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.staterelay.contract.dag.spatial.VectorStorageType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * Worker 上报新 STAGED Artifact 请求（对齐文档 §15.1）。
 *
 * <p>Worker 完成算法执行、上传结果文件后，先调用
 * {@code POST /staterelay/internal/v1/artifacts} 提交本请求登记一条
 * {@link SpatialArtifactState#STAGED} 记录，调度中心返回分配的 {@code artifactId}。
 *
 * <p>Worker 取得 {@code artifactId} 后填入 {@link com.staterelay.contract.dag.spatial.VectorLayerRef}
 * 或 {@link com.staterelay.contract.dag.spatial.VectorDatasetRef} 的 outputs Map，
 * 通过 {@code TaskResultReport} 回传调度侧。
 *
 * <p><b>状态推进约束（文档 §16.2）：</b>
 * Worker 只能创建 STAGED 记录，不能直接提升为 AVAILABLE。
 * 只有 Scheduler 校验当前 Attempt 完整围栏通过后，才由调度侧推进 STAGED → AVAILABLE。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ArtifactStageRequest {

    /** 所属 DAG 实例 ID（Worker 从 executionContext 取得） */
    private Long dagInstanceId;

    /** 所属节点编码（Worker 从 executionContext 取得） */
    private String nodeCode;

    /** 所属 Attempt ID（Worker 从 executionContext 取得） */
    private String attemptId;

    /** 所属 Attempt 编号（Worker 从 executionContext 取得） */
    private Integer attemptNo;

    /** 算法输出端口名，例如 {@code resultLayer}（用于回填 outputs Map） */
    private String outputKey;

    /** 存储类型，{@link VectorStorageType#OBJECT_STORAGE} 表示对象存储容器 */
    private VectorStorageType storageType;

    /** 容器物理格式，例如 {@code FILE_GDB} */
    private String format;

    /** 对象存储 Key */
    private String objectKey;

    /** 校验和，例如 {@code sha256:xxx} */
    private String checksum;

    public ArtifactStageRequest(Long dagInstanceId, String nodeCode, String attemptId,
                                Integer attemptNo, String outputKey,
                                VectorStorageType storageType) {
        this.dagInstanceId = Objects.requireNonNull(dagInstanceId, "dagInstanceId");
        this.nodeCode = Objects.requireNonNull(nodeCode, "nodeCode");
        this.attemptId = Objects.requireNonNull(attemptId, "attemptId");
        this.attemptNo = Objects.requireNonNull(attemptNo, "attemptNo");
        this.outputKey = Objects.requireNonNull(outputKey, "outputKey");
        this.storageType = Objects.requireNonNull(storageType, "storageType");
    }
}
