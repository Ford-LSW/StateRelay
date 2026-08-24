package com.staterelay.contract.dag.artifact;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Worker 上报新 STAGED Artifact 的响应。
 *
 * <p>调度中心收到 {@link ArtifactStageRequest} 后：
 * <ul>
 *   <li>分配 {@code artifactId}</li>
 *   <li>写入 {@code sr_dag_artifact} 表，{@code status = STAGED}</li>
 *   <li>返回本响应</li>
 * </ul>
 *
 * <p>Worker 拿到 {@code artifactId} 后将其填入
 * {@link com.staterelay.contract.dag.spatial.VectorLayerRef#getArtifactId()} 或
 * {@link com.staterelay.contract.dag.spatial.VectorDatasetRef#getArtifactId()}，
 * 并通过 {@code TaskResultReport} 回传 outputs。
 *
 * <p>下游节点通过 {@link ArtifactMetadataResponse} 用同一个 {@code artifactId}
 * 查询 STAGED→AVAILABLE 推进后的容器元数据。
 */
@Data
@NoArgsConstructor
public class ArtifactStageResponse {

    /** 调度中心分配的 Artifact ID */
    private Long artifactId;

    /** 当前状态，固定为 {@link SpatialArtifactState#STAGED} */
    private SpatialArtifactState status;

    public ArtifactStageResponse(Long artifactId, SpatialArtifactState status) {
        this.artifactId = artifactId;
        this.status = status;
    }
}
