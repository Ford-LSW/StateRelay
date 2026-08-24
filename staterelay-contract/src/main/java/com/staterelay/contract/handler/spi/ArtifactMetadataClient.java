package com.staterelay.contract.handler.spi;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import com.staterelay.contract.dag.artifact.ArtifactStageRequest;
import com.staterelay.contract.dag.artifact.ArtifactStageResponse;

import java.util.Optional;

/**
 * Worker 侧 Artifact 元数据查询 SPI（对齐文档 §15.1）。
 *
 * <p><b>设计原则（文档 §15.1 末段）：</b>
 * 调度侧不下传 {@code objectKey} 给 Worker（避免 URL 失效时引用不一致），
 * 统一以 {@code artifactId} 作为稳定 ID 通过本 SPI 查询。
 *
 * <p>Worker 执行算法前：
 * <pre>
 *   根据 VectorLayerRef.artifactId 调用 {@link #query(Long)}
 *       → 取得 objectKey / checksum / format / status
 *       → 校验 status = AVAILABLE
 *       → 通过 {@link ArtifactClient#download} 下载并解压
 * </pre>
 *
 * <p>Worker 生成结果后：
 * <pre>
 *   调用 {@link #stage(ArtifactStageRequest)}
 *       → 取得新分配的 artifactId
 *       → 填入 VectorLayerRef.artifactId
 *       → 通过 TaskResultReport 回传调度侧
 * </pre>
 *
 * <p>调度侧收到 TaskResultReport 后由 SpatialArtifactService 推进 STAGED → AVAILABLE（文档 §16.2）。
 */
public interface ArtifactMetadataClient {

    /**
     * 按 artifactId 查询 Artifact 元数据。
     *
     * @param artifactId 容器 Artifact ID
     * @return 元数据；artifactId 不存在时返回 {@link Optional#empty()}
     */
    Optional<ArtifactMetadataResponse> query(Long artifactId);

    /**
     * 登记 STAGED Artifact 并取得调度中心分配的 artifactId。
     *
     * <p>Worker 完成上传后调用本方法登记一条 {@code status = STAGED} 记录，
     * 取得 artifactId 后填入 {@link com.staterelay.contract.dag.spatial.VectorLayerRef}。
     * 调度侧后续通过 Attempt 围栏校验后推进 STAGED → AVAILABLE。
     *
     * @param request 上报请求，包含 dagInstanceId / nodeCode / attemptId / objectKey / checksum 等
     * @return 调度中心分配的 artifactId 与 STAGED 状态
     */
    ArtifactStageResponse stage(ArtifactStageRequest request);
}
