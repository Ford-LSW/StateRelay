package com.staterelay.starter.artifact;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import com.staterelay.contract.dag.artifact.ArtifactStageRequest;
import com.staterelay.contract.dag.artifact.ArtifactStageResponse;
import com.staterelay.contract.handler.spi.ArtifactMetadataClient;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Worker 侧 {@link ArtifactMetadataClient} HTTP 实现（对齐文档 §15.1）。
 *
 * <p>通过 {@link RestClient} 调用调度中心的两个端点：
 * <ul>
 *   <li>{@code GET  /internal/v1/artifacts/{artifactId}/metadata} —— 查询容器元数据</li>
 *   <li>{@code POST /internal/v1/artifacts}                        —— 登记 STAGED Artifact</li>
 * </ul>
 *
 * <p><b>设计原则（文档 §15.1 末段）：</b>
 * 调度侧不下传 {@code objectKey} 给 Worker，统一以 {@code artifactId} 作为稳定 ID 查询。
 * Worker 先通过 {@link #query(Long)} 取得 {@code objectKey} + {@code checksum}，
 * 再通过 {@code ArtifactClient} 下载数据并校验。
 *
 * <p><b>Stage 流程：</b>
 * Worker 上传结果文件后，调用 {@link #stage(ArtifactStageRequest)} 取得调度中心分配的
 * {@code artifactId}，填入 {@link com.staterelay.contract.dag.spatial.VectorLayerRef}，
 * 通过 {@code TaskResultReport} 回传 outputs。
 */
public class RestClientArtifactMetadataClient implements ArtifactMetadataClient {

    private static final Logger log = Logger.getLogger(RestClientArtifactMetadataClient.class.getName());

    private final RestClient restClient;

    public RestClientArtifactMetadataClient(RestClient restClient) {
        this.restClient = Objects.requireNonNull(restClient, "restClient");
    }

    /**
     * 按 artifactId 查询容器元数据（§15.1）。
     *
     * <p>仅返回 AVAILABLE 状态的记录；调度侧对不存在 / 不可读返回 404，
     * 本方法将 404 转换为 {@link Optional#empty()}，其它 HTTP 错误抛运行时异常。
     *
     * @param artifactId 容器 Artifact ID
     * @return 元数据；不存在或不可读时返回 empty
     */
    @Override
    public Optional<ArtifactMetadataResponse> query(Long artifactId) {
        Objects.requireNonNull(artifactId, "artifactId");
        try {
            ArtifactMetadataResponse response = restClient.get()
                    .uri("/internal/v1/artifacts/{artifactId}/metadata", artifactId)
                    .retrieve()
                    .body(ArtifactMetadataResponse.class);
            return Optional.ofNullable(response);
        } catch (HttpClientErrorException.NotFound notFound) {
            // Artifact 不存在或非 AVAILABLE，按 §16.2 下游读取前置条件返回 empty
            log.fine("Artifact not found or not AVAILABLE: artifactId=" + artifactId);
            return Optional.empty();
        }
    }

    /**
     * 登记 STAGED Artifact 并取得调度中心分配的 artifactId（§15.1 / §16.2）。
     *
     * <p>Worker 完成上传后调用本方法登记一条 {@code status = STAGED} 记录。
     * 调度侧后续通过 Attempt 围栏校验后推进 STAGED → AVAILABLE（§16.2）。
     *
     * @param request 上报请求，须包含 dagInstanceId / nodeCode / attemptId / objectKey / checksum 等
     * @return 调度中心分配的 artifactId 与 STAGED 状态
     */
    @Override
    public ArtifactStageResponse stage(ArtifactStageRequest request) {
        Objects.requireNonNull(request, "request");
        return restClient.post()
                .uri("/internal/v1/artifacts")
                .body(request)
                .retrieve()
                .body(ArtifactStageResponse.class);
    }
}
