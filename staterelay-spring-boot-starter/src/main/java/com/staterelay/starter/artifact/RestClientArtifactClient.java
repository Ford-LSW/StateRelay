package com.staterelay.starter.artifact;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import com.staterelay.contract.dag.artifact.ArtifactStageRequest;
import com.staterelay.contract.handler.spi.ArtifactClient;
import com.staterelay.contract.handler.spi.ArtifactMetadataClient;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Worker 侧 {@link ArtifactClient} HTTP 实现（对齐文档 §15.1）。
 *
 * <p>通过 {@link RestClient} 调用调度中心两个数据传输端点：
 * <ul>
 *   <li>{@code GET  /internal/v1/artifacts/{artifactId}/data} —— 下载数据流</li>
 *   <li>{@code POST /internal/v1/artifacts/data}             —— 上传 multipart 数据流</li>
 * </ul>
 *
 * <p><b>第一版正确性流程（文档 §15.1）：</b>
 * <pre>
 *   Worker 通过 ArtifactMetadataClient 查询元数据 → 取得 objectKey + checksum
 *       ↓
 *   调用 {@link #download} 下载到本地 input 目录
 *       ↓
 *   校验 checksum（与服务端上报的一致）
 *       ↓
 *   Handler 执行算法
 *       ↓
 *   结果写入 output 目录
 *       ↓
 *   调用 {@link #upload} 上传，取得 objectKey + checksum
 *       ↓
 *   调用 ArtifactMetadataClient.stage 登记 STAGED artifact
 * </pre>
 *
 * <p><b>并发约束（文档 §14.2）：</b>
 * 原始 GDB Artifact 只读，B、C 并行各自下载独立副本到自己的 input 目录，
 * 互不覆盖；最终结果由 PACKAGE_GDB 节点组装。
 */
public class RestClientArtifactClient implements ArtifactClient {

    private static final Logger log = Logger.getLogger(RestClientArtifactClient.class.getName());

    private final RestClient restClient;
    private final ArtifactMetadataClient metadataClient;

    public RestClientArtifactClient(RestClient restClient, ArtifactMetadataClient metadataClient) {
        this.restClient = Objects.requireNonNull(restClient, "restClient");
        this.metadataClient = Objects.requireNonNull(metadataClient, "metadataClient");
    }

    /**
     * 下载指定 Artifact 到本地路径（§15.1）。
     *
     * <p>流程：
     * <ol>
     *   <li>通过 {@link ArtifactMetadataClient#query} 取得 checksum（用于校验）</li>
     *   <li>调用 {@code GET /data} 下载流到 {@code targetPath}</li>
     *   <li>校验下载内容的 SHA-256 与元数据 checksum 一致</li>
     * </ol>
     *
     * <p>下载失败抛 {@link ArtifactDownloadException}，由调用方按 §19.1 处理：
     * 网络临时异常可重试，checksum 不一致且无法恢复时最终失败。
     *
     * @param artifactId 容器 Artifact ID
     * @param targetPath 目标本地路径（通常位于 {@link com.staterelay.contract.handler.spi.WorkDirectory#inputDir()}）
     * @return 下载后的本地文件路径（即 {@code targetPath}）
     */
    @Override
    public Path download(Long artifactId, Path targetPath) {
        Objects.requireNonNull(artifactId, "artifactId");
        Objects.requireNonNull(targetPath, "targetPath");

        // 1. 查询元数据取得 checksum（§15.1）
        Optional<ArtifactMetadataResponse> metaOpt = metadataClient.query(artifactId);
        if (metaOpt.isEmpty()) {
            throw new ArtifactDownloadException(artifactId,
                    "Artifact not found or not AVAILABLE: " + artifactId);
        }
        ArtifactMetadataResponse metadata = metaOpt.get();
        String expectedChecksum = metadata.getChecksum();
        boolean directoryArtifact = isDirectoryArtifact(metadata.getFormat());
        Path downloadPath = directoryArtifact
                ? targetPath.resolveSibling(targetPath.getFileName() + ".download.zip")
                : targetPath;

        // 2. 下载流到本地文件
        try {
            Path parent = downloadPath.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Resource resource = restClient.get()
                    .uri("/internal/v1/artifacts/{artifactId}/data", artifactId)
                    .accept(MediaType.APPLICATION_OCTET_STREAM)
                    .retrieve()
                    .body(Resource.class);
            if (resource == null) {
                throw new ArtifactDownloadException(artifactId,
                        "Download returned empty body for artifactId=" + artifactId);
            }
            try (InputStream in = resource.getInputStream()) {
                Files.copy(in, downloadPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (HttpClientErrorException.NotFound notFound) {
            throw new ArtifactDownloadException(artifactId,
                    "Artifact data not found: " + artifactId, notFound);
        } catch (IOException e) {
            throw new ArtifactDownloadException(artifactId,
                    "下载 Artifact 文件失败: " + downloadPath, e);
        }

        // 3. 校验 checksum（§15.1，防止传输损坏或调度侧文件丢失）
        if (expectedChecksum != null && !expectedChecksum.isBlank()) {
            String actualChecksum = SafeZipSupport.sha256(downloadPath);
            if (!expectedChecksum.equals(actualChecksum)) {
                deleteQuietly(downloadPath);
                throw new ArtifactDownloadException(artifactId,
                        "Artifact 校验和不一致: expected=" + expectedChecksum
                                + ", actual=" + actualChecksum);
            }
        }

        if (directoryArtifact) {
            try {
                SafeZipSupport.unzip(downloadPath, targetPath);
            } finally {
                deleteQuietly(downloadPath);
            }
        }

        log.fine("Artifact downloaded: artifactId=" + artifactId + ", path=" + targetPath);
        return targetPath;
    }

    /**
     * 上传本地结果文件到对象存储（§15.1）。
     *
     * <p>流程：
     * <ol>
     *   <li>计算本地文件 SHA-256 checksum</li>
     *   <li>调用 {@code POST /data} multipart 上传，调度中心返回 objectKey + checksum</li>
     *   <li>填充 {@link ArtifactStageRequest#getObjectKey()} 与 {@link ArtifactStageRequest#getChecksum()}</li>
     * </ol>
     *
     * <p>Worker 拿到填充后的 request，再调用
     * {@link ArtifactMetadataClient#stage} 登记 STAGED 记录取得 artifactId。
     *
     * @param localPath 本地结果文件路径（通常位于 {@link com.staterelay.contract.handler.spi.WorkDirectory#outputDir()}）
     * @param request   Stage 请求，本方法填充 objectKey 和 checksum 后回传
     * @return 填充了 objectKey + checksum 的请求对象
     */
    @Override
    public ArtifactStageRequest upload(Path localPath, ArtifactStageRequest request) {
        Objects.requireNonNull(localPath, "localPath");
        Objects.requireNonNull(request, "request");
        if (!Files.exists(localPath)) {
            throw new IllegalStateException("Result file not found: " + localPath);
        }

        Path uploadPath = localPath;
        boolean temporaryArchive = Files.isDirectory(localPath);
        if (temporaryArchive) {
            uploadPath = localPath.resolveSibling(localPath.getFileName() + "-"
                    + UUID.randomUUID() + ".zip");
            SafeZipSupport.zipDirectory(localPath, uploadPath);
        }
        String localChecksum = SafeZipSupport.sha256(uploadPath);

        ArtifactStageRequest filled;
        try {
            MultiValueMap<String, Object> parts = createUploadParts(uploadPath, request);
            filled = restClient.post()
                    .uri("/internal/v1/artifacts/data")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(parts)
                    .retrieve()
                    .body(ArtifactStageRequest.class);
        } finally {
            if (temporaryArchive) {
                deleteQuietly(uploadPath);
            }
        }

        if (filled == null) {
            throw new IllegalStateException("Upload returned null response");
        }
        if (!localChecksum.equals(filled.getChecksum())) {
            throw new IllegalStateException("上传后的 Artifact 校验和不一致: expected="
                    + localChecksum + ", actual=" + filled.getChecksum());
        }

        // 3. 保留原 request 的 format / storageType 等字段（服务端返回的是新实例）
        if (filled.getFormat() == null) {
            filled.setFormat(request.getFormat());
        }
        if (filled.getStorageType() == null) {
            filled.setStorageType(request.getStorageType());
        }

        log.fine("Artifact uploaded: localPath=" + localPath
                + ", objectKey=" + filled.getObjectKey()
                + ", checksum=" + filled.getChecksum());
        return filled;
    }

    private MultiValueMap<String, Object> createUploadParts(Path uploadPath,
                                                             ArtifactStageRequest request) {
        if (request.getExecutionContext() == null) {
            throw new IllegalArgumentException("DAG Artifact 上传必须携带完整执行围栏");
        }
        com.staterelay.contract.dag.algorithm.WorkerExecutionContext context =
                request.getExecutionContext();
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("file", new FileSystemResource(uploadPath));
        parts.add("dagInstanceId", context.getDagInstanceId());
        parts.add("nodeInstanceId", context.getNodeInstanceId());
        parts.add("nodeCode", context.getNodeCode());
        parts.add("attemptId", context.getAttemptId());
        parts.add("attemptNo", context.getAttemptNo());
        parts.add("outputKey", request.getOutputKey());
        parts.add("requestId", context.getRequestId());
        parts.add("requestChecksum", context.getRequestChecksum());
        parts.add("dispatchGeneration", context.getDispatchGeneration());
        parts.add("dispatchToken", context.getDispatchToken());
        parts.add("attemptLeaseVersion", context.getAttemptLeaseVersion());
        parts.add("workerId", context.getWorkerId());
        parts.add("workerEpoch", context.getWorkerEpoch());
        return parts;
    }

    private boolean isDirectoryArtifact(String format) {
        if (format == null) {
            return false;
        }
        return "FILE_GDB".equalsIgnoreCase(format)
                || "DIRECTORY".equalsIgnoreCase(format)
                || format.toUpperCase(java.util.Locale.ROOT).endsWith("_DIRECTORY");
    }

    private void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warning("清理临时 Artifact 文件失败: " + path + ", 原因: " + e.getMessage());
        }
    }
}
