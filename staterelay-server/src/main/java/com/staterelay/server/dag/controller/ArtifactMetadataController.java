package com.staterelay.server.dag.controller;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import com.staterelay.contract.dag.artifact.ArtifactStageRequest;
import com.staterelay.contract.dag.artifact.ArtifactStageResponse;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.server.dag.service.SpatialArtifactService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Optional;

/**
 * Artifact 元数据 HTTP 端点（对齐 GIS-Worker 设计文档 §15.1 / §16.2）。
 *
 * <p>Worker 通过本控制器与调度中心交互空间成果：
 * <ul>
 *   <li>{@code POST /internal/v1/artifacts}：Worker 上报新 STAGED Artifact，取得 artifactId</li>
 *   <li>{@code GET  /internal/v1/artifacts/{artifactId}/metadata}：Worker 按 artifactId 查询容器元数据</li>
 *   <li>{@code GET  /internal/v1/artifacts/{artifactId}/data}：Worker 下载 Artifact 数据流（§15.1）</li>
 *   <li>{@code POST /internal/v1/artifacts/data}：Worker 上传结果数据流（§15.1，返回 objectKey + checksum）</li>
 * </ul>
 *
 * <p><b>设计原则（§15.1 末段）：</b>
 * 调度侧不下传 {@code objectKey} 给 Worker，统一以 {@code artifactId} 作为稳定 ID 查询。
 * Worker 通过 {@code ArtifactMetadataClient} SPI 调用本端点取得 {@code objectKey} 和 {@code checksum}，
 * 再自行下载并校验压缩包。
 *
 * <p><b>状态推进约束（§16.2）：</b>
 * Worker 只能创建 STAGED 记录（{@link #stage}）；
 * STAGED → AVAILABLE 由 {@link SpatialArtifactService#promoteArtifactsForAttempt}
 * 在 Attempt 围栏校验通过后推进，Worker 无权直接提升。
 */
@RestController
@RequestMapping("/internal/v1/artifacts")
public class ArtifactMetadataController {

    private final SpatialArtifactService artifactService;

    public ArtifactMetadataController(SpatialArtifactService artifactService) {
        this.artifactService = artifactService;
    }

    /**
     * Worker 上报新 STAGED Artifact（§15.1 / §16.2）。
     *
     * <p>Worker 完成算法执行并上传结果文件后调用：
     * <ol>
     *   <li>调度中心分配 {@code artifactId}</li>
     *   <li>写入 {@code sr_dag_artifact}，{@code status = STAGED}</li>
     *   <li>返回 {@link ArtifactStageResponse}，Worker 填入 outputs Map</li>
     * </ol>
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ArtifactStageResponse stage(@RequestBody ArtifactStageRequest request) {
        return artifactService.stageArtifact(request);
    }

    /**
     * Worker 通过 artifactId 查询容器元数据（§15.1 末段）。
     *
     * <p>仅返回 {@code status = AVAILABLE} 的记录（下游读取前置条件，§16.2）。
     * 不存在或不可读时返回 404。
     */
    @GetMapping(value = "/{artifactId}/metadata",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ArtifactMetadataResponse getMetadata(@PathVariable Long artifactId) {
        ArtifactMetadataResponse response = artifactService.findMetadata(artifactId);
        if (response == null) {
            throw new ArtifactNotFoundException(artifactId);
        }
        return response;
    }

    /**
     * Worker 下载 Artifact 数据流（§15.1）。
     *
     * <p>第一版调度中心代理对象存储下载；生产环境可直接返回 302 重定向到
     * 预签名 URL，由 Worker 直连对象存储。
     *
     * <p>仅返回 AVAILABLE 状态的 Artifact 数据；不存在或文件丢失时返回 404。
     */
    @GetMapping(value = "/{artifactId}/data")
    public ResponseEntity<InputStreamResource> downloadData(@PathVariable Long artifactId) {
        Optional<java.io.InputStream> data = artifactService.openDataStream(artifactId);
        if (data.isEmpty()) {
            throw new ArtifactNotFoundException(artifactId);
        }
        ArtifactMetadataResponse meta = artifactService.findMetadata(artifactId);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        if (meta != null && meta.getObjectKey() != null) {
            String fileName = meta.getObjectKey().substring(meta.getObjectKey().lastIndexOf('/') + 1);
            headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
        }
        return new ResponseEntity<>(new InputStreamResource(data.get()), headers, HttpStatus.OK);
    }

    /**
     * Worker 上传结果数据流（§15.1）。
     *
     * <p>调度中心代理接收 multipart 上传，计算 checksum 并生成 objectKey，
     * 不在此处创建 STAGED 记录；Worker 拿到 objectKey + checksum 后再调用
     * {@link #stage} 登记 STAGED Artifact。
     *
     * @param file              结果文件（multipart）
     * @param dagInstanceId     所属 DAG 实例 ID
     * @param nodeCode          节点编码
     * @param attemptId         Attempt ID
     * @param attemptNo         Attempt 序号
     * @param outputKey         算法输出端口名
     * @return 填充了 objectKey + checksum 的 Stage 请求
     */
    @PostMapping(value = "/data", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ArtifactStageRequest uploadData(
            @RequestParam("file") MultipartFile file,
            @RequestParam("dagInstanceId") Long dagInstanceId,
            @RequestParam("nodeCode") String nodeCode,
            @RequestParam("attemptId") String attemptId,
            @RequestParam("attemptNo") Integer attemptNo,
            @RequestParam("outputKey") String outputKey,
            @RequestParam("nodeInstanceId") Long nodeInstanceId,
            @RequestParam("requestId") String requestId,
            @RequestParam("requestChecksum") String requestChecksum,
            @RequestParam("dispatchGeneration") Long dispatchGeneration,
            @RequestParam("dispatchToken") String dispatchToken,
            @RequestParam("attemptLeaseVersion") Long attemptLeaseVersion,
            @RequestParam("workerId") String workerId,
            @RequestParam("workerEpoch") String workerEpoch) {
        try {
            WorkerExecutionContext context = new WorkerExecutionContext();
            context.setDagInstanceId(dagInstanceId);
            context.setNodeInstanceId(nodeInstanceId);
            context.setNodeCode(nodeCode);
            context.setAttemptId(attemptId);
            context.setAttemptNo(attemptNo);
            context.setRequestId(requestId);
            context.setRequestChecksum(requestChecksum);
            context.setDispatchGeneration(dispatchGeneration);
            context.setDispatchToken(dispatchToken);
            context.setAttemptLeaseVersion(attemptLeaseVersion);
            context.setWorkerId(workerId);
            context.setWorkerEpoch(workerEpoch);
            ArtifactStageRequest request = new ArtifactStageRequest(
                    context, outputKey,
                    com.staterelay.contract.dag.spatial.VectorStorageType.OBJECT_STORAGE);
            request.setFormat("FILE_GDB");
            return artifactService.uploadDataStream(file.getInputStream(), request);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read uploaded file: " + e.getMessage(), e);
        }
    }

    /**
     * Artifact 不存在或不可读时抛出（§16.2 下游读取前置条件）。
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public static class ArtifactNotFoundException extends RuntimeException {
        public ArtifactNotFoundException(Long artifactId) {
            super("Artifact not found or not AVAILABLE: " + artifactId);
        }
    }
}
