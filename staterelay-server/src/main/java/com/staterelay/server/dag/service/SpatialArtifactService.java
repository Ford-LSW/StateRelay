package com.staterelay.server.dag.service;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import com.staterelay.contract.dag.artifact.ArtifactStageRequest;
import com.staterelay.contract.dag.artifact.ArtifactStageResponse;
import com.staterelay.contract.dag.artifact.SpatialArtifactState;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.dag.enums.DagArtifactType;
import com.staterelay.server.dag.entity.DagArtifactEntity;
import com.staterelay.server.dag.mapper.DagArtifactMapper;
import com.staterelay.server.dag.repository.DagArtifactRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * 空间 Artifact 服务（对齐 GIS-Worker 设计文档 §15.1 / §16.1 / §16.2 / §19.4）。
 *
 * <p>承担空间成果的完整生命周期管理：
 * <ol>
 *   <li>{@link #stageArtifact}：Worker 上报新 STAGED Artifact（§15.1 / §16.2）</li>
 *   <li>{@link #promoteArtifactsForAttempt}：当前权威 Attempt SUCCESS 后批量 STAGED → AVAILABLE（§16.2）</li>
 *   <li>{@link #orphanArtifactsForAttempt}：晚到 / 失败 Attempt 的 STAGED → ORPHANED（§16.2 / §16.3）</li>
 *   <li>{@link #findMetadata}：Worker 通过 artifactId 查询容器元数据（§15.1 末段）</li>
 *   <li>{@link #cleanupOrphaned}：ORPHANED → DELETING → DELETED 回收（§19.4）</li>
 * </ol>
 *
 * <p><b>关键不变式（§16.2）：</b>
 * <ul>
 *   <li>Worker 只能创建 STAGED，不能直接 AVAILABLE</li>
 *   <li>只有通过当前 Attempt 完整围栏的结果才能使 Artifact → AVAILABLE</li>
 *   <li>晚到 Attempt 的成果 → ORPHANED，由清理程序回收</li>
 * </ul>
 *
 * <p>insert 走 JPA Repository（简单操作，返回 ID）；
 * 状态机 CAS、按 Attempt 批量推进、元数据投影走 MyBatis Mapper（复杂 SQL）。
 */
@Service
public class SpatialArtifactService {

    private static final Logger log = LoggerFactory.getLogger(SpatialArtifactService.class);

    private final DagArtifactRepository artifactRepository;
    private final DagArtifactMapper artifactMapper;
    private final Path storageBaseDir;

    public SpatialArtifactService(DagArtifactRepository artifactRepository,
                                   DagArtifactMapper artifactMapper,
                                   @Value("${staterelay.dag.artifact-storage-dir:./artifact-storage}") String storageDir) {
        this.artifactRepository = artifactRepository;
        this.artifactMapper = artifactMapper;
        this.storageBaseDir = Paths.get(storageDir).toAbsolutePath().normalize();
        log.info("SpatialArtifactService initialized: storageDir={}", storageBaseDir);
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
     *
     * <p>使用独立事务（{@link Propagation#REQUIRES_NEW}），确保即使后续 Attempt 回滚，
     * STAGED 记录仍然保留（供晚到判断 / 清理流程使用）。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ArtifactStageResponse stageArtifact(ArtifactStageRequest request) {
        WorkerExecutionContext context = requireCurrentFence(request);
        validateIdentifier(request.getOutputKey(), "outputKey");
        synchronizeOwnership(request, context);
        verifyUploadedObject(request);
        if (!artifactMapper.matchesActiveFence(context)) {
            throw new IllegalStateException("登记 Artifact 时执行围栏已失效");
        }

        Optional<DagArtifactEntity> existing = artifactRepository
                .findByAttemptIdAndOutputName(context.getAttemptId(), request.getOutputKey());
        if (existing.isPresent()) {
            DagArtifactEntity artifact = existing.get();
            if (!Objects.equals(artifact.getChecksum(), request.getChecksum())
                    || !Objects.equals(artifact.getObjectKey(), request.getObjectKey())) {
                throw new IllegalStateException("同一 Attempt 输出已使用不同内容登记");
            }
            return new ArtifactStageResponse(artifact.getId(), artifact.getStatus());
        }

        Instant now = Instant.now();
        DagArtifactEntity entity = new DagArtifactEntity();
        entity.setDagInstanceId(request.getDagInstanceId());
        entity.setNodeId(request.getNodeCode());
        entity.setOutputName(request.getOutputKey());
        entity.setArtifactType(DagArtifactType.OBJECT_STORAGE);
        entity.setStorageType(request.getStorageType() != null ? request.getStorageType().name() : null);
        entity.setUri(request.getObjectKey());
        entity.setStatus(SpatialArtifactState.STAGED);
        entity.setFormat(request.getFormat());
        entity.setObjectKey(request.getObjectKey());
        entity.setChecksum(request.getChecksum());
        entity.setAttemptId(request.getAttemptId());
        entity.setAttemptNo(request.getAttemptNo());
        entity.setRequestId(context.getRequestId());
        entity.setRequestChecksum(context.getRequestChecksum());
        entity.setDispatchGeneration(context.getDispatchGeneration());
        entity.setDispatchToken(context.getDispatchToken());
        entity.setAttemptLeaseVersion(context.getAttemptLeaseVersion());
        entity.setWorkerId(context.getWorkerId());
        entity.setWorkerEpoch(context.getWorkerEpoch());
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);

        DagArtifactEntity saved = artifactRepository.save(entity);
        log.info("Artifact staged: id={}, dagInstanceId={}, node={}, attemptNo={}, outputKey={}",
            saved.getId(), request.getDagInstanceId(), request.getNodeCode(),
            request.getAttemptNo(), request.getOutputKey());

        return new ArtifactStageResponse(saved.getId(), SpatialArtifactState.STAGED);
    }

    /**
     * 批量推进当前权威 Attempt 的 STAGED Artifact → AVAILABLE（§16.2）。
     *
     * <p>调用时机：NodeAttemptSyncService 在 Attempt SUCCESS 且通过围栏校验后，同事务调用。
     * 围栏校验在调用方完成（attemptNo == node.currentAttemptNo）。
     *
     * @return 推进为 AVAILABLE 的行数
     */
    public int promoteArtifactsForAttempt(Long dagInstanceId, String nodeId,
                                          Integer attemptNo, Instant now) {
        int promoted = artifactMapper.promoteStagedToAvailableByAttempt(
            dagInstanceId, nodeId, attemptNo, now);
        if (promoted > 0) {
            log.info("Promoted {} STAGED artifacts to AVAILABLE: dagInstanceId={}, node={}, attemptNo={}",
                promoted, dagInstanceId, nodeId, attemptNo);
        }
        return promoted;
    }

    /**
     * 批量标记晚到 / 失败 Attempt 的 STAGED Artifact → ORPHANED（§16.2 / §16.3）。
     *
     * <p>调用时机：
     * <ul>
     *   <li>NodeAttemptSyncService 发现晚到 Attempt（attemptNo != currentAttemptNo）</li>
     *   <li>Attempt 终态为 FAILED / TIMEOUT 且不重试</li>
     * </ul>
     *
     * @return 标记为 ORPHANED 的行数
     */
    public int orphanArtifactsForAttempt(Long dagInstanceId, String nodeId,
                                         Integer attemptNo, Instant now) {
        int orphaned = artifactMapper.markStagedAsOrphanedByAttempt(
            dagInstanceId, nodeId, attemptNo, now);
        if (orphaned > 0) {
            log.info("Orphaned {} STAGED artifacts: dagInstanceId={}, node={}, attemptNo={}",
                orphaned, dagInstanceId, nodeId, attemptNo);
        }
        return orphaned;
    }

    /**
     * Worker 通过 artifactId 查询容器元数据（§15.1 末段）。
     *
     * <p>调度侧不下传 objectKey 给 Worker，统一以 artifactId 作为稳定 ID 查询。
     * 仅返回 AVAILABLE 状态的记录（下游读取前置条件，§16.2）。
     *
     * @return 元数据响应；不存在或不可读时返回 null
     */
    public ArtifactMetadataResponse findMetadata(Long artifactId) {
        return artifactMapper.findMetadataById(artifactId);
    }

    /**
     * 回收 ORPHANED Artifact（§19.4）。
     *
     * <p>由 Artifact Cleanup Scanner 定期调用：
     * <ol>
     *   <li>扫描 {@code updated_at < before} 的 ORPHANED 记录</li>
     *   <li>CAS ORPHANED → DELETING（防止并发回收）</li>
     *   <li>物理删除对象存储文件（由调用方完成）</li>
     *   <li>CAS DELETING → DELETED</li>
     * </ol>
     *
     * @param retention 租约保留期（ORPHANED 后经过该时间才回收）
     * @param batchSize 单批回收上限
     * @return 本轮标记为 DELETING 的 Artifact ID 列表
     */
    public List<Long> cleanupOrphaned(java.time.Duration retention, int batchSize) {
        Instant before = Instant.now().minus(retention);
        List<Long> orphanedIds = artifactMapper.scanOrphaned(before, batchSize);
        Instant now = Instant.now();
        for (Long id : orphanedIds) {
            int updated = artifactMapper.markOrphanedAsDeleting(id, now);
            if (updated > 0) {
                log.info("Artifact {} marked DELETING (orphaned cleanup)", id);
            }
        }
        return orphanedIds;
    }

    /**
     * 标记 DELETING → DELETED（物理删除完成后调用，§19.4）。
     */
    public void markDeleted(Long artifactId) {
        artifactMapper.markDeletingAsDeleted(artifactId, Instant.now());
    }

    // ===== 第一版对象存储代理（§15.1 下载 / 上传） =====

    /**
     * 按 artifactId 下载 Artifact 数据流（§15.1）。
     *
     * <p>第一版使用本地文件系统模拟对象存储；生产环境替换为 S3/MinIO SDK。
     * 仅返回 AVAILABLE 状态的 Artifact 数据。
     *
     * @return 数据流；Artifact 不存在 / 不可读 / 文件丢失时返回 empty
     */
    public Optional<InputStream> openDataStream(Long artifactId) {
        ArtifactMetadataResponse meta = artifactMapper.findMetadataById(artifactId);
        if (meta == null || meta.getObjectKey() == null) {
            return Optional.empty();
        }
        Path file = resolveObjectKey(meta.getObjectKey());
        if (!Files.exists(file)) {
            log.warn("Artifact data file not found: artifactId={}, objectKey={}", artifactId, meta.getObjectKey());
            return Optional.empty();
        }
        try {
            return Optional.of(Files.newInputStream(file));
        } catch (IOException e) {
            log.error("Failed to open artifact data stream: artifactId=" + artifactId, e);
            return Optional.empty();
        }
    }

    /**
     * 上传数据流到对象存储，计算 checksum 并填充 request（§15.1）。
     *
     * <p>第一版使用本地文件系统模拟对象存储；生产环境替换为 S3/MinIO SDK。
     *
     * @param inputStream 数据流
     * @param request     Stage 请求，本方法填充 objectKey 和 checksum
     * @return 填充了 objectKey + checksum 的请求对象
     */
    public ArtifactStageRequest uploadDataStream(InputStream inputStream, ArtifactStageRequest request) {
        Objects.requireNonNull(inputStream, "inputStream");
        WorkerExecutionContext context = requireCurrentFence(request);
        validateIdentifier(request.getOutputKey(), "outputKey");
        synchronizeOwnership(request, context);
        Path temporary = null;
        try {
            String objectKey = generateObjectKey(request);
            Path target = resolveObjectKey(objectKey);
            Files.createDirectories(target.getParent());
            temporary = target.resolveSibling(target.getFileName() + ".upload-" + UUID.randomUUID());
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            long bytes = copyWithDigest(inputStream, temporary, sha256);
            String checksum = "sha256:" + bytesToHex(sha256.digest());
            if (!artifactMapper.matchesActiveFence(context)) {
                Files.deleteIfExists(temporary);
                throw new IllegalStateException("上传完成时执行围栏已失效");
            }
            publishUploadedObject(temporary, target, checksum);
            temporary = null;
            request.setObjectKey(objectKey);
            request.setChecksum(checksum);
            log.info("Artifact data uploaded: objectKey={}, checksum={}, bytes={}", objectKey, checksum, bytes);
            return request;
        } catch (Exception e) {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanupError) {
                    e.addSuppressed(cleanupError);
                }
            }
            throw new IllegalStateException("Failed to upload artifact data: " + e.getMessage(), e);
        }
    }

    /** 原子发布上传对象；同一 Attempt 的重复上传只允许内容完全一致。 */
    private void publishUploadedObject(Path temporary, Path target, String checksum)
            throws IOException {
        if (Files.exists(target)) {
            if (!Objects.equals(checksum(target), checksum)) {
                throw new IllegalStateException("同一 Attempt 输出禁止覆盖为不同内容");
            }
            Files.deleteIfExists(temporary);
            return;
        }
        try {
            Files.move(temporary, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target);
        } catch (java.nio.file.FileAlreadyExistsException exception) {
            if (!Objects.equals(checksum(target), checksum)) {
                throw new IllegalStateException("并发上传的同一 Attempt 输出内容不一致", exception);
            }
            Files.deleteIfExists(temporary);
        }
    }

    private Path resolveObjectKey(String objectKey) {
        Path resolved = storageBaseDir.resolve(objectKey).normalize();
        if (!resolved.startsWith(storageBaseDir)) {
            throw new IllegalArgumentException("Artifact objectKey 超出存储根目录");
        }
        return resolved;
    }

    private String generateObjectKey(ArtifactStageRequest request) {
        return String.format("dag/%d/%s/attempt-%s/%s.bin",
            request.getDagInstanceId(), request.getNodeCode(),
            request.getAttemptId(), request.getOutputKey());
    }

    /** 校验待登记对象确实由当前 Attempt 上传，且物理内容摘要未被替换。 */
    private void verifyUploadedObject(ArtifactStageRequest request) {
        String expectedKey = generateObjectKey(request);
        if (!Objects.equals(expectedKey, request.getObjectKey())) {
            throw new IllegalArgumentException("objectKey 不属于当前 Attempt 输出");
        }
        Path object = resolveObjectKey(expectedKey);
        if (!Files.isRegularFile(object)) {
            throw new IllegalStateException("待登记的 Artifact 数据不存在");
        }
        String actualChecksum = checksum(object);
        if (!Objects.equals(actualChecksum, request.getChecksum())) {
            throw new IllegalStateException("Artifact checksum 与已上传内容不一致");
        }
    }

    /** 计算服务端存储对象的 SHA-256 摘要。 */
    private String checksum(Path path) {
        try (InputStream input = Files.newInputStream(path)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
            return "sha256:" + bytesToHex(digest.digest());
        } catch (Exception exception) {
            throw new IllegalStateException("无法校验 Artifact checksum", exception);
        }
    }

    /** 校验请求携带的完整 DAG 执行围栏仍为当前权威执行。 */
    private WorkerExecutionContext requireCurrentFence(ArtifactStageRequest request) {
        Objects.requireNonNull(request, "request");
        WorkerExecutionContext context = Objects.requireNonNull(
                request.getExecutionContext(), "executionContext");
        Objects.requireNonNull(context.getDagInstanceId(), "dagInstanceId");
        Objects.requireNonNull(context.getNodeInstanceId(), "nodeInstanceId");
        validateIdentifier(context.getNodeCode(), "nodeCode");
        validateIdentifier(context.getAttemptId(), "attemptId");
        Objects.requireNonNull(context.getAttemptNo(), "attemptNo");
        Objects.requireNonNull(context.getRequestId(), "requestId");
        Objects.requireNonNull(context.getRequestChecksum(), "requestChecksum");
        Objects.requireNonNull(context.getDispatchGeneration(), "dispatchGeneration");
        Objects.requireNonNull(context.getDispatchToken(), "dispatchToken");
        Objects.requireNonNull(context.getAttemptLeaseVersion(), "attemptLeaseVersion");
        Objects.requireNonNull(context.getWorkerId(), "workerId");
        Objects.requireNonNull(context.getWorkerEpoch(), "workerEpoch");
        if (!artifactMapper.matchesActiveFence(context)) {
            throw new IllegalStateException("Artifact 执行围栏不匹配或 DAG 已不再运行");
        }
        return context;
    }

    /** 以完整执行上下文覆盖可由旧客户端单独传入的归属字段。 */
    private void synchronizeOwnership(ArtifactStageRequest request,
                                      WorkerExecutionContext context) {
        request.setDagInstanceId(context.getDagInstanceId());
        request.setNodeCode(context.getNodeCode());
        request.setAttemptId(context.getAttemptId());
        request.setAttemptNo(context.getAttemptNo());
    }

    /** 限制路径组成字段只能使用安全的物理名称字符。 */
    private void validateIdentifier(String value, String field) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{1,128}")) {
            throw new IllegalArgumentException(field + " 含有非法字符");
        }
    }

    private long copyWithDigest(InputStream in, Path target, MessageDigest digest) throws IOException {
        try (var out = Files.newOutputStream(target)) {
            byte[] buffer = new byte[8192];
            long total = 0;
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
                digest.update(buffer, 0, read);
                total += read;
            }
            return total;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
