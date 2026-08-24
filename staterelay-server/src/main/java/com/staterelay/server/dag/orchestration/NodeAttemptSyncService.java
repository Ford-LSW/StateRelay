package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.mapper.dto.NodeCompletionSnapshot;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import com.staterelay.server.dag.service.SpatialArtifactService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * NodeAttempt 终态同步 Service（对齐文档 §19.1 事务边界 + §19.2 轮询模型 + §29 重试策略）。
 *
 * <p>第一版走轮询：周期扫描 {@code NodeAttempt 终态 + NodeInstance 仍 RUNNING} 的记录，
 * 在同一个事务内完成：
 * <ol>
 *   <li>读取 NodeAttempt 终态结果</li>
 *   <li>查 AlgorithmDefinition.max_retry / retry_interval_seconds</li>
 *   <li>判定是重试回退还是终态推进：
 *      <ul>
 *          <li>SUCCESS：CAS NodeInstance RUNNING → SUCCESS；同事务 +1 finished_node_count</li>
 *          <li>FAILED / TIMEOUT / UNKNOWN（已收敛为 TIMEOUT）：
 *              <ul>
 *                  <li>retry_count &lt; max_retry：CAS NodeInstance 回退 READY，retry_count + 1，
 *                      不增加 finished_node_count</li>
 *                  <li>retry_count &gt;= max_retry：CAS NodeInstance → FAILED / TIMEOUT，
 *                      同事务 +1 finished_node_count</li>
 *              </ul>
 *          </li>
 *      </ul>
 *   </li>
 * </ol>
 *
 * <p>UNKNOWN 状态的收敛由独立 Scanner 调用 NodeAttemptMapper.markUnknownAsTimeout 完成（§19.1）。
 *
 * <p>事务管理：使用 {@link TransactionTemplate} 显式开启事务，避免 Spring AOP 自调用代理失效问题。
 */
@Service
public class NodeAttemptSyncService {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptSyncService.class);

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final DagInstanceMapper dagInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final SpatialArtifactService spatialArtifactService;
    private final TransactionTemplate transactions;
    private final int batchSize;

    public NodeAttemptSyncService(NodeAttemptMapper attemptMapper,
                                   NodeInstanceMapper nodeInstanceMapper,
                                   DagInstanceMapper dagInstanceMapper,
                                   NodeAttemptRepository attemptRepository,
                                   NodeInstanceJpaRepository nodeInstanceRepository,
                                   AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                   SpatialArtifactService spatialArtifactService,
                                   TransactionTemplate transactions,
                                   @Value("${staterelay.dag.attempt-sync.batch-size:50}") int batchSize) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.dagInstanceMapper = dagInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.spatialArtifactService = spatialArtifactService;
        this.transactions = transactions;
        this.batchSize = batchSize;
    }

    /**
     * 周期扫描 NodeAttempt 终态但 NodeInstance 未推进的记录，统一同步。
     */
    @Scheduled(fixedDelayString = "${staterelay.dag.attempt-sync.interval-ms:1000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<Long> attemptIds = attemptMapper.scanTerminalAttempts(now, batchSize);
            if (attemptIds.isEmpty()) {
                return;
            }
            for (Long attemptId : attemptIds) {
                try {
                    syncOne(attemptId, now);
                } catch (Exception ex) {
                    log.error("Failed to sync NodeAttempt {}: {}", attemptId, ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("NodeAttempt sync scan failed: {}", ex.getMessage(), ex);
        }
    }

    /**
     * 同步单个 Attempt：把 Attempt 终态结果推进到 NodeInstance。
     *
     * <p>使用 {@link TransactionTemplate} 显式事务，确保 CAS + finished_node_count 同事务。
     */
    public void syncOne(Long attemptId, Instant now) {
        NodeAttemptEntity attempt = attemptRepository.findById(attemptId).orElse(null);
        if (attempt == null) {
            return;
        }
        NodeAttemptStatus attemptStatus = attempt.getStatus();
        if (attemptStatus != NodeAttemptStatus.SUCCESS
                && attemptStatus != NodeAttemptStatus.FAILED
                && attemptStatus != NodeAttemptStatus.TIMEOUT
                && attemptStatus != NodeAttemptStatus.UNKNOWN) {
            return;  // 非终态
        }
        NodeInstanceEntity node = nodeInstanceRepository.findById(attempt.getNodeInstanceId()).orElse(null);
        if (node == null || node.getStatus() != NodeInstanceStatus.RUNNING) {
            return;  // NodeInstance 已被其他线程推进
        }

        transactions.executeWithoutResult(status -> {
            switch (attemptStatus) {
                case SUCCESS -> applySuccess(attempt, node, now);
                case FAILED -> applyFailure(attempt, node, now);
                case TIMEOUT, UNKNOWN -> applyTimeout(attempt, node, now);
                default -> { /* 不会到这里 */ }
            }
        });
    }

    /**
     * 处理 SUCCESS（§29.1 + GIS-Worker §16.2 围栏校验）：
     * <ol>
     *   <li>CAS NodeInstance RUNNING → SUCCESS，含 {@code current_attempt_no} 围栏校验</li>
     *   <li>CAS 通过 → 当前权威 Attempt：批量推进 STAGED Artifact → AVAILABLE</li>
     *   <li>CAS 失败 → 晚到 Attempt 或已被其他线程推进：批量标记 STAGED Artifact → ORPHANED</li>
     *   <li>同事务 +1 finished_node_count；重置 schedule_fail_count</li>
     * </ol>
     *
     * <p>同时重置 schedule_fail_count = 0（§9.1 重置时机之一：Worker 返回非容量拒绝 SUCCESS）。
     */
    private void applySuccess(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        // §16.2 Attempt 围栏：current_attempt_no 必须匹配，防止晚到 Attempt 覆盖当前权威结果
        NodeCompletionSnapshot snapshot = nodeInstanceMapper.markSuccess(
            node.getId(),
            attempt.getAttemptNo(),
            attempt.getResultJson(),
            attempt.getResultRef(),
            now);
        if (snapshot == null) {
            // CAS 失败：晚到 Attempt 或已被其他线程推进（§16.3）
            // 将本 Attempt 的 STAGED Artifact 标记为 ORPHANED，等待清理回收
            int orphaned = spatialArtifactService.orphanArtifactsForAttempt(
                node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
            if (orphaned > 0) {
                log.info("Late attempt {} artifacts orphaned: node={}, attemptNo={}",
                    attempt.getId(), node.getNodeId(), attempt.getAttemptNo());
            }
            log.debug("NodeInstance {} SUCCESS CAS failed (late attempt or already finalized)", node.getId());
            return;
        }
        // CAS 通过：当前权威 Attempt，推进 STAGED Artifact → AVAILABLE（§16.2）
        spatialArtifactService.promoteArtifactsForAttempt(
            node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
        // 同事务 +1 finished_node_count（§39.1 T2）+ 重置 schedule_fail_count（§9.1）
        dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
        nodeInstanceMapper.resetScheduleFailCount(node.getId(), now);
        log.info("NodeInstance {} SUCCESS via attempt {} (attemptNo={}, artifacts promoted)",
            node.getId(), attempt.getId(), attempt.getAttemptNo());
    }

    /**
     * 处理 FAILED（§29.3）：根据 retry_count 和 max_retry 分流：
     * <ul>
     *   <li>retry_count &lt; max_retry：CAS NodeInstance → READY，retry_count + 1</li>
     *   <li>retry_count &gt;= max_retry：CAS NodeInstance → FAILED，+1 finished_node_count</li>
     * </ul>
     */
    private void applyFailure(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        AlgorithmDefinitionEntity algo = algorithmDefinitionRepository
            .findByAlgorithmCode(attempt.getAlgorithmCode())
            .orElse(null);
        int maxRetry = algo == null || algo.getMaxRetry() == null ? 0 : algo.getMaxRetry();
        int retryIntervalSec = algo == null || algo.getRetryIntervalSeconds() == null
            ? 10 : algo.getRetryIntervalSeconds();

        if (node.getRetryCount() != null && node.getRetryCount() < maxRetry) {
            // 重试：CAS NodeInstance → READY，retry_count + 1（§29.2 / §29.3）
            // 失败 Attempt 的 STAGED Artifact → ORPHANED（新 Attempt 会产出新成果）
            spatialArtifactService.orphanArtifactsForAttempt(
                node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
            Instant nextScheduleTime = now.plus(Duration.ofSeconds(retryIntervalSec));
            int updated = nodeInstanceMapper.revertToReady(node.getId(), nextScheduleTime, now);
            if (updated > 0) {
                log.info("NodeInstance {} FAILED but retrying (retry_count={}, max_retry={})",
                    node.getId(), node.getRetryCount() + 1, maxRetry);
            }
            return;
        }
        // 终态：CAS NodeInstance → FAILED（§29.3）
        // 失败 Attempt 的 STAGED Artifact → ORPHANED（§16.2 / §19.3）
        spatialArtifactService.orphanArtifactsForAttempt(
            node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
        int updated = nodeInstanceMapper.markFailed(
            node.getId(),
            attempt.getErrorCode(),
            attempt.getErrorMessage(),
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            // §9.1 重置时机：Worker 返回非容量拒绝 FAILED 结果（终态时重置）
            nodeInstanceMapper.resetScheduleFailCount(node.getId(), now);
            log.info("NodeInstance {} FAILED (terminal, retry_count={}, max_retry={})",
                node.getId(), node.getRetryCount(), maxRetry);
        }
    }

    /**
     * 处理 TIMEOUT / UNKNOWN（已收敛为 TIMEOUT，§29.4）：
     * <ul>
     *   <li>retry_count &lt; max_retry：CAS NodeInstance → READY，retry_count + 1</li>
     *   <li>retry_count &gt;= max_retry：CAS NodeInstance → TIMEOUT，+1 finished_node_count</li>
     * </ul>
     *
     * <p>UNKNOWN 状态在收敛 Scanner 中先被 {@code markUnknownAsTimeout} 转为 TIMEOUT，
     * 然后这里按 TIMEOUT 处理。
     */
    private void applyTimeout(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        AlgorithmDefinitionEntity algo = algorithmDefinitionRepository
            .findByAlgorithmCode(attempt.getAlgorithmCode())
            .orElse(null);
        int maxRetry = algo == null || algo.getMaxRetry() == null ? 0 : algo.getMaxRetry();
        int retryIntervalSec = algo == null || algo.getRetryIntervalSeconds() == null
            ? 10 : algo.getRetryIntervalSeconds();

        if (node.getRetryCount() != null && node.getRetryCount() < maxRetry) {
            // 重试：CAS NodeInstance → READY，retry_count + 1（§29.4）
            // 超时 Attempt 的 STAGED Artifact → ORPHANED（新 Attempt 会产出新成果）
            spatialArtifactService.orphanArtifactsForAttempt(
                node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
            Instant nextScheduleTime = now.plus(Duration.ofSeconds(retryIntervalSec));
            int updated = nodeInstanceMapper.revertToReady(node.getId(), nextScheduleTime, now);
            if (updated > 0) {
                log.info("NodeInstance {} TIMEOUT but retrying (retry_count={}, max_retry={})",
                    node.getId(), node.getRetryCount() + 1, maxRetry);
            }
            return;
        }
        // 终态：CAS NodeInstance → TIMEOUT（§29.4）
        // 超时 Attempt 的 STAGED Artifact → ORPHANED（§16.2 / §19.3）
        spatialArtifactService.orphanArtifactsForAttempt(
            node.getDagInstanceId(), node.getNodeId(), attempt.getAttemptNo(), now);
        String errorCode = attempt.getErrorCode() != null ? attempt.getErrorCode() : "TIMEOUT";
        String errorMessage = attempt.getErrorMessage() != null
            ? attempt.getErrorMessage()
            : "Node execution timed out (max_retry reached)";
        int updated = nodeInstanceMapper.markTimeout(
            node.getId(),
            errorCode,
            errorMessage,
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            // §9.1 重置时机：非容量拒绝 Attempt 按 TIMEOUT 收敛（终态时重置）
            nodeInstanceMapper.resetScheduleFailCount(node.getId(), now);
            log.info("NodeInstance {} TIMEOUT (terminal, retry_count={}, max_retry={})",
                node.getId(), node.getRetryCount(), maxRetry);
        }
    }
}
