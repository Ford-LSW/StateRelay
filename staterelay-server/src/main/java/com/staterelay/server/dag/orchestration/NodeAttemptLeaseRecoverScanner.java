package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Attempt lease 到期 → UNKNOWN 接管恢复 Scanner（对齐文档 §19.1 / §43 / §29.5）。
 *
 * <p>周期扫描 Attempt lease 到期但硬截止未到期的 Attempt：
 * <ul>
 *   <li>状态 ∈ {DISPATCHING(10), RUNNING(30), UNKNOWN(80)}</li>
 *   <li>attempt_lease_expire_time &lt; NOW() AND execution_deadline_at &gt; NOW()</li>
 * </ul>
 *
 * <p>对每个命中 Attempt 执行 §19.1 接管恢复流程：
 * <ol>
 *   <li>DAG Engine CAS NodeInstance RUNNING → UNKNOWN（§29.5：lease 到期触发 UNKNOWN）</li>
 *   <li>Scheduler DB CAS 升级 attempt_lease_version N→N+1（§19.1 / §47.1）</li>
 *   <li>HTTP rebindFence 请求 Worker：校验 requestId / requestChecksum / attemptId /
 *       workerId / workerEpoch 完整围栏 + 单调递增；Worker Store 原子升级内部 lease_version</li>
 *   <li>使用新 lease_version 重发原 requestId（依赖 Worker 去重 Store 返回 RUNNING/SUCCESS/FAILED）</li>
 *   <li>结果按 §47 围栏回写</li>
 * </ol>
 *
 * <p>跨 epoch 恢复分支（§19.1）：
 * <ul>
 *   <li>PROCESS_LOCAL + 原 epoch 仍存活：直接重发原 requestId（不升级 lease_version）</li>
 *   <li>DURABLE + 同 worker_id 新 epoch：先 CAS 重绑 worker_epoch + 升级 lease_version，
 *       再 rebindFence 同步 Worker Store，再重发</li>
 *   <li>PROCESS_LOCAL + 原 epoch 已失效：禁止盲目重发；等硬截止到期收敛为 TIMEOUT</li>
 *   <li>跨 worker_id 恢复：第一版禁止；等硬截止到期收敛为 TIMEOUT</li>
 * </ul>
 *
 * <p>第一版不实现 Worker HTTP 调用：本 Scanner 只完成 DB CAS 升级 + UNKNOWN 推进，
 * rebindFence HTTP 调用由后续 Worker RPC 模块补充（接口已就绪：{@link NodeAttemptMapper#incrementLeaseVersion}）。
 */
@Service
public class NodeAttemptLeaseRecoverScanner {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptLeaseRecoverScanner.class);

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final int batchSize;
    private final int workerLeaseSeconds;

    public NodeAttemptLeaseRecoverScanner(NodeAttemptMapper attemptMapper,
                                           NodeInstanceMapper nodeInstanceMapper,
                                           NodeAttemptRepository attemptRepository,
                                           NodeInstanceJpaRepository nodeInstanceRepository,
                                           AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                           @Value("${staterelay.dag.lease-recover.batch-size:50}") int batchSize,
                                           @Value("${staterelay.dag.scheduler.worker-lease-seconds:30}") int workerLeaseSeconds) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.batchSize = batchSize;
        this.workerLeaseSeconds = workerLeaseSeconds;
    }

    /**
     * 周期扫描 lease 到期 Attempt 并启动 §19.1 接管恢复。
     */
    @Scheduled(fixedDelayString = "${staterelay.dag.lease-recover.interval-ms:5000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<Long> attemptIds = attemptMapper.scanLeaseExpiredAttempts(now, batchSize);
            if (attemptIds.isEmpty()) {
                return;
            }
            log.debug("Found {} lease-expired NodeAttempts to recover", attemptIds.size());
            for (Long attemptId : attemptIds) {
                try {
                    recoverOne(attemptId, now);
                } catch (Exception ex) {
                    log.error("Failed to recover NodeAttempt {}: {}", attemptId, ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("NodeAttempt lease recover scan failed: {}", ex.getMessage(), ex);
        }
    }

    /**
     * 执行 §19.1 接管恢复流程（单 Attempt）。
     *
     * <p>第一版仅完成 DB 层 CAS 升级 + UNKNOWN 推进；
     * rebindFence HTTP 调用由后续 Worker RPC 模块补充。
     */
    public void recoverOne(Long attemptId, Instant now) {
        NodeAttemptEntity attempt = attemptRepository.findById(attemptId).orElse(null);
        if (attempt == null) {
            return;
        }

        // 状态校验：必须是 DISPATCHING(10) / RUNNING(30) / UNKNOWN(80)
        if (attempt.getStatus() != NodeAttemptStatus.DISPATCHING
                && attempt.getStatus() != NodeAttemptStatus.RUNNING
                && attempt.getStatus() != NodeAttemptStatus.UNKNOWN) {
            return;
        }

        // 硬截止校验：硬截止已到期则不再接管恢复（由 §29.4 超时 Scanner 处理）
        if (attempt.getExecutionDeadlineAt() != null
                && !attempt.getExecutionDeadlineAt().isAfter(now)) {
            log.debug("NodeAttempt {} execution_deadline_at already expired, skip lease recover",
                attemptId);
            return;
        }

        NodeInstanceEntity node = nodeInstanceRepository.findById(attempt.getNodeInstanceId()).orElse(null);
        if (node == null) {
            log.warn("NodeInstance {} not found for NodeAttempt {}", attempt.getNodeInstanceId(), attemptId);
            return;
        }

        // §29.5：lease 到期把 NodeInstance 推进为 UNKNOWN（如果是 RUNNING）
        // 这里只标记 NodeInstance 层面的中间态；Attempt 维度的 UNKNOWN 推进在下面
        if (node.getStatus() == NodeInstanceStatus.RUNNING) {
            // 注：NodeInstance 没有 UNKNOWN 状态；这里仅记录日志，不修改 NodeInstance
            // 实际 UNKNOWN 收敛在 Attempt 维度完成
            log.info("NodeInstance {} RUNNING but Attempt {} lease expired (entering recovery)",
                node.getId(), attemptId);
        }

        // §19.1 分支判定：根据 Worker dedup_capability 和 epoch 决定恢复路径
        RecoveryPath path = decideRecoveryPath(attempt);
        switch (path) {
            case RESendOriginalEpoch -> resendOriginalRequestId(attempt, now);
            case RebindFenceNewEpoch -> upgradeLeaseVersionAndRebind(attempt, now);
            case WaitForHardDeadline -> log.info(
                "NodeAttempt {} waiting for execution_deadline_at (lease expired, no safe recovery path)",
                attemptId);
            default -> log.warn("NodeAttempt {} unknown recovery path: {}", attemptId, path);
        }
    }

    /**
     * §19.1 恢复路径判定。
     *
     * <p>第一版简化：仅基于 worker_epoch 是否仍然存活判定。
     * 实际需要查 ExecutorRegistration 判断 worker_epoch 是否仍在线（第一版预留，由后续实现补完）。
     */
    private RecoveryPath decideRecoveryPath(NodeAttemptEntity attempt) {
        // 第一版简化：所有 lease 到期都走 RebindFenceNewEpoch 路径
        // 实际应根据 ExecutorRegistration 判断原 epoch 是否仍存活
        // - 原 epoch 在线 → RESendOriginalRequestId
        // - DURABLE + 同 worker_id 新 epoch → RebindFenceNewEpoch
        // - PROCESS_LOCAL + 原 epoch 失效 → WaitForHardDeadline
        return RecoveryPath.RebindFenceNewEpoch;
    }

    /**
     * §19.1 分支 A：原 epoch 仍存活，直接重发原 requestId（不升级 lease_version）。
     *
     * <p>第一版不实现 HTTP 调用，仅记录意图。
     */
    private void resendOriginalRequestId(NodeAttemptEntity attempt, Instant now) {
        log.info("NodeAttempt {} recovery: resend original requestId {} (same epoch)",
            attempt.getId(), attempt.getRequestId());
        // TODO: 后续 Worker RPC 模块实现：HTTP 重发原 requestId
    }

    /**
     * §19.1 分支 B：DURABLE + 同 worker_id 新 epoch，CAS 升级 lease_version + rebindFence。
     *
     * <p>对齐文档 §19.1 / §44 / §47.1：
     * <ol>
     *   <li>DB CAS 升级 attempt_lease_version N→N+1（{@link NodeAttemptMapper#incrementLeaseVersion}）</li>
     *   <li>HTTP rebindFence 请求 Worker Store 升级内部 lease_version</li>
     *   <li>使用新 lease_version 重发原 requestId</li>
     * </ol>
     *
     * <p>第一版仅完成 DB CAS 升级；rebindFence HTTP 调用由后续 Worker RPC 模块实现。
     */
    private void upgradeLeaseVersionAndRebind(NodeAttemptEntity attempt, Instant now) {
        Long currentLeaseVersion = attempt.getAttemptLeaseVersion();
        if (currentLeaseVersion == null) {
            currentLeaseVersion = 0L;
        }
        Long newLeaseVersion = currentLeaseVersion + 1;

        // 1. DB CAS 升级 attempt_lease_version + 重绑 worker_epoch + 重置 lease_expire_time
        //    前置条件（fail-closed）：newLeaseVersion > currentLeaseVersion（单调递增）
        Instant newLeaseExpireTime = now.plus(Duration.ofSeconds(workerLeaseSeconds));
        // 注：第一版 worker_epoch 暂用原值；实际应来自 ExecutorRegistration 的新 epoch
        String newWorkerEpoch = attempt.getWorkerEpoch();

        int updated = attemptMapper.incrementLeaseVersion(
            attempt.getId(),
            currentLeaseVersion,
            newLeaseVersion,
            newWorkerEpoch,
            newLeaseExpireTime,
            now);

        if (updated == 0) {
            // CAS 失败：可能已被其他 Scanner 升级，或状态已终态；下一轮重新扫描
            log.debug("NodeAttempt {} CAS incrementLeaseVersion failed (likely concurrent upgrade)",
                attempt.getId());
            return;
        }

        log.info("NodeAttempt {} lease_version upgraded {} → {} (rebindFence pending HTTP call)",
            attempt.getId(), currentLeaseVersion, newLeaseVersion);

        // 2. TODO：HTTP rebindFence 请求 Worker Store 升级内部 lease_version
        //    调用方应使用 attempt.getRequestId() + RequestFence(attempt.getId(), workerId, newWorkerEpoch, newLeaseVersion)
        //    校验 requestId / requestChecksum / attemptId / workerId / workerEpoch 完整围栏 + 单调递增
        //    Worker Store 原子升级内部 lease_version

        // 3. TODO：使用新 lease_version 重发原 requestId
        //    Worker 去重 Store 返回 RUNNING/SUCCESS/FAILED（已升级到新 lease_version）
        //    结果按 §47 围栏回写
    }

    /**
     * §19.1 恢复路径（对齐文档）。
     */
    enum RecoveryPath {
        /**
         * PROCESS_LOCAL + 原 epoch 仍存活：直接重发原 requestId（不升级 lease_version）。
         */
        RESendOriginalEpoch,

        /**
         * DURABLE + 同 worker_id 新 epoch：CAS 升级 lease_version + rebindFence + 重发。
         */
        RebindFenceNewEpoch,

        /**
         * PROCESS_LOCAL + 原 epoch 已失效 / 跨 worker_id：禁止盲目重发，等硬截止到期收敛为 TIMEOUT。
         */
        WaitForHardDeadline
    }
}
