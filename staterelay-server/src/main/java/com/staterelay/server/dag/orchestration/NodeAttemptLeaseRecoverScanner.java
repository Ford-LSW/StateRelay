package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.contract.protocol.RebindFenceRequest;
import com.staterelay.contract.protocol.RebindFenceResponse;
import com.staterelay.contract.protocol.RequestStatusResponse;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import com.staterelay.server.dag.worker.WorkerHttpClient;
import com.staterelay.server.dag.worker.WorkerHttpException;
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
 *   <li>Scheduler DB CAS 升级 attempt_lease_version N→N+1（§19.1 / §47.1）</li>
 *   <li>HTTP rebindFence 请求 Worker：校验 requestId / requestChecksum / attemptId /
 *       workerId / workerEpoch 完整围栏 + 单调递增；Worker Store 原子升级内部 lease_version</li>
 *   <li>HTTP 查询 requestId 当前状态（"使用原 requestId 重发"语义，§19.1）</li>
 *   <li>根据 Worker 响应按 §47 围栏回写 NodeAttempt（{@link NodeAttemptMapper#markSuccessFromRecovery}
 *       / {@link NodeAttemptMapper#markFailedFromRecovery} CAS）</li>
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
 * <p>HTTP 失败 fail-closed：网络错误 / 超时 / Worker 5xx 一律抛 {@link WorkerHttpException}，
 * Scanner 捕获后等下一轮重试或硬截止到期收敛为 TIMEOUT（§29.4）。
 */
@Service
public class NodeAttemptLeaseRecoverScanner {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptLeaseRecoverScanner.class);

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final WorkerHttpClient workerHttpClient;
    private final int batchSize;
    private final int workerLeaseSeconds;

    public NodeAttemptLeaseRecoverScanner(NodeAttemptMapper attemptMapper,
                                           NodeInstanceMapper nodeInstanceMapper,
                                           NodeAttemptRepository attemptRepository,
                                           NodeInstanceJpaRepository nodeInstanceRepository,
                                           AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                           WorkerHttpClient workerHttpClient,
                                           @Value("${staterelay.dag.lease-recover.batch-size:50}") int batchSize,
                                           @Value("${staterelay.dag.scheduler.worker-lease-seconds:30}") int workerLeaseSeconds) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.workerHttpClient = workerHttpClient;
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
     * <p>第一版简化：所有 lease 到期都走 RebindFenceNewEpoch 路径
     * （兼容同 epoch 和跨 epoch 场景；Worker Store 不在线时 HTTP 失败由 fail-closed 兜底）。
     * 实际生产应查 ExecutorRegistration 判断原 epoch 是否仍存活：
     * <ul>
     *   <li>原 epoch 在线 → RESendOriginalEpoch（直接重发，不升级 lease_version）</li>
     *   <li>DURABLE + 同 worker_id 新 epoch → RebindFenceNewEpoch（CAS 升级 + rebindFence + 重发）</li>
     *   <li>PROCESS_LOCAL + 原 epoch 失效 → WaitForHardDeadline</li>
     * </ul>
     */
    private RecoveryPath decideRecoveryPath(NodeAttemptEntity attempt) {
        return RecoveryPath.RebindFenceNewEpoch;
    }

    /**
     * §19.1 分支 A：原 epoch 仍存活，直接重发原 requestId（不升级 lease_version）。
     *
     * <p>实现：HTTP 调用 Worker /status 端点查询 requestId 当前状态（§19.1 "重发原 requestId"语义）。
     * 根据 Worker 端响应：
     * <ul>
     *   <li>present=true + state=RUNNING：Worker 仍在执行，等下一轮或硬截止</li>
     *   <li>present=true + state=SUCCESS：按 §47 围栏回写 NodeAttempt SUCCESS</li>
     *   <li>present=true + state=FAILED：按 §47 围栏回写 NodeAttempt FAILED</li>
     *   <li>present=false：Worker 端记录丢失（PROCESS_LOCAL + Pod 重启），等硬截止收敛</li>
     * </ul>
     */
    private void resendOriginalRequestId(NodeAttemptEntity attempt, Instant now) {
        log.info("NodeAttempt {} recovery: resend original requestId {} (same epoch)",
            attempt.getId(), attempt.getRequestId());

        RequestStatusResponse status;
        try {
            status = workerHttpClient.findRequestStatus(
                attempt.getWorkerAddress(), attempt.getRequestId());
        } catch (WorkerHttpException ex) {
            log.warn("NodeAttempt {} resendOriginalRequestId HTTP failed: {}",
                attempt.getId(), ex.getMessage());
            return;  // 下一轮重试
        }

        applyWorkerStatusToAttempt(attempt, status, attempt.getAttemptLeaseVersion(), now);
    }

    /**
     * §19.1 分支 B：DURABLE + 同 worker_id 新 epoch，CAS 升级 lease_version + rebindFence + 重发。
     *
     * <p>对齐文档 §19.1 / §44 / §47.1：
     * <ol>
     *   <li>DB CAS 升级 attempt_lease_version N→N+1（{@link NodeAttemptMapper#incrementLeaseVersion}）</li>
     *   <li>HTTP rebindFence 请求 Worker Store 升级内部 lease_version（§44.2 单调递增 + 完整围栏校验）</li>
     *   <li>HTTP 查询 requestId 状态（"使用新 lease_version 重发原 requestId"语义）</li>
     *   <li>根据 Worker 响应按 §47 围栏回写 NodeAttempt（CAS 带 lease_version 围栏）</li>
     * </ol>
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

        log.info("NodeAttempt {} lease_version upgraded {} → {} (proceeding to rebindFence HTTP)",
            attempt.getId(), currentLeaseVersion, newLeaseVersion);

        // 2. HTTP rebindFence 请求 Worker Store 升级内部 lease_version（§44.2）
        RebindFenceRequest rebindRequest = new RebindFenceRequest(
            attempt.getRequestId(),
            attempt.getRequestChecksum(),
            attempt.getId(),
            attempt.getWorkerId(),
            newWorkerEpoch,
            newLeaseVersion);

        try {
            RebindFenceResponse response = workerHttpClient.rebindFence(
                attempt.getWorkerAddress(), rebindRequest);
            if (!response.success()) {
                log.warn("NodeAttempt {} rebindFence rejected by Worker: {}; wait for hard deadline",
                    attempt.getId(), response.reason());
                return;  // 等硬截止收敛
            }
            log.info("NodeAttempt {} rebindFence success: Worker Store upgraded to leaseVersion={}",
                attempt.getId(), newLeaseVersion);
        } catch (WorkerHttpException ex) {
            log.warn("NodeAttempt {} rebindFence HTTP failed: {}; DB lease_version upgraded but Worker Store stale",
                attempt.getId(), ex.getMessage());
            return;  // 下一轮重试（DB 已升级，下一轮 Scanner 会重新查状态）
        }

        // 3. HTTP 查询 requestId 状态（"使用新 lease_version 重发原 requestId"语义）
        RequestStatusResponse status;
        try {
            status = workerHttpClient.findRequestStatus(
                attempt.getWorkerAddress(), attempt.getRequestId());
        } catch (WorkerHttpException ex) {
            log.warn("NodeAttempt {} findRequestStatus HTTP failed after rebindFence: {}",
                attempt.getId(), ex.getMessage());
            return;  // Worker Store 已升级；下一轮 Scanner 会重新查询
        }

        // 4. 按 §47 围栏回写 NodeAttempt（使用新 lease_version 作为围栏）
        applyWorkerStatusToAttempt(attempt, status, newLeaseVersion, now);
    }

    /**
     * 根据 Worker 端响应状态，按 §47 围栏回写 NodeAttempt 终态。
     *
     * <p>使用 {@link NodeAttemptMapper#markSuccessFromRecovery} /
     * {@link NodeAttemptMapper#markFailedFromRecovery} CAS：
     * <ul>
     *   <li>CAS 条件：状态 ∈ {DISPATCHING(10), RUNNING(30), UNKNOWN(80)}
     *       AND attempt_lease_version = #{leaseVersion}（防旧版本反向覆盖）</li>
     *   <li>CAS 失败：lease_version 不匹配（被其他 Scanner 升级）或状态已终态，等下一轮</li>
     * </ul>
     */
    private void applyWorkerStatusToAttempt(NodeAttemptEntity attempt,
                                             RequestStatusResponse status,
                                             Long leaseVersion,
                                             Instant now) {
        if (!status.present()) {
            log.warn("NodeAttempt {} Worker Store has no record for requestId={} "
                    + "(PROCESS_LOCAL + Pod restart?); wait for execution_deadline_at to converge to TIMEOUT",
                attempt.getId(), attempt.getRequestId());
            return;
        }

        switch (status.state()) {
            case "RUNNING":
                log.info("NodeAttempt {} Worker still RUNNING for requestId={}; wait for heartbeat or hard deadline",
                    attempt.getId(), attempt.getRequestId());
                return;
            case "SUCCESS":
                int updated = attemptMapper.markSuccessFromRecovery(
                    attempt.getId(),
                    leaseVersion,
                    status.resultJson(),
                    status.resultRef(),
                    now);
                if (updated > 0) {
                    log.info("NodeAttempt {} SUCCESS via recovery (leaseVersion={})",
                        attempt.getId(), leaseVersion);
                } else {
                    log.warn("NodeAttempt {} markSuccessFromRecovery CAS failed (concurrent upgrade or terminal)",
                        attempt.getId());
                }
                return;
            case "FAILED":
                int failed = attemptMapper.markFailedFromRecovery(
                    attempt.getId(),
                    leaseVersion,
                    status.errorCode() == null ? "WORKER_REPORTED_FAILED" : status.errorCode(),
                    status.errorMessage() == null ? "Worker reported FAILED via recovery" : status.errorMessage(),
                    now);
                if (failed > 0) {
                    log.info("NodeAttempt {} FAILED via recovery (leaseVersion={})",
                        attempt.getId(), leaseVersion);
                } else {
                    log.warn("NodeAttempt {} markFailedFromRecovery CAS failed (concurrent upgrade or terminal)",
                        attempt.getId());
                }
                return;
            default:
                log.warn("NodeAttempt {} unknown Worker state: {}", attempt.getId(), status.state());
        }
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
