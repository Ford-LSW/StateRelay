package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * Worker 本地容量拒绝处理 Service（对齐文档 §27.2 / T6C）。
 *
 * <p>对齐文档 §27.2：Worker 在进入 Handler 前返回 CAPACITY_REJECTED，
 * 与"无 Worker"区分：
 * <ul>
 *   <li>已创建 Attempt（DISPATCHING → RUNNING 之后的 NodeInstance 已是 RUNNING）</li>
 *   <li>Attempt FAILED + error_code=CAPACITY_REJECTED（T6C 事务）</li>
 *   <li>NodeInstance RUNNING → READY（schedule_fail_count + 1）或 → FAILED（达到上限）</li>
 *   <li>不计 retry_count；不创建新 Attempt（下次调度重新创建）</li>
 *   <li>不重置 attempt_lease_version（旧 Attempt 已 FAILED，下次调度创建新 Attempt）</li>
 * </ul>
 *
 * <p>事务边界 T6C（§39.1）：
 * <ol>
 *   <li>CAS NodeAttempt RUNNING → FAILED + error_code=CAPACITY_REJECTED</li>
 *   <li>CAS NodeInstance RUNNING → READY（schedule_fail_count + 1）或 → FAILED（达上限）</li>
 *   <li>不 +1 finished_node_count（仅终态才 +1）</li>
 * </ol>
 *
 * <p>调度失败回退 vs 容量拒绝的区分：
 * <ul>
 *   <li>§9.1 调度失败：未创建 Attempt，DISPATCHING → READY</li>
 *   <li>§27.2 容量拒绝：已创建 Attempt，Attempt FAILED + NodeInstance RUNNING → READY/FAILED</li>
 * </ul>
 */
@Service
public class NodeAttemptCapacityRejectedService {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptCapacityRejectedService.class);

    /**
     * Worker 容量拒绝的标准 error_code（对齐文档 §27.2）。
     */
    public static final String ERROR_CODE_CAPACITY_REJECTED = "CAPACITY_REJECTED";

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final DagInstanceMapper dagInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final TransactionTemplate transactions;
    private final int maxScheduleFailCount;
    private final int capacityRejectedRetryIntervalSeconds;

    public NodeAttemptCapacityRejectedService(NodeAttemptMapper attemptMapper,
                                                NodeInstanceMapper nodeInstanceMapper,
                                                DagInstanceMapper dagInstanceMapper,
                                                NodeAttemptRepository attemptRepository,
                                                NodeInstanceJpaRepository nodeInstanceRepository,
                                                TransactionTemplate transactions,
                                                @Value("${staterelay.dag.scheduler.max-schedule-fail-count:10}") int maxScheduleFailCount,
                                                @Value("${staterelay.dag.scheduler.capacity-rejected-retry-interval-seconds:5}") int capacityRejectedRetryIntervalSeconds) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.dagInstanceMapper = dagInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.transactions = transactions;
        this.maxScheduleFailCount = maxScheduleFailCount;
        this.capacityRejectedRetryIntervalSeconds = capacityRejectedRetryIntervalSeconds;
    }

    /**
     * 处理 Worker 容量拒绝回报（§27.2 / T6C 事务）。
     *
     * <p>调用方：Worker HTTP 回报接口收到 CAPACITY_REJECTED 后调用本方法。
     *
     * <p>必须完整围栏校验：attempt_id + attempt_lease_version + worker_id + worker_epoch 全部匹配，
     * 防止旧 epoch Worker 误回报或重复回报（对齐文档 §47 围栏规则）。
     *
     * @param attemptId    NodeAttempt ID
     * @param leaseVersion Attempt lease 围栏版本号
     * @param workerId     Worker ID
     * @param workerEpoch  Worker 重启周期
     * @param now          当前时间
     * @return true 表示已成功推进；false 表示围栏不匹配或状态不合法
     */
    public boolean handleCapacityRejected(Long attemptId,
                                           Long leaseVersion,
                                           String workerId,
                                           String workerEpoch,
                                           Instant now) {
        NodeAttemptEntity attempt = attemptRepository.findById(attemptId).orElse(null);
        if (attempt == null) {
            log.warn("NodeAttempt {} not found for capacity rejection", attemptId);
            return false;
        }

        // 围栏校验（§47）：lease_version + worker_id + worker_epoch 必须全部匹配
        if (!matchesFence(attempt, leaseVersion, workerId, workerEpoch)) {
            log.warn("NodeAttempt {} fence mismatch for capacity rejection "
                    + "(stored leaseVersion={}, incoming={}, workerId={}, workerEpoch={})",
                attemptId, attempt.getAttemptLeaseVersion(), leaseVersion, workerId, workerEpoch);
            return false;
        }

        // 状态校验：必须是 RUNNING(30) 或 DISPATCHING(10) 才允许容量拒绝
        if (attempt.getStatus() != NodeAttemptStatus.RUNNING
                && attempt.getStatus() != NodeAttemptStatus.DISPATCHING) {
            log.debug("NodeAttempt {} not in RUNNING/DISPATCHING (status={}), skip capacity rejection",
                attemptId, attempt.getStatus());
            return false;
        }

        NodeInstanceEntity node = nodeInstanceRepository.findById(attempt.getNodeInstanceId()).orElse(null);
        if (node == null) {
            log.warn("NodeInstance {} not found for NodeAttempt {}", attempt.getNodeInstanceId(), attemptId);
            return false;
        }

        // 状态校验：NodeInstance 必须是 RUNNING(40)
        if (node.getStatus() != NodeInstanceStatus.RUNNING) {
            log.debug("NodeInstance {} not RUNNING (status={}), skip capacity rejection",
                node.getId(), node.getStatus());
            return false;
        }

        transactions.executeWithoutResult(status ->
            applyCapacityRejected(attempt, node, now));
        return true;
    }

    /**
     * T6C 事务：Attempt FAILED + NodeInstance 回退/终态（§27.2 / §39.1 T6C）。
     */
    private void applyCapacityRejected(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        // 1. CAS NodeAttempt RUNNING → FAILED + error_code=CAPACITY_REJECTED
        int attemptUpdated = attemptMapper.markFailed(
            attempt.getId(),
            ERROR_CODE_CAPACITY_REJECTED,
            "Worker reported CAPACITY_REJECTED before entering Handler",
            now);
        if (attemptUpdated == 0) {
            log.debug("NodeAttempt {} CAS RUNNING → FAILED (CAPACITY_REJECTED) failed; "
                + "likely already terminal", attempt.getId());
            return;
        }

        // 2. CAS NodeInstance：RUNNING → READY（未达上限）/ FAILED（达上限）
        int newFailCount = (node.getScheduleFailCount() == null ? 0 : node.getScheduleFailCount()) + 1;
        if (newFailCount < maxScheduleFailCount) {
            Instant nextScheduleTime = now.plus(Duration.ofSeconds(capacityRejectedRetryIntervalSeconds));
            int updated = nodeInstanceMapper.revertToReadyForCapacityRejected(
                node.getId(),
                newFailCount,
                nextScheduleTime,
                ERROR_CODE_CAPACITY_REJECTED,
                "Worker CAPACITY_REJECTED (schedule_fail_count=" + newFailCount + "/" + maxScheduleFailCount + ")",
                now);
            if (updated > 0) {
                log.info("NodeInstance {} reverted to READY after CAPACITY_REJECTED "
                    + "(schedule_fail_count={}/{}, attemptId={})",
                    node.getId(), newFailCount, maxScheduleFailCount, attempt.getId());
            }
            return;
        }

        // 达上限：CAS NodeInstance → FAILED（§27.2 终态）
        // 不调用 markFailedFromDispatching（NodeInstance 当前是 RUNNING 不是 DISPATCHING）
        // 复用 markFailed（CAS RUNNING → FAILED）
        int updated = nodeInstanceMapper.markFailed(
            node.getId(),
            ERROR_CODE_CAPACITY_REJECTED,
            "Worker CAPACITY_REJECTED after " + newFailCount + " schedule failures",
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            log.error("NodeInstance {} FAILED (CAPACITY_REJECTED, schedule_fail_count={})",
                node.getId(), newFailCount);
        }
    }

    /**
     * 围栏匹配校验（§47）：attempt_lease_version + worker_id + worker_epoch 全部匹配。
     */
    private boolean matchesFence(NodeAttemptEntity attempt, Long leaseVersion, String workerId, String workerEpoch) {
        if (leaseVersion == null || workerId == null || workerEpoch == null) {
            return false;
        }
        if (attempt.getAttemptLeaseVersion() == null
                || !attempt.getAttemptLeaseVersion().equals(leaseVersion)) {
            return false;
        }
        if (attempt.getWorkerId() == null || !attempt.getWorkerId().equals(workerId)) {
            return false;
        }
        return attempt.getWorkerEpoch() != null && attempt.getWorkerEpoch().equals(workerEpoch);
    }
}
