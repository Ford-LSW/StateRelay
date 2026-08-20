package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * Worker heartbeat 续约 Service（对齐文档 §23 / §27 / §43 T8A 事务）。
 *
 * <p>Worker 周期性 heartbeat 上报 activeAttempts（attemptId / attemptLeaseVersion），
 * Scheduler 据此续约 Attempt lease。
 *
 * <p>T8A 事务边界（§39.1）：
 * <ol>
 *   <li>校验 {@code worker_id + worker_epoch + attempt_id + attempt_lease_version + current_attempt_id}
 *       完整围栏匹配</li>
 *   <li>DISPATCHING(10) / UNKNOWN(80) → RUNNING(30)（heartbeat 证明 Worker 已开始执行）</li>
 *   <li>首次写 started_at 供审计</li>
 *   <li>续约 attempt_lease_expire_time；显式不修改 execution_deadline_at（§43）</li>
 *   <li>同事务调 {@link NodeInstanceMapper#resetScheduleFailCount}，
 *       把 schedule_fail_count 重置为 0（§9.1 重置时机之一：heartbeat 证明 Handler 已运行）</li>
 * </ol>
 *
 * <p>第一版采用轮询模型：Worker heartbeat 通过 HTTP 接口接收，
 * 由本 Service 处理 Attempt 维度的续约逻辑。
 */
@Service
public class NodeAttemptHeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptHeartbeatService.class);

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final TransactionTemplate transactions;
    private final int workerLeaseSeconds;

    public NodeAttemptHeartbeatService(NodeAttemptMapper attemptMapper,
                                        NodeInstanceMapper nodeInstanceMapper,
                                        NodeAttemptRepository attemptRepository,
                                        TransactionTemplate transactions,
                                        @Value("${staterelay.dag.scheduler.worker-lease-seconds:30}") int workerLeaseSeconds) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.transactions = transactions;
        this.workerLeaseSeconds = workerLeaseSeconds;
    }

    /**
     * 处理 Worker heartbeat 续约（§23 / §43 T8A）。
     *
     * <p>调用方：Worker HTTP heartbeat 接口在收到 activeAttempts 列表后，对每个 Attempt 调用本方法。
     *
     * @param attemptId     NodeAttempt ID
     * @param leaseVersion  Attempt lease 围栏版本号
     * @param workerId      Worker ID
     * @param workerEpoch   Worker 重启周期
     * @param now           当前时间
     * @return true 表示续约成功；false 表示围栏不匹配或状态已终态
     */
    public boolean renewLease(Long attemptId, Long leaseVersion, String workerId, String workerEpoch, Instant now) {
        if (attemptId == null || leaseVersion == null || workerId == null || workerEpoch == null) {
            log.warn("Heartbeat renewLease missing fence fields (attemptId={}, leaseVersion={}, workerId={}, workerEpoch={})",
                attemptId, leaseVersion, workerId, workerEpoch);
            return false;
        }

        NodeAttemptEntity attempt = attemptRepository.findById(attemptId).orElse(null);
        if (attempt == null) {
            log.debug("NodeAttempt {} not found for heartbeat renewLease", attemptId);
            return false;
        }

        // 围栏校验（§47）：lease_version + worker_id + worker_epoch 必须全部匹配
        // 防止旧 epoch Worker 误续约或跨 Attempt 串扰
        if (!attempt.getAttemptLeaseVersion().equals(leaseVersion)
                || !attempt.getWorkerId().equals(workerId)
                || !attempt.getWorkerEpoch().equals(workerEpoch)) {
            log.warn("NodeAttempt {} heartbeat fence mismatch "
                    + "(stored leaseVersion={}, incoming={}, workerId={}, workerEpoch={})",
                attemptId, attempt.getAttemptLeaseVersion(), leaseVersion, workerId, workerEpoch);
            return false;
        }

        Instant newLeaseExpireTime = now.plus(Duration.ofSeconds(workerLeaseSeconds));
        transactions.executeWithoutResult(status -> {
            // 1. T8A: CAS 续约 Attempt lease + 状态推进（DISPATCHING/UNKNOWN → RUNNING）
            int attemptUpdated = attemptMapper.renewLease(
                attemptId, leaseVersion, workerId, workerEpoch, newLeaseExpireTime, now);
            if (attemptUpdated == 0) {
                log.debug("NodeAttempt {} CAS renewLease failed (state changed or fence mismatch)", attemptId);
                return;
            }

            // 2. §9.1: 同事务重置 NodeInstance.schedule_fail_count = 0
            //    heartbeat 证明 Handler 已运行，之前因调度失败累计的 schedule_fail_count 应重置
            nodeInstanceMapper.resetScheduleFailCount(attempt.getNodeInstanceId(), now);

            log.debug("NodeAttempt {} lease renewed until {} (leaseVersion={})",
                attemptId, newLeaseExpireTime, leaseVersion);
        });
        return true;
    }
}
