package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * NodeAttempt 终态同步 Service（对齐文档 §19.1 事务边界 + §19.2 轮询模型）。
 *
 * <p>第一版走轮询：周期扫描 {@code NodeAttempt 终态 + NodeInstance 仍 RUNNING} 的记录，
 * 在同一个事务内完成：
 * <ol>
 *   <li>读取 NodeAttempt 终态结果</li>
 *   <li>CAS NodeInstance: RUNNING → SUCCESS/FAILED</li>
 *   <li>若 SUCCESS：持久化输出 Artifact</li>
 * </ol>
 *
 * <p>重试与超时回退由 Scheduler 负责（{@code max_retry} / {@code timeout_seconds} 持有方）；
 * 后继节点 WAITING → READY 推进由 DAG Engine 在下一轮扫描完成。
 */
@Service
public class NodeAttemptSyncService {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptSyncService.class);

    private final NodeAttemptMapper attemptMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final NodeAttemptRepository attemptRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final int batchSize;

    public NodeAttemptSyncService(NodeAttemptMapper attemptMapper,
                                   NodeInstanceMapper nodeInstanceMapper,
                                   NodeAttemptRepository attemptRepository,
                                   NodeInstanceJpaRepository nodeInstanceRepository,
                                   @Value("${staterelay.dag.attempt-sync.batch-size:50}") int batchSize) {
        this.attemptMapper = attemptMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.attemptRepository = attemptRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
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
     */
    @Transactional
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

        switch (attemptStatus) {
            case SUCCESS -> applySuccess(attempt, node, now);
            case FAILED, TIMEOUT, UNKNOWN -> applyFailure(attempt, node, now);
            default -> { /* 不会到这里 */ }
        }
    }

    private void applySuccess(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        var snapshot = nodeInstanceMapper.markSuccess(
            node.getId(),
            attempt.getResultJson(),
            attempt.getResultRef(),
            now);
        if (snapshot == null) {
            log.debug("NodeInstance {} already finalized or not RUNNING", node.getId());
            return;
        }
        // Attempt 已存 result_json / result_ref；
        // Artifact 的具体写入由业务侧根据 result_ref 反序列化后调用 artifactService.saveArtifacts(...) 补充
        log.debug("NodeInstance {} SUCCESS via attempt {}", node.getId(), attempt.getId());
    }

    private void applyFailure(NodeAttemptEntity attempt, NodeInstanceEntity node, Instant now) {
        // FAILED / TIMEOUT / UNKNOWN：先 CAS NodeInstance → FAILED
        // 重试回退到 READY 由 Scheduler 根据 max_retry 决定，这里只做最终状态同步
        int updated = nodeInstanceMapper.markFailed(
            node.getId(),
            attempt.getErrorCode(),
            attempt.getErrorMessage(),
            now);
        if (updated > 0) {
            log.debug("NodeInstance {} FAILED via attempt {} (status={})",
                node.getId(), attempt.getId(), attempt.getStatus());
        }
    }
}
