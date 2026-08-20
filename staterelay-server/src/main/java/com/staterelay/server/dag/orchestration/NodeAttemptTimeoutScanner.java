package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * NodeAttempt TIMEOUT Scanner（对齐文档 §29.4 TIMEOUT 完整处理链路）。
 *
 * <p>周期扫描 RUNNING 状态超时的 NodeAttempt（{@code now - started_at > timeout_seconds}），
 * CAS NodeAttempt RUNNING → TIMEOUT。后续 NodeInstance 推进由 {@link NodeAttemptSyncService}
 * 统一处理（重试或终态）。
 *
 * <p>第一版由 Scheduler 侧职责（持有 AlgorithmDefinition.timeout_seconds）：
 * <ul>
 *   <li>SQL 中已通过 JOIN sr_algorithm_definition 取出 timeout_seconds</li>
 *   <li>默认 300 秒</li>
 * </ul>
 *
 * <p>幂等性：CAS 失败说明已被其他 Scanner 实例处理，跳过即可。
 */
@Component
public class NodeAttemptTimeoutScanner {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptTimeoutScanner.class);

    private final NodeAttemptMapper attemptMapper;
    private final int batchSize;

    public NodeAttemptTimeoutScanner(NodeAttemptMapper attemptMapper,
                                     @Value("${staterelay.dag.timeout-scan.batch-size:50}") int batchSize) {
        this.attemptMapper = attemptMapper;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${staterelay.dag.timeout-scan.interval-ms:5000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<Long> attemptIds = attemptMapper.scanTimeoutAttempts(now, batchSize);
            if (attemptIds.isEmpty()) {
                return;
            }
            log.debug("Found {} timed-out NodeAttempts", attemptIds.size());
            for (Long attemptId : attemptIds) {
                try {
                    int updated = attemptMapper.markTimeout(attemptId, now);
                    if (updated > 0) {
                        log.info("NodeAttempt {} marked TIMEOUT (timeout_seconds exceeded)", attemptId);
                    }
                } catch (Exception ex) {
                    log.error("Failed to mark NodeAttempt {} as TIMEOUT: {}",
                        attemptId, ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("NodeAttempt TIMEOUT scan failed: {}", ex.getMessage(), ex);
        }
    }
}
