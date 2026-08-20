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
 * UNKNOWN 收敛 Scanner（对齐文档 §19.1 / §29.5）。
 *
 * <p>周期扫描 UNKNOWN 状态的 NodeAttempt，达到收敛窗口阈值（{@code 2 × timeout_seconds}）后
 * CAS UNKNOWN → TIMEOUT，避免永久卡死。
 *
 * <p>SQL 已通过 JOIN sr_algorithm_definition 取出 timeout_seconds，默认 300 秒；
 * 收敛窗口默认 600 秒（2 × 300）。
 *
 * <p>收敛后由 {@link NodeAttemptSyncService#applyTimeout} 按 TIMEOUT 处理：
 * 重试或终态推进 NodeInstance。
 */
@Component
public class NodeAttemptUnknownScanner {

    private static final Logger log = LoggerFactory.getLogger(NodeAttemptUnknownScanner.class);

    private final NodeAttemptMapper attemptMapper;
    private final int batchSize;

    public NodeAttemptUnknownScanner(NodeAttemptMapper attemptMapper,
                                     @Value("${staterelay.dag.unknown-scan.batch-size:50}") int batchSize) {
        this.attemptMapper = attemptMapper;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${staterelay.dag.unknown-scan.interval-ms:10000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<Long> attemptIds = attemptMapper.scanUnknownAttempts(now, batchSize);
            if (attemptIds.isEmpty()) {
                return;
            }
            log.debug("Found {} UNKNOWN NodeAttempts ready to converge to TIMEOUT", attemptIds.size());
            for (Long attemptId : attemptIds) {
                try {
                    int updated = attemptMapper.markUnknownAsTimeout(attemptId, now);
                    if (updated > 0) {
                        log.info("NodeAttempt {} UNKNOWN → TIMEOUT (converged after 2x timeout window)", attemptId);
                    }
                } catch (Exception ex) {
                    log.error("Failed to converge NodeAttempt {} UNKNOWN → TIMEOUT: {}",
                        attemptId, ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("UNKNOWN convergence scan failed: {}", ex.getMessage(), ex);
        }
    }
}
