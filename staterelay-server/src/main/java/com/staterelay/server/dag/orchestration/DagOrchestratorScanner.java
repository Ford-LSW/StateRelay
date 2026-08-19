package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * DAG Engine Scanner（对齐文档 §19.2 轮询模型）。
 *
 * <p>周期扫描 RUNNING 实例，提交到线程池推进后继节点 WAITING → READY。
 *
 * <p>使用 {@code FOR UPDATE SKIP LOCKED} 避免多实例重复领取。
 */
@Component
public class DagOrchestratorScanner {

    private static final Logger log = LoggerFactory.getLogger(DagOrchestratorScanner.class);

    private final DagInstanceMapper instanceMapper;
    private final DagOrchestratorExecutor executor;
    private final int batchSize;

    public DagOrchestratorScanner(DagInstanceMapper instanceMapper,
                                   DagOrchestratorExecutor executor,
                                   @Value("${staterelay.dag.orchestrator.scan-batch-size:20}") int batchSize) {
        this.instanceMapper = instanceMapper;
        this.executor = executor;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${staterelay.dag.orchestrator.scan-interval-ms:1000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<DagInstanceLease> due = instanceMapper.scanRunningInstances(now, batchSize);
            if (due.isEmpty()) {
                return;
            }
            log.debug("Found {} RUNNING DAG instances to advance", due.size());
            for (DagInstanceLease lease : due) {
                try {
                    executor.submit(lease);
                } catch (Exception ex) {
                    log.error("Failed to submit DAG instance {}: {}", lease.getId(), ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("DAG orchestrator scan failed: {}", ex.getMessage(), ex);
        }
    }
}
