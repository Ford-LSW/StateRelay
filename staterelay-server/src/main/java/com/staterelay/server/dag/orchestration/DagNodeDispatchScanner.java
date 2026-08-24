package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.dispatch.DagDispatchGateway;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.mapper.dto.NodeInstanceLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * DAG 节点调度 Scanner（对齐文档 §26 / §27）。
 *
 * <p>周期扫描 READY 状态的 NodeInstance，调用 {@link DagNodeDispatchService#dispatch} 完成完整调度流程：
 * <ol>
 *   <li>CAS READY → DISPATCHING（抢占）</li>
 *   <li>查 AlgorithmDefinition（executorGroupCode / timeoutSeconds / maxRetry）</li>
 *   <li>ParameterResolver 解析 inputBindings（失败则 DISPATCHING → FAILED，§17）</li>
 *   <li>在 {@code sr_worker} 中按应用、环境、能力、租约与容量选择 Worker</li>
 *   <li>无 Worker：schedule_fail_count + 1；未达上限回退 READY，达上限 DISPATCHING → FAILED（§9.1）</li>
 *   <li>有 Worker：原子预留容量、创建 NodeAttempt，并更新 current-attempt fence</li>
 *   <li>事务提交后调用 Worker；不确定网络结果由独立扫描复用原请求重发</li>
 * </ol>
 *
 * <p>租约过期由 {@link #scanStuckDispatching} 周期回退 DISPATCHING 节点到 READY。
 */
@Component
public class DagNodeDispatchScanner {

    private static final Logger log = LoggerFactory.getLogger(DagNodeDispatchScanner.class);

    private final NodeInstanceMapper nodeInstanceMapper;
    private final DagNodeDispatchService dispatchService;
    private final DagDispatchGateway dispatchGateway;
    private final int batchSize;
    private final long leaseSeconds;
    private final int stuckScanBatchSize;
    private final int transportRetryBatchSize;

    public DagNodeDispatchScanner(NodeInstanceMapper nodeInstanceMapper,
                                  DagNodeDispatchService dispatchService,
                                  DagDispatchGateway dispatchGateway,
                                  @Value("${staterelay.dag.dispatch.batch-size:50}") int batchSize,
                                  @Value("${staterelay.dag.dispatch.lease-seconds:30}") long leaseSeconds,
                                  @Value("${staterelay.dag.dispatch.stuck-scan-batch-size:50}") int stuckScanBatchSize,
                                  @Value("${staterelay.dag.dispatch.transport-retry-batch-size:50}") int transportRetryBatchSize) {
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.dispatchService = dispatchService;
        this.dispatchGateway = dispatchGateway;
        this.batchSize = batchSize;
        this.leaseSeconds = leaseSeconds;
        this.stuckScanBatchSize = stuckScanBatchSize;
        this.transportRetryBatchSize = transportRetryBatchSize;
    }

    /** Retransmits ACK-uncertain HTTP sends without creating another Attempt. */
    @Scheduled(fixedDelayString = "${staterelay.dag.dispatch.transport-retry-scan-interval-ms:1000}")
    public void retryUncertainDispatches() {
        try {
            dispatchGateway.retryUncertain(Instant.now(), transportRetryBatchSize);
        } catch (Exception ex) {
            log.error("DAG uncertain dispatch retry scan failed: {}", ex.getMessage(), ex);
        }
    }

    /**
     * 周期扫描 READY 节点并调度。
     */
    @Scheduled(fixedDelayString = "${staterelay.dag.dispatch.scan-interval-ms:1000}")
    public void scan() {
        Instant now = Instant.now();
        try {
            List<NodeInstanceLease> leases = nodeInstanceMapper.scanReadyNodes(now, batchSize);
            if (leases.isEmpty()) {
                return;
            }
            log.debug("Found {} READY NodeInstances to dispatch", leases.size());
            for (NodeInstanceLease lease : leases) {
                try {
                    dispatchService.dispatch(lease, now);
                } catch (Exception ex) {
                    log.error("Failed to dispatch NodeInstance {}: {}",
                        lease.getId(), ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("DAG node dispatch scan failed: {}", ex.getMessage(), ex);
        }
    }

    /**
     * 周期扫描 DISPATCHING 卡住的节点（租约过期），回退到 READY（§26 租约机制）。
     */
    @Scheduled(fixedDelayString = "${staterelay.dag.dispatch.stuck-scan-interval-ms:5000}")
    public void scanStuckDispatching() {
        Instant now = Instant.now();
        try {
            List<Long> stuckIds = nodeInstanceMapper.scanStuckDispatching(now, stuckScanBatchSize);
            if (stuckIds.isEmpty()) {
                return;
            }
            log.debug("Found {} stuck DISPATCHING NodeInstances", stuckIds.size());
            for (Long nodeInstanceId : stuckIds) {
                try {
                    Instant nextScheduleTime = now.plus(Duration.ofSeconds(leaseSeconds));
                    int updated = nodeInstanceMapper.revertToReadyForNoWorker(
                        nodeInstanceId,
                        nextScheduleTime,
                        "DISPATCH_LEASE_EXPIRED",
                        "Dispatch lease expired before worker assigned",
                        now);
                    if (updated > 0) {
                        log.info("NodeInstance {} reverted to READY (dispatch lease expired)", nodeInstanceId);
                    }
                } catch (Exception ex) {
                    log.error("Failed to revert stuck NodeInstance {}: {}",
                        nodeInstanceId, ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            log.error("Stuck DISPATCHING scan failed: {}", ex.getMessage(), ex);
        }
    }
}
