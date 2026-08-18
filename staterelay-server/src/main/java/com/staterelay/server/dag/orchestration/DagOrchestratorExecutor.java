package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAG Orchestrator 线程池执行器，负责领取的 DAG 实例推进。
 *
 * <p>每个 DAG 实例对应一个 Runnable：先 CAS 抢占推进权（READY→QUEUED），
 * 成功后再 CAS 进入 EXECUTING，然后调用 {@link DagOrchestratorService} 推进节点。
 */
@Component
public class DagOrchestratorExecutor {

    private static final Logger log = LoggerFactory.getLogger(DagOrchestratorExecutor.class);

    private final DagOrchestratorService orchestratorService;
    private final ThreadPoolExecutor executor;
    private final Duration deadlineBuffer;

    public DagOrchestratorExecutor(DagOrchestratorService orchestratorService,
                                    @Value("${staterelay.dag.orchestrator.pool-size:8}") int poolSize,
                                    @Value("${staterelay.dag.orchestrator.deadline-buffer-seconds:30}") int deadlineBufferSeconds) {
        this.orchestratorService = orchestratorService;
        this.deadlineBuffer = Duration.ofSeconds(deadlineBufferSeconds);
        this.executor = new ThreadPoolExecutor(
            poolSize, poolSize, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new DagInstanceThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void submit(DagInstanceLease lease) {
        executor.execute(() -> runInstance(lease));
    }

    private void runInstance(DagInstanceLease lease) {
        Instant now = Instant.now();
        Instant deadline = now.plus(deadlineBuffer);
        try {
            if (!orchestratorService.tryEnqueueForAdvance(lease, now, deadline)) {
                log.debug("DAG instance {} already advanced by another orchestrator", lease.getId());
                return;
            }
            Long expectedVersion = orchestratorService.getCurrentOrchestrationVersion(lease.getId());
            if (expectedVersion == null) {
                return;
            }
            if (!orchestratorService.tryEnterExecuting(lease, expectedVersion, now)) {
                log.debug("DAG instance {} lease expired before entering EXECUTING", lease.getId());
                return;
            }
            orchestratorService.advanceInstance(lease.getId());
        } catch (Exception ex) {
            log.error("Failed to advance DAG instance {}: {}", lease.getId(), ex.getMessage(), ex);
        } finally {
            try {
                orchestratorService.tryEnterWaitingNodes(lease.getId());
            } catch (Exception ex) {
                log.warn("Failed to enter WAITING_NODES for DAG instance {}: {}", lease.getId(), ex.getMessage());
            }
        }
    }

    private static final class DagInstanceThreadFactory implements java.util.concurrent.ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "dag-orchestrator-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
