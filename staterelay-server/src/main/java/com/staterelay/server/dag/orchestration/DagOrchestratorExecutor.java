package com.staterelay.server.dag.orchestration;

import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAG Engine 线程池执行器（对齐文档 §19.2 轮询模型）。
 *
 * <p>对齐文档：去掉 READY → QUEUED → EXECUTING → WAITING_NODES 的 orchestration_state 子状态机，
 * scanner 领取后直接调用 {@link DagOrchestratorService#advanceInstance} 推进节点。
 */
@Component
public class DagOrchestratorExecutor {

    private static final Logger log = LoggerFactory.getLogger(DagOrchestratorExecutor.class);

    private final DagOrchestratorService orchestratorService;
    private final ThreadPoolExecutor executor;

    public DagOrchestratorExecutor(DagOrchestratorService orchestratorService,
                                    @Value("${staterelay.dag.orchestrator.pool-size:8}") int poolSize) {
        this.orchestratorService = orchestratorService;
        this.executor = new ThreadPoolExecutor(
            poolSize, poolSize, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new DagInstanceThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void submit(DagInstanceLease lease) {
        executor.execute(() -> {
            try {
                orchestratorService.advanceInstance(lease.getId());
            } catch (Exception ex) {
                log.error("Failed to advance DAG instance {}: {}", lease.getId(), ex.getMessage(), ex);
            }
        });
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
