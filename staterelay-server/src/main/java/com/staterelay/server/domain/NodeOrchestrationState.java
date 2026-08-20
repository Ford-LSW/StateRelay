package com.staterelay.server.domain;

/**
 * DAG 节点编排子状态。
 *
 * <p>与 {@code sr_dag_node_execution.orchestration_state} 对齐，防止重复投递。
 *
 * <pre>
 * PENDING ──→ READY ──→ QUEUED ──→ EXECUTING ──→ WAITING_RESULT ──→ DONE
 * </pre>
 */
public enum NodeOrchestrationState {
    PENDING,
    READY,
    QUEUED,
    EXECUTING,
    WAITING_RESULT,
    DONE
}
