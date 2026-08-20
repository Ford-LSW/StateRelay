package com.staterelay.server.domain;

/**
 * DAG 实例编排子状态。
 *
 * <p>与 {@code sr_dag_instance.orchestration_state} 对齐，防止多实例 Orchestrator 重复推进。
 *
 * <pre>
 * READY ──→ QUEUED ──→ EXECUTING ──→ WAITING_NODES ──→ DONE
 *   ↑                                       │
 *   └───────────────────────────────────────┘
 *                (节点完成触发重编排)
 * </pre>
 */
public enum DagInstanceOrchestrationState {
    READY,
    QUEUED,
    EXECUTING,
    WAITING_NODES,
    DONE
}
