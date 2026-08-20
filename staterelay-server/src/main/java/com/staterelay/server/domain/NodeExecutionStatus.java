package com.staterelay.server.domain;

/**
 * DAG 节点执行业务状态。
 *
 * <p>与 {@code sr_dag_node_execution.status} 对齐，描述节点生命周期。
 *
 * <pre>
 * PENDING ──→ WAITING ──→ READY ──→ RUNNING ──→ SUCCESS
 *                 │           │          │
 *                 │           │          ├──→ FAILED ──→ (retry) READY
 *                 │           │          └──→ CANCELLING ──→ CANCELLED
 *                 │           └──→ CANCELLING ──→ CANCELLED
 *                 └──→ SKIPPED (阶段三)
 * </pre>
 */
public enum NodeExecutionStatus {
    PENDING,
    WAITING,
    READY,
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED,
    CANCELLING,
    CANCELLED
}
