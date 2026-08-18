package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 节点执行编排子状态。
 *
 * <p>与 {@code sr_dag_node_execution.orchestration_state} 列对应（存数值）。
 * 用于节点级 CAS 防止重复推进/重复创建 TaskInstance。
 */
@Getter
@AllArgsConstructor
public enum DagNodeExecutionOrchestrationState implements CodedEnum {
    PENDING(0),
    READY(1),
    QUEUED(2),
    EXECUTING(3),
    WAITING_RESULT(4),
    DONE(5);

    private final int code;
}
