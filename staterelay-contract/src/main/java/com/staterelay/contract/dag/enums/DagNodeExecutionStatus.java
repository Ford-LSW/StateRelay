package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 节点执行业务状态。
 *
 * <p>与 {@code sr_dag_node_execution.status} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagNodeExecutionStatus implements CodedEnum {
    PENDING(0),
    WAITING(1),
    READY(2),
    RUNNING(3),
    SUCCESS(4),
    FAILED(5),
    SKIPPED(6),
    CANCELLING(7),
    CANCELLED(8);

    private final int code;
}
