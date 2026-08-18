package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 实例编排子状态。
 *
 * <p>用于防止重复推进，与 {@code sr_dag_instance.orchestration_state} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagInstanceOrchestrationState implements CodedEnum {
    READY(0),
    QUEUED(1),
    EXECUTING(2),
    WAITING_NODES(3),
    DONE(4);

    private final int code;
}
