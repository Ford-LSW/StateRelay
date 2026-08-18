package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 实例业务状态。
 *
 * <p>描述 DAG 实例的生命周期状态，与 {@code sr_dag_instance.status} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagInstanceStatus implements CodedEnum {
    PENDING(0),
    RUNNING(1),
    SUCCESS(2),
    FAILED(3),
    CANCELLING(4),
    CANCELLED(5);

    private final int code;
}
