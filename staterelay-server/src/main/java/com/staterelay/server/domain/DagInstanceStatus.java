package com.staterelay.server.domain;

/**
 * DAG 实例业务状态。
 *
 * <p>描述 DAG 实例的生命周期，与 {@code sr_dag_instance.status} 对齐。
 */
public enum DagInstanceStatus {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLING,
    CANCELLED
}
