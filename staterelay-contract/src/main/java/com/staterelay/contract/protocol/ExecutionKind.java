package com.staterelay.contract.protocol;

/** 标识活跃 Worker 执行租约所属的持久化领域。 */
public enum ExecutionKind {
    GENERIC_TASK,
    DAG_NODE
}
