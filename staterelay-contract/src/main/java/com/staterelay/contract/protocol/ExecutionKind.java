package com.staterelay.contract.protocol;

/** Identifies the persistence domain owning an active Worker execution lease. */
public enum ExecutionKind {
    GENERIC_TASK,
    DAG_NODE
}
