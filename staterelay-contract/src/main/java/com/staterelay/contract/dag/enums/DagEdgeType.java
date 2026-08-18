package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 边类型。
 *
 * <p>与 {@code sr_dag_edge.edge_type} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagEdgeType implements CodedEnum {
    /** 显式控制依赖 */
    CONTROL(0),
    /** 由 input binding ${nodes.X.outputs.Y} 推导的隐式数据依赖 */
    DATA(1);

    private final int code;
}
