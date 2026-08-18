package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 节点扇出策略。
 *
 * <p>与 {@code sr_dag_node_execution.fanout_strategy} 列对应（存数值）。
 * 阶段一固定 SINGLE（1:1 TaskInstance），阶段三支持 SHARD/REPLICATE 等策略。
 */
@Getter
@AllArgsConstructor
public enum DagNodeFanoutStrategy implements CodedEnum {
    /** 1:1 TaskInstance（阶段一） */
    SINGLE(0),
    /** 节点内分片并行（阶段三预留） */
    SHARD(1),
    /** 节点内复制并行（阶段三预留） */
    REPLICATE(2);

    private final int code;
}
