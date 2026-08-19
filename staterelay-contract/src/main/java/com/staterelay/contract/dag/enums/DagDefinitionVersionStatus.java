package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 定义版本状态。
 *
 * <p>与 {@code sr_dag_definition_version.status} 列对应（存数值）。
 * 发布时强制做拓扑校验（见文档 §3.2），校验通过才能 ENABLED。
 *
 * <pre>
 * 0   DRAFT    草稿
 * 10  ENABLED  已发布（通过拓扑校验）
 * 20  DISABLED 已禁用
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum DagDefinitionVersionStatus implements CodedEnum {
    DRAFT(0),
    ENABLED(10),
    DISABLED(20);

    private final int code;
}
