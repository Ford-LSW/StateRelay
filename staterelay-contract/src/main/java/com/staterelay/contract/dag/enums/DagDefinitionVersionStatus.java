package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 定义版本状态。
 *
 * <p>与 {@code sr_dag_definition_version.status} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagDefinitionVersionStatus implements CodedEnum {
    DRAFT(0),
    PUBLISHED(1);

    private final int code;
}
