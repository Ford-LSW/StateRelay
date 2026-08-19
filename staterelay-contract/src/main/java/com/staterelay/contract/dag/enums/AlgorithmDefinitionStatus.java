package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 算法定义状态。
 *
 * <p>与 {@code sr_algorithm_definition.status} 列对应（存数值）。
 *
 * <pre>
 * 0   DISABLED  禁用
 * 10  ENABLED   启用
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum AlgorithmDefinitionStatus implements CodedEnum {
    DISABLED(0),
    ENABLED(10);

    private final int code;
}
