package com.staterelay.contract.dag.algorithm;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * 算法输入定义（对齐文档 §3.1）。
 *
 * <p>声明算法需要的一个输入参数：数据类型、是否必填、默认值。
 * 用户在 DAG 节点 {@code inputBindings} 中只能配置契约已声明的输入名（文档 §3.1 末）。
 *
 * <p>JSON 示例：
 * <pre>{@code
 * "sourceLayer": {
 *   "dataType": "VECTOR_LAYER_REF",
 *   "required": true
 * },
 * "options": {
 *   "dataType": "INTERSECTION_OPTIONS",
 *   "required": false,
 *   "defaultValue": {}
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AlgorithmInputDef {

    /**
     * 数据类型，对应 {@link AlgorithmDataType} 常量或业务自定义类型字符串。
     * ParameterResolver 校验绑定值结构是否符合该类型。
     */
    private String dataType;

    /** 是否必填。true 时 inputBindings 必须配置该输入（文档 §7 第 2 项校验） */
    private boolean required;

    /**
     * 默认值。{@code required=false} 且未配置 inputBindings 时使用，
     * 可为复合 JSON 结构。
     */
    private Object defaultValue;

    public AlgorithmInputDef(String dataType, boolean required) {
        this(dataType, required, null);
    }

    public AlgorithmInputDef(String dataType, boolean required, Object defaultValue) {
        this.dataType = Objects.requireNonNull(dataType, "dataType");
        this.required = required;
        this.defaultValue = defaultValue;
    }
}
