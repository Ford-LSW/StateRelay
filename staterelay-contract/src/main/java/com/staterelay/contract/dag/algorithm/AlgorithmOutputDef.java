package com.staterelay.contract.dag.algorithm;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * 算法输出定义（对齐文档 §3.1、§13.2）。
 *
 * <p>声明算法可能产生的一个输出端口：数据类型、基数、是否必填、是否主要输出。
 *
 * <p>JSON 示例：
 * <pre>{@code
 * "resultLayer": {
 *   "dataType": "VECTOR_LAYER_REF",
 *   "cardinality": "SINGLE",
 *   "required": true,
 *   "primary": true
 * },
 * "warnings": {
 *   "dataType": "JSON",
 *   "cardinality": "LIST",
 *   "required": false
 * }
 * }</pre>
 *
 * <p><b>primary 语义（文档 §13.3）：</b>
 * 算法契约通过 {@code primary=true} 标记主要输出，{@code NodeInstance.result_ref}
 * 字段由该输出确定，不能假设每个算法只返回一个结果。
 *
 * <p><b>输出端口名与物理图层名分离（文档 §3.3）：</b>
 * 算法固定的是输出端口名（如 {@code resultLayer}），物理图层名由 Worker Starter
 * 按 {@code dagInstanceId + nodeCode + attemptNo + outputKey} 生成，避免：
 * <ul>
 *   <li>不同 DAG 实例名称冲突</li>
 *   <li>B、C 并行输出冲突</li>
 *   <li>重试 Attempt 覆盖旧 Attempt</li>
 *   <li>晚到结果覆盖当前结果</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AlgorithmOutputDef {

    /** 数据类型，对应 {@link AlgorithmDataType} 常量或业务自定义类型字符串 */
    private String dataType;

    /** 输出基数，决定下游 {@code selector} 选择方式 */
    private OutputCardinality cardinality;

    /** 是否必填。false 时算法可省略该输出 */
    private boolean required;

    /**
     * 是否为算法主要输出。每个算法契约最多一个 {@code primary=true} 输出，
     * 用于填充 {@code NodeInstance.result_ref} 快捷字段（文档 §13.3）。
     */
    private boolean primary;

    public AlgorithmOutputDef(String dataType, OutputCardinality cardinality,
                              boolean required, boolean primary) {
        this.dataType = Objects.requireNonNull(dataType, "dataType");
        this.cardinality = Objects.requireNonNull(cardinality, "cardinality");
        this.required = required;
        this.primary = primary;
    }
}
