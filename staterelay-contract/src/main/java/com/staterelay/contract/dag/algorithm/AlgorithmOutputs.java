package com.staterelay.contract.dag.algorithm;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 算法多输出结果（对齐文档 §13.1）。
 *
 * <p>GIS 算法可能同时产生多个命名输出：
 * <ul>
 *   <li>正常结果图层（{@code resultLayer}）</li>
 *   <li>无效要素图层（{@code invalidFeatures}）</li>
 *   <li>未命中要素图层（{@code missedFeatures}）</li>
 *   <li>统计信息（{@code statistics}）</li>
 *   <li>警告列表（{@code warnings}）</li>
 *   <li>报告文件（{@code report}）</li>
 * </ul>
 *
 * <p>Worker 统一通过 {@code outputs} Map 返回，每个 value 可以是：
 * <ul>
 *   <li>{@link com.staterelay.contract.dag.spatial.VectorLayerRef}</li>
 *   <li>{@link com.staterelay.contract.dag.spatial.VectorDatasetRef}</li>
 *   <li>JSON 对象 / 数组（statistics、warnings）</li>
 * </ul>
 *
 * <p>本类是 {@link com.staterelay.contract.handler.TaskHandler} 的结果类型 R，
 * {@link AlgorithmExecutor} 实现返回 {@code TaskResult<AlgorithmOutputs>}。
 *
 * <p>JSON 示例（文档 §13.1，GDAL_INTERSECTION 输出）：
 * <pre>{@code
 * {
 *   "outputs": {
 *     "resultLayer": {
 *       "artifactId": 30001,
 *       "dataType": "VECTOR_LAYER",
 *       "storageType": "FILE_GDB",
 *       "layerName": "dag_50001_node_B_attempt_1_resultLayer",
 *       "geometryType": "MULTIPOLYGON",
 *       "srid": 4490
 *     },
 *     "invalidFeatures": {
 *       "artifactId": 30002,
 *       "dataType": "VECTOR_LAYER",
 *       "storageType": "FILE_GDB",
 *       "layerName": "dag_50001_node_B_attempt_1_invalidFeatures",
 *       "geometryType": "MULTIPOLYGON",
 *       "srid": 4490
 *     },
 *     "statistics": {
 *       "inputCount": 10000,
 *       "targetCount": 2000,
 *       "resultCount": 1200,
 *       "invalidCount": 3
 *     },
 *     "warnings": [
 *       { "code": "INVALID_GEOMETRY_REPAIRED", "count": 12 }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <p><b>输出端口名与物理图层名分离（文档 §3.3）：</b>
 * outputs Map 的 key 是算法契约声明的端口名（如 {@code resultLayer}），
 * value 中的 {@code layerName} 由 Worker Starter 按
 * {@code dagInstanceId + nodeCode + attemptNo + outputKey} 生成物理名称，
 * 避免 B/C 并行冲突、重试覆盖、晚到结果覆盖。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AlgorithmOutputs {

    /**
     * 命名输出映射，key 为算法契约输出端口名，value 为对应输出值。
     * 不同 key 对应不同输出（如 resultLayer / invalidFeatures / statistics / warnings）。
     */
    private Map<String, Object> outputs = new LinkedHashMap<>();

    public AlgorithmOutputs(Map<String, Object> outputs) {
        this.outputs = outputs == null ? new LinkedHashMap<>() : outputs;
    }

    /**
     * 构造单一输出的结果（用于只有一个主要输出的算法）。
     *
     * @param outputKey 算法契约输出端口名
     * @param value     输出值（VectorLayerRef / JSON 等）
     */
    public static AlgorithmOutputs single(String outputKey, Object value) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(Objects.requireNonNull(outputKey, "outputKey"), value);
        return new AlgorithmOutputs(map);
    }

    /**
     * 构造多输出结果。
     *
     * @param outputs 命名输出映射
     */
    public static AlgorithmOutputs of(Map<String, Object> outputs) {
        return new AlgorithmOutputs(outputs);
    }

    /**
     * 添加一个输出。
     */
    public AlgorithmOutputs with(String outputKey, Object value) {
        outputs.put(Objects.requireNonNull(outputKey, "outputKey"), value);
        return this;
    }

    /**
     * 按端口名获取输出值。
     */
    public Object get(String outputKey) {
        return outputs == null ? null : outputs.get(outputKey);
    }
}
