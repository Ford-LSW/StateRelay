package com.staterelay.contract.dag.binding;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * DAG 节点输入绑定协议（对齐文档 §6）。
 *
 * <p>与旧版 {@code ${dag.inputs.*}} / {@code ${nodes.*.outputs.*}} 表达式<b>并存</b>，
 * 提供结构化、强校验的绑定协议。
 *
 * <p>本类描述"本节点每个算法输入具体从哪里取得"，由三部分组成：
 * <pre>
 *   sourceType  —— 来源类型（决定其余字段语义）
 *   定位字段    —— DAG_INPUT 用 key，NODE_OUTPUT 用 nodeCode + outputKey + 可选 selector
 *   值字段      —— CONST 用 value
 * </pre>
 *
 * <h3>形态 1：DAG_INPUT（§6.2）</h3>
 * <pre>{@code
 * { "sourceType": "DAG_INPUT", "key": "sourceGdbFileId" }
 * }</pre>
 * 从 {@code DagInstance.input_json} 读取，适合用户上传文件 ID、动态阈值等随 DAG 实例变化的业务参数。
 *
 * <h3>形态 2：NODE_OUTPUT（§6.3）</h3>
 * <pre>{@code
 * { "sourceType": "NODE_OUTPUT", "nodeCode": "B", "outputKey": "resultLayer" }
 * }</pre>
 * 带 selector：
 * <pre>{@code
 * {
 *   "sourceType": "NODE_OUTPUT",
 *   "nodeCode": "A",
 *   "outputKey": "layers",
 *   "selector": { "type": "MAP_KEY", "key": "layerA" }
 * }
 * }</pre>
 * 完整定位关系：{@code dagInstanceId}（由运行上下文提供） + {@code nodeCode} + {@code outputKey} + 可选 {@code selector}。
 * 仅可读取同一 DagInstance 中已成功上游 NodeInstance 对外公开的 outputs（§6.5）。
 *
 * <h3>形态 3：CONST（§6.4）</h3>
 * <pre>{@code
 * {
 *   "sourceType": "CONST",
 *   "value": {
 *     "dataType": "VECTOR_LAYER",
 *     "storageType": "POSTGIS",
 *     "dataSourceId": 101,
 *     "schema": "public",
 *     "table": "pglayer_a",
 *     "geometryField": "geom"
 *   }
 * }
 * }</pre>
 * 适合固定参考图层、固定数据源 ID、阈值容差、输出格式、字段映射、算法选项。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InputBinding {

    /** 来源类型，决定本对象的其余字段语义 */
    private BindingSourceType sourceType;

    // ===== DAG_INPUT 形态字段 =====

    /** {@link BindingSourceType#DAG_INPUT} 形态必填。DagInstance.input_json 中的 Key */
    private String key;

    // ===== NODE_OUTPUT 形态字段 =====

    /** {@link BindingSourceType#NODE_OUTPUT} 形态必填。上游节点编码 */
    private String nodeCode;

    /** {@link BindingSourceType#NODE_OUTPUT} 形态必填。上游算法输出端口名 */
    private String outputKey;

    /**
     * {@link BindingSourceType#NODE_OUTPUT} 形态可选。
     * 当 {@code outputKey} 指向 Map 或 List 时进一步定位其中一个值。
     */
    private BindingSelector selector;

    // ===== CONST 形态字段 =====

    /**
     * {@link BindingSourceType#CONST} 形态必填。固定值，可以是任意 JSON 结构，
     * 包括 {@link com.staterelay.contract.dag.spatial.VectorLayerRef}、字符串、数字等。
     */
    private Object value;

    public InputBinding(BindingSourceType sourceType) {
        this.sourceType = Objects.requireNonNull(sourceType, "sourceType");
    }

    /**
     * 构造 DAG_INPUT 绑定。
     *
     * @param key DagInstance.input_json 中的 Key
     */
    public static InputBinding dagInput(String key) {
        InputBinding binding = new InputBinding(BindingSourceType.DAG_INPUT);
        binding.key = Objects.requireNonNull(key, "key");
        return binding;
    }

    /**
     * 构造 NODE_OUTPUT 绑定（无 selector，对应 SINGLE 基数上游输出）。
     *
     * @param nodeCode  上游节点编码
     * @param outputKey 上游算法输出端口名
     */
    public static InputBinding nodeOutput(String nodeCode, String outputKey) {
        return nodeOutput(nodeCode, outputKey, null);
    }

    /**
     * 构造 NODE_OUTPUT 绑定（带 selector，对应 MAP / LIST 复合上游输出）。
     *
     * @param nodeCode  上游节点编码
     * @param outputKey 上游算法输出端口名
     * @param selector  复合输出选择器，可为 null
     */
    public static InputBinding nodeOutput(String nodeCode, String outputKey, BindingSelector selector) {
        InputBinding binding = new InputBinding(BindingSourceType.NODE_OUTPUT);
        binding.nodeCode = Objects.requireNonNull(nodeCode, "nodeCode");
        binding.outputKey = Objects.requireNonNull(outputKey, "outputKey");
        binding.selector = selector;
        return binding;
    }

    /**
     * 构造 CONST 绑定。
     *
     * @param value 固定值，可为任意 JSON 结构
     */
    public static InputBinding constValue(Object value) {
        InputBinding binding = new InputBinding(BindingSourceType.CONST);
        binding.value = Objects.requireNonNull(value, "value");
        return binding;
    }

    /**
     * 校验当前 sourceType 形态下必填字段是否齐全（文档 §7、§8.3）。
     *
     * @return true 表示必填字段齐全
     */
    public boolean hasRequiredFields() {
        switch (sourceType) {
            case DAG_INPUT:
                return key != null && !key.isBlank();
            case NODE_OUTPUT:
                return nodeCode != null && !nodeCode.isBlank()
                        && outputKey != null && !outputKey.isBlank();
            case CONST:
                return value != null;
            default:
                return false;
        }
    }
}
