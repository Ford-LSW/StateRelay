package com.staterelay.contract.dag.algorithm;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 算法结构化契约（对齐文档 §3.1、§3.2）。
 *
 * <p>调度中心保存的稳定算法契约，定义算法需要什么输入、能够产生什么输出。
 * Worker 心跳上报 {@code algorithmCode + contractVersion + contractChecksum +
 * implementationVersion}，调度中心校验后决定该 Worker 是否可接收该算法任务（文档 §3.2）。
 *
 * <p>调度中心的 AlgorithmDefinition 是稳定契约；Worker 不应在每次注册或心跳时任意覆盖
 * 已发布契约。如果 Worker 上报的契约版本或 checksum 与中心不一致：
 * <pre>
 *   该 Worker 不得接收该算法的新任务
 *   + 记录 ALGORITHM_CONTRACT_MISMATCH 告警
 * </pre>
 *
 * <p>JSON 示例（文档 §3.1，GDAL_INTERSECTION）：
 * <pre>{@code
 * {
 *   "algorithmCode": "GDAL_INTERSECTION",
 *   "algorithmName": "GDAL空间相交",
 *   "contractVersion": "1.0",
 *   "inputs": {
 *     "sourceLayer": { "dataType": "VECTOR_LAYER_REF", "required": true },
 *     "targetLayer": { "dataType": "VECTOR_LAYER_REF", "required": true },
 *     "options": { "dataType": "INTERSECTION_OPTIONS", "required": false, "defaultValue": {} }
 *   },
 *   "outputs": {
 *     "resultLayer": { "dataType": "VECTOR_LAYER_REF", "cardinality": "SINGLE", "required": true, "primary": true },
 *     "invalidFeatures": { "dataType": "VECTOR_LAYER_REF", "cardinality": "SINGLE", "required": false },
 *     "statistics": { "dataType": "JSON", "cardinality": "SINGLE", "required": false },
 *     "warnings": { "dataType": "JSON", "cardinality": "LIST", "required": false }
 *   }
 * }
 * }</pre>
 *
 * <p><b>不可修改约束（对齐 §10）：b>一旦 DagInstance 开始执行，本表行记录
 * （含 timeout_seconds / max_retry / contract_version / contract_checksum /
 * input_schema_json / output_schema_json）在本次执行期间禁止修改。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AlgorithmContract {

    /** 算法业务编码，例如 {@code GDAL_INTERSECTION} */
    private String algorithmCode;

    /** 算法名称（人类可读） */
    private String algorithmName;

    /** 契约版本号，例如 {@code 1.0}。Worker 必须上报相同版本才能接收任务 */
    private String contractVersion;

    /**
     * 契约 checksum（例如 {@code sha256:xxx}），由 contractVersion + inputs + outputs
     * 序列化后哈希得到，用于防止 Worker 假冒实现。
     */
    private String contractChecksum;

    /**
     * 算法输入契约，key 为输入名（如 {@code sourceLayer}），value 为输入定义。
     * 用户在 DAG 节点 {@code inputBindings} 中只能配置这些已声明的输入名（文档 §3.1 末）。
     */
    private Map<String, AlgorithmInputDef> inputs = new LinkedHashMap<>();

    /**
     * 算法输出契约，key 为输出端口名（如 {@code resultLayer}），value 为输出定义。
     * 决定下游 {@code NODE_OUTPUT + outputKey} 可引用的范围。
     */
    private Map<String, AlgorithmOutputDef> outputs = new LinkedHashMap<>();

    public AlgorithmContract(String algorithmCode, String algorithmName, String contractVersion) {
        this.algorithmCode = Objects.requireNonNull(algorithmCode, "algorithmCode");
        this.algorithmName = Objects.requireNonNull(algorithmName, "algorithmName");
        this.contractVersion = Objects.requireNonNull(contractVersion, "contractVersion");
    }

    /**
     * 查找指定输入端口定义。DAG 发布校验时使用（文档 §7 第 3、9 项）。
     *
     * @param inputName 输入端口名
     * @return 输入定义，不存在返回 null
     */
    public AlgorithmInputDef getInput(String inputName) {
        return inputs == null ? null : inputs.get(inputName);
    }

    /**
     * 查找指定输出端口定义。DAG 发布校验时使用（文档 §7 第 6 项）。
     *
     * @param outputKey 输出端口名
     * @return 输出定义，不存在返回 null
     */
    public AlgorithmOutputDef getOutput(String outputKey) {
        return outputs == null ? null : outputs.get(outputKey);
    }

    /**
     * 获取主要输出端口名（{@code primary=true}）。
     * 用于 {@code NodeInstance.result_ref} 自动填充（文档 §13.3）。
     *
     * @return 主要输出端口名；无 primary 输出时返回 null
     */
    public String getPrimaryOutputKey() {
        if (outputs == null || outputs.isEmpty()) {
            return null;
        }
        return outputs.entrySet().stream()
                .filter(e -> e.getValue() != null && e.getValue().isPrimary())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }
}
