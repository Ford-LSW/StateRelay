package com.staterelay.server.dag.orchestration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.ArtifactRef;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.algorithm.AlgorithmDataType;
import com.staterelay.contract.dag.algorithm.AlgorithmInputDef;
import com.staterelay.contract.dag.artifact.SpatialArtifactState;
import com.staterelay.contract.dag.binding.BindingSourceType;
import com.staterelay.contract.dag.binding.InputBinding;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.contract.dag.spatial.VectorLayerRef;
import com.staterelay.server.dag.entity.DagArtifactEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.repository.DagArtifactRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import com.staterelay.server.dag.service.DagArtifactService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 参数解析器（对齐文档 §16 ParameterResolver、§8 ParameterResolver 运行时解析）。
 *
 * <p>Scheduler 在 READY → DISPATCHING 后调用本组件，将 {@link NodeInstanceEntity#getInputBindingsSnapshot()}
 * 中保存的 binding 快照解析为最终请求参数 JSON。
 *
 * <p>与 {@link BindingResolver} 的区别：
 * <ul>
 *   <li>{@link BindingResolver}：DAG Engine 阶段，WAITING → READY 时冻结 binding 快照</li>
 *   <li>{@link ParameterResolver}：Scheduler 阶段，DISPATCHING 时解析 binding 快照为最终 params（读上游 Artifact）</li>
 * </ul>
 *
 * <p>解析失败时抛出 {@link InputResolveException}，由调用方按 §17 处理：
 * CAS NodeInstance DISPATCHING → FAILED（不重试，+1 finished_node_count）。
 *
 * <p><b>双协议并存：</b>
 * <ul>
 *   <li>旧协议（snapshot 为 Map<String, Object>）：值可以是 {@code ${dag.inputs.*}} /
 *       {@code ${nodes.*.outputs.*}} 字符串表达式或静态值。本类按值是否为 {@code ${}} 字符串解析。</li>
 *   <li>新协议（snapshot 为 Map<String, InputBinding>）：每个值含 {@code sourceType} 字段，
 *       本类按 {@link BindingSourceType} 分支解析：
 *       <ul>
 *         <li>{@link BindingSourceType#DAG_INPUT}：从 DagInstance.input_json 读取</li>
 *         <li>{@link BindingSourceType#NODE_OUTPUT}：读上游 NodeInstance.result_json.outputs[outputKey]，
 *             按 selector 取子值；并校验上游 SUCCESS + Artifact AVAILABLE</li>
 *         <li>{@link BindingSourceType#CONST}：直接返回 value</li>
 *       </ul>
 *   </li>
 * </ul>
 */
@Component
public class ParameterResolver {

    private static final String PREFIX_DAG_INPUT = "${dag.inputs.";
    private static final String PREFIX_NODE_OUTPUT = "${nodes.";
    private static final String FIELD_SOURCE_TYPE = "sourceType";

    private final DagArtifactService artifactService;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final DagArtifactRepository dagArtifactRepository;
    private final ObjectMapper objectMapper;

    public ParameterResolver(DagArtifactService artifactService,
                             NodeInstanceJpaRepository nodeInstanceRepository,
                             DagArtifactRepository dagArtifactRepository,
                             ObjectMapper objectMapper) {
        this.artifactService = artifactService;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.dagArtifactRepository = dagArtifactRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * 解析 NodeInstance 的 input_bindings_snapshot 为最终请求参数 JSON。
     *
     * <p>自动识别 snapshot 是新协议（InputBinding Map）还是旧协议（${} 表达式 + 静态值）。
     *
     * @param instance DAG 实例（用于读取 input_json 和 dag_instance_id）
     * @param node     NodeInstance（input_bindings_snapshot 已在 READY 阶段冻结）
     * @return 最终请求参数 JSON 字符串
     * @throws InputResolveException 当引用字段不存在或解析失败时抛出
     */
    public String resolve(DagInstanceEntity instance, NodeInstanceEntity node) {
        String snapshot = node.getInputBindingsSnapshot();
        if (snapshot == null || snapshot.isBlank()) {
            return "{}";
        }
        // 先尝试新协议（InputBinding Map）
        Map<String, InputBinding> bindings = tryParseInputBindings(snapshot);
        if (bindings != null) {
            return resolveInputBindings(instance, bindings);
        }
        // 旧协议分支
        Map<String, Object> legacy = parseBindings(snapshot);
        Map<String, Object> resolved = new HashMap<>();
        legacy.forEach((key, rawValue) -> resolved.put(key, resolveValue(rawValue, instance)));
        try {
            return objectMapper.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            throw new InputResolveException(
                "Failed to serialize resolved params for node " + node.getNodeId(), e);
        }
    }

    /**
     * 按算法契约解析 NodeInstance 输入。
     *
     * @param instance DAG 实例
     * @param node NodeInstance
     * @param contract 当前算法契约
     * @return 最终请求参数 JSON 字符串
     */
    public String resolve(DagInstanceEntity instance, NodeInstanceEntity node, AlgorithmContract contract) {
        if (contract == null) {
            throw new InputResolveException("Algorithm contract is required");
        }
        if (!isStructuredSnapshot(node.getInputBindingsSnapshot())) {
            throw new InputResolveException(
                "Invalid structured input binding snapshot for algorithm " + contract.getAlgorithmCode());
        }
        Map<String, Object> resolved = parseBindings(resolve(instance, node));
        Map<String, AlgorithmInputDef> inputDefs = contract.getInputs() == null
            ? Map.of() : contract.getInputs();

        for (String inputName : resolved.keySet()) {
            if (!inputDefs.containsKey(inputName)) {
                throw new InputResolveException(
                    "Input " + inputName + " is not declared by algorithm " + contract.getAlgorithmCode());
            }
        }
        for (Map.Entry<String, AlgorithmInputDef> entry : inputDefs.entrySet()) {
            String inputName = entry.getKey();
            AlgorithmInputDef inputDef = entry.getValue();
            if (inputDef == null || inputDef.getDataType() == null || inputDef.getDataType().isBlank()) {
                throw new InputResolveException("Invalid algorithm input definition: " + inputName);
            }
            if (!resolved.containsKey(inputName)) {
                if (inputDef.getDefaultValue() != null) {
                    resolved.put(inputName, inputDef.getDefaultValue());
                } else if (inputDef.isRequired()) {
                    throw new InputResolveException("Missing required input: " + inputName);
                } else {
                    continue;
                }
            }
            Object value = resolved.get(inputName);
            validateResolvedValue(inputName, value);
            validateContractType(inputName, value, inputDef.getDataType());
        }
        try {
            return objectMapper.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            throw new InputResolveException("Failed to serialize contract-resolved params", e);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isStructuredSnapshot(String snapshot) {
        if (snapshot == null || snapshot.isBlank()) {
            return true;
        }
        try {
            Map<String, Object> raw = objectMapper.readValue(snapshot, Map.class);
            return raw == null || raw.isEmpty() || raw.values().stream()
                .anyMatch(value -> value instanceof Map<?, ?> mapValue
                    && mapValue.containsKey(FIELD_SOURCE_TYPE));
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * 尝试把 snapshot 反序列化为 Map<String, InputBinding>；
     * 若任一值含 sourceType 字段，判定为新协议；否则返回 null 走旧协议路径。
     */
    @SuppressWarnings("unchecked")
    private Map<String, InputBinding> tryParseInputBindings(String snapshot) {
        try {
            Map<String, Object> raw = objectMapper.readValue(snapshot, Map.class);
            if (raw == null || raw.isEmpty()) {
                return null;
            }
            boolean anyInputBinding = raw.values().stream()
                .anyMatch(value -> value instanceof Map<?, ?> mapValue
                    && mapValue.containsKey(FIELD_SOURCE_TYPE));
            if (!anyInputBinding) {
                return null;
            }
            Map<String, InputBinding> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                Object value = entry.getValue();
                if (!(value instanceof Map<?, ?> mapValue)
                        || !mapValue.containsKey(FIELD_SOURCE_TYPE)) {
                    throw invalidStructuredBinding(entry.getKey(), null);
                }
                try {
                    InputBinding binding = objectMapper.convertValue(value, InputBinding.class);
                    if (binding.getSourceType() == null || !binding.hasRequiredFields()) {
                        throw invalidStructuredBinding(entry.getKey(), null);
                    }
                    result.put(entry.getKey(), binding);
                } catch (IllegalArgumentException e) {
                    throw invalidStructuredBinding(entry.getKey(), e);
                }
            }
            return result;
        } catch (InputResolveException e) {
            throw e;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private InputResolveException invalidStructuredBinding(String inputName, Exception cause) {
        String message = "Invalid structured input binding: input=" + inputName;
        return cause == null ? new InputResolveException(message) : new InputResolveException(message, cause);
    }

    /**
     * 新协议解析（对齐文档 §8.1）：
     * 按 {@link BindingSourceType} 分支处理，并执行 §8.1 / §8.3 的运行时校验。
     */
    private String resolveInputBindings(DagInstanceEntity instance, Map<String, InputBinding> bindings) {
        Map<String, Object> resolved = new HashMap<>();
        for (Map.Entry<String, InputBinding> entry : bindings.entrySet()) {
            String inputName = entry.getKey();
            InputBinding binding = entry.getValue();
            if (binding == null || binding.getSourceType() == null) {
                throw new InputResolveException(
                    "Input " + inputName + " has null binding or sourceType");
            }
            if (!binding.hasRequiredFields()) {
                throw new InputResolveException(
                    "Input " + inputName + " binding missing required fields for sourceType=" + binding.getSourceType());
            }
            Object value = resolveBinding(inputName, binding, instance);
            validateResolvedValue(inputName, value);
            resolved.put(inputName, value);
        }
        try {
            return objectMapper.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            throw new InputResolveException("Failed to serialize resolved params", e);
        }
    }

    private Object resolveBinding(String inputName, InputBinding binding, DagInstanceEntity instance) {
        switch (binding.getSourceType()) {
            case DAG_INPUT:
                return resolveDagInputBinding(inputName, binding, instance);
            case NODE_OUTPUT:
                return resolveNodeOutputBinding(inputName, binding, instance);
            case CONST:
                return binding.getValue();
            default:
                throw new InputResolveException(
                    "Input " + inputName + " has unsupported sourceType: " + binding.getSourceType());
        }
    }

    /**
     * §8.1 DAG_INPUT 解析：从 DagInstance.input_json 读取。
     */
    private Object resolveDagInputBinding(String inputName, InputBinding binding, DagInstanceEntity instance) {
        try {
            JsonNode root = objectMapper.readTree(instance.getInputJson());
            JsonNode node = root.get(binding.getKey());
            if (node == null || node.isMissingNode() || node.isNull()) {
                throw new InputResolveException(
                    "DAG input not found: " + binding.getKey() + " (input " + inputName
                        + ", instance=" + instance.getId() + ")");
            }
            return objectMapper.treeToValue(node, Object.class);
        } catch (InputResolveException e) {
            throw e;
        } catch (Exception e) {
            throw new InputResolveException(
                "Failed to read dag input: " + binding.getKey() + " (input " + inputName + ")", e);
        }
    }

    /**
     * §8.1 / §8.3 NODE_OUTPUT 解析：
     * <ol>
     *   <li>根据 dagInstanceId + nodeCode 找到上游 NodeInstance</li>
     *   <li>要求 NodeInstance.status = SUCCESS</li>
     *   <li>读取 result_json.outputs[outputKey]</li>
     *   <li>应用可选 selector（MAP_KEY）</li>
     * </ol>
     */
    private Object resolveNodeOutputBinding(String inputName, InputBinding binding, DagInstanceEntity instance) {
        NodeInstanceEntity upstream = nodeInstanceRepository
            .findByDagInstanceIdAndNodeId(instance.getId(), binding.getNodeCode())
            .orElseThrow(() -> new InputResolveException(
                "Upstream node not found: dagInstanceId=" + instance.getId()
                    + ", nodeCode=" + binding.getNodeCode() + " (input " + inputName + ")"));
        if (upstream.getStatus() != NodeInstanceStatus.SUCCESS) {
            throw new InputResolveException(
                "Upstream node not SUCCESS: nodeCode=" + binding.getNodeCode()
                    + ", status=" + upstream.getStatus() + " (input " + inputName + ")");
        }
        JsonNode outputsNode;
        try {
            JsonNode resultRoot = upstream.getResultJson() == null || upstream.getResultJson().isBlank()
                ? objectMapper.createObjectNode()
                : objectMapper.readTree(upstream.getResultJson());
            outputsNode = resultRoot.get("outputs");
        } catch (Exception e) {
            throw new InputResolveException(
                "Failed to parse upstream result_json: nodeCode=" + binding.getNodeCode()
                    + " (input " + inputName + ")", e);
        }
        if (outputsNode == null || outputsNode.isMissingNode() || outputsNode.isNull()) {
            throw new InputResolveException(
                "Upstream outputs missing: nodeCode=" + binding.getNodeCode()
                    + " (input " + inputName + ")");
        }
        JsonNode outputValue = outputsNode.get(binding.getOutputKey());
        if (outputValue == null || outputValue.isMissingNode() || outputValue.isNull()) {
            throw new InputResolveException(
                "Upstream outputKey not found: nodeCode=" + binding.getNodeCode()
                    + ", outputKey=" + binding.getOutputKey() + " (input " + inputName + ")");
        }
        // 应用 selector
        if (binding.getSelector() != null) {
            outputValue = applySelector(binding, outputValue, inputName);
        }
        try {
            return objectMapper.treeToValue(outputValue, Object.class);
        } catch (Exception e) {
            throw new InputResolveException(
                "Failed to convert upstream output value: " + outputValue, e);
        }
    }

    /**
     * §6.3 / §13.2 selector：MAP_KEY 从 Map 输出按 key 取值。
     */
    private JsonNode applySelector(InputBinding binding, JsonNode value, String inputName) {
        switch (binding.getSelector().getType()) {
            case MAP_KEY:
                String key = binding.getSelector().getKey();
                JsonNode selected = value.get(key);
                if (selected == null || selected.isMissingNode() || selected.isNull()) {
                    throw new InputResolveException(
                        "MAP_KEY selector key not found: key=" + key
                            + " (input " + inputName + ")");
                }
                return selected;
            default:
                throw new InputResolveException(
                    "Unsupported selector type: " + binding.getSelector().getType()
                        + " (input " + inputName + ")");
        }
    }

    /**
     * §8.1 / §8.3 运行时校验：
     * <ul>
     *   <li>解析值非空</li>
     *   <li>如果是 VectorLayerRef，校验 hasRequiredFields + 引用的 Artifact 状态 AVAILABLE</li>
     * </ul>
     */
    private void validateResolvedValue(String inputName, Object value) {
        if (value == null) {
            throw new InputResolveException("Input " + inputName + " resolved to null");
        }
        // 尝试转为 VectorLayerRef 做图层级校验
        if (value instanceof Map<?, ?> map && map.containsKey("dataType")
                && "VECTOR_LAYER".equals(map.get("dataType"))) {
            VectorLayerRef layerRef;
            try {
                layerRef = objectMapper.convertValue(value, VectorLayerRef.class);
            } catch (IllegalArgumentException e) {
                throw new InputResolveException(
                    "Input " + inputName + " has invalid VECTOR_LAYER structure: " + e.getMessage(), e);
            }
            if (!layerRef.hasRequiredFields()) {
                throw new InputResolveException(
                    "Input " + inputName + " VectorLayerRef missing required fields for storageType="
                        + layerRef.getStorageType());
            }
            // FileGDB 形态必须校验 Artifact 状态 = AVAILABLE
            if (layerRef.getArtifactId() != null) {
                validateArtifactAvailable(layerRef.getArtifactId(), inputName);
            }
        }
    }

    private void validateContractType(String inputName, Object value, String expectedType) {
        boolean matches = switch (expectedType) {
            case "STRING" -> value instanceof String;
            case "INTEGER" -> value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long || value instanceof java.math.BigInteger;
            case "NUMBER", "DECIMAL" -> value instanceof Number;
            case "BOOLEAN" -> value instanceof Boolean;
            case "MAP", "OBJECT" -> value instanceof Map<?, ?>;
            case "LIST", "ARRAY" -> value instanceof List<?>;
            case "JSON" -> true;
            case "VECTOR_LAYER_REF" -> value instanceof Map<?, ?> map
                && ("VECTOR_LAYER".equals(map.get("dataType"))
                    || "VECTOR_LAYER_REF".equals(map.get("dataType")));
            case "VECTOR_DATASET_REF" -> value instanceof Map<?, ?> map
                && ("VECTOR_DATASET".equals(map.get("dataType"))
                    || "VECTOR_DATASET_REF".equals(map.get("dataType")));
            default -> AlgorithmDataType.isSupportedOptionsType(expectedType)
                && value instanceof Map<?, ?>;
        };
        if (!matches) {
            throw new InputResolveException(
                "Input " + inputName + " expected " + expectedType + " but resolved to "
                    + value.getClass().getSimpleName());
        }
    }

    /**
     * §8.1 校验 Artifact 状态 = AVAILABLE，下游读取前置条件（§16.2）。
     */
    private void validateArtifactAvailable(Long artifactId, String inputName) {
        Optional<DagArtifactEntity> artifact = dagArtifactRepository.findById(artifactId);
        if (artifact.isEmpty()) {
            throw new InputResolveException(
                "Input " + inputName + " references non-existent artifactId=" + artifactId);
        }
        DagArtifactEntity entity = artifact.get();
        if (entity.getStatus() != SpatialArtifactState.AVAILABLE) {
            throw new InputResolveException(
                "Input " + inputName + " references artifact " + artifactId
                    + " not AVAILABLE (current=" + entity.getStatus() + ")");
        }
    }

    // ===== 旧协议路径（保持原有逻辑，与新协议并存） =====

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseBindings(String bindingsJson) {
        if (bindingsJson == null || bindingsJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(bindingsJson, Map.class);
        } catch (JsonProcessingException e) {
            throw new InputResolveException("Invalid input_bindings_snapshot JSON: " + bindingsJson, e);
        }
    }

    private Object resolveValue(Object rawValue, DagInstanceEntity instance) {
        if (!(rawValue instanceof String str)) {
            return rawValue;
        }
        if (str.startsWith(PREFIX_DAG_INPUT) && str.endsWith("}")) {
            String key = str.substring(PREFIX_DAG_INPUT.length(), str.length() - 1);
            return readDagInput(instance, key);
        }
        if (str.startsWith(PREFIX_NODE_OUTPUT) && str.endsWith("}")) {
            return resolveNodeOutputRef(str, instance);
        }
        return rawValue;
    }

    private Object readDagInput(DagInstanceEntity instance, String key) {
        try {
            JsonNode root = objectMapper.readTree(instance.getInputJson());
            JsonNode node = root.get(key);
            if (node == null || node.isMissingNode() || node.isNull()) {
                throw new InputResolveException(
                    "DAG input not found: dag.inputs." + key + " (instance=" + instance.getId() + ")");
            }
            return objectMapper.treeToValue(node, Object.class);
        } catch (InputResolveException e) {
            throw e;
        } catch (Exception e) {
            throw new InputResolveException("Failed to read dag input: " + key, e);
        }
    }

    private Object resolveNodeOutputRef(String ref, DagInstanceEntity instance) {
        // ${nodes.<nodeId>.outputs.<name>}
        String body = ref.substring(PREFIX_NODE_OUTPUT.length(), ref.length() - 1);
        String[] parts = body.split("\\.");
        if (parts.length < 4 || !"outputs".equals(parts[1])) {
            throw new InputResolveException("Invalid node output reference: " + ref);
        }
        String fromNodeId = parts[0];
        String outputName = parts[2];
        List<DagArtifactEntity> artifacts = artifactService.findNodeArtifacts(instance.getId(), fromNodeId);
        return artifacts.stream()
            .filter(a -> outputName.equals(a.getOutputName()))
            .findFirst()
            .map(this::toArtifactRef)
            .orElseThrow(() -> new InputResolveException(
                "Upstream artifact not found: node=" + fromNodeId + ", output=" + outputName));
    }

    private ArtifactRef toArtifactRef(DagArtifactEntity entity) {
        Map<String, Object> value = parseJson(entity.getValueJson());
        Map<String, Object> metadata = parseJson(entity.getMetadata());
        return new ArtifactRef(
            entity.getArtifactType(),
            entity.getStorageType(),
            entity.getUri(),
            value,
            metadata
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            return Map.of();
        }
    }
}
