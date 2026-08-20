package com.staterelay.server.dag.orchestration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.ArtifactRef;
import com.staterelay.server.dag.entity.DagArtifactEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.service.DagArtifactService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 参数解析器（对齐文档 §16 ParameterResolver）。
 *
 * <p>Scheduler 在 READY → DISPATCHING 后调用本组件，将 {@link NodeInstanceEntity#getInputBindingsSnapshot()}
 * 中保存的 binding 快照解析为最终请求参数 JSON。
 *
 * <p>与 {@link BindingResolver} 的区别：
 * <ul>
 *   <li>{@link BindingResolver}：DAG Engine 阶段，WAITING → READY 时冻结 binding 快照（不解析 Artifact 引用）</li>
 *   <li>{@link ParameterResolver}：Scheduler 阶段，DISPATCHING 时解析 binding 快照为最终 params（读上游 Artifact）</li>
 * </ul>
 *
 * <p>解析失败时抛出 {@link InputResolveException}，由调用方按 §17 处理：
 * CAS NodeInstance DISPATCHING → FAILED（不重试，+1 finished_node_count）。
 *
 * <p>支持的引用语法：
 * <ul>
 *   <li>{@code ${dag.inputs.<key>}}：从 DagInstance.input_json 取值</li>
 *   <li>{@code ${nodes.<nodeId>.outputs.<name>}}：从 DagArtifact 表取上游 ArtifactRef</li>
 *   <li>静态值：原样保留</li>
 * </ul>
 */
@Component
public class ParameterResolver {

    private static final String PREFIX_DAG_INPUT = "${dag.inputs.";
    private static final String PREFIX_NODE_OUTPUT = "${nodes.";

    private final DagArtifactService artifactService;
    private final ObjectMapper objectMapper;

    public ParameterResolver(DagArtifactService artifactService, ObjectMapper objectMapper) {
        this.artifactService = artifactService;
        this.objectMapper = objectMapper;
    }

    /**
     * 解析 NodeInstance 的 input_bindings_snapshot 为最终请求参数 JSON。
     *
     * @param instance DAG 实例（用于读取 input_json 和 dag_instance_id）
     * @param node     NodeInstance（input_bindings_snapshot 已在 READY 阶段冻结）
     * @return 最终请求参数 JSON 字符串
     * @throws InputResolveException 当引用字段不存在或解析失败时抛出
     */
    public String resolve(DagInstanceEntity instance, NodeInstanceEntity node) {
        Map<String, Object> bindings = parseBindings(node.getInputBindingsSnapshot());
        Map<String, Object> resolved = new HashMap<>();
        bindings.forEach((key, rawValue) -> resolved.put(key, resolveValue(rawValue, instance)));
        try {
            return objectMapper.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            throw new InputResolveException(
                "Failed to serialize resolved params for node " + node.getNodeId(), e);
        }
    }

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
