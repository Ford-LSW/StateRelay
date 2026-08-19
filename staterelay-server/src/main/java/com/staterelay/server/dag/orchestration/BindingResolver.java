package com.staterelay.server.dag.orchestration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.ArtifactRef;
import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.server.dag.entity.DagArtifactEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.mapper.DagEdgeMapper;
import com.staterelay.server.dag.service.DagArtifactService;
import com.staterelay.server.dag.service.DagDefinitionService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 输入绑定解析器。
 *
 * <p>节点 WAITING → READY 时调用，将上游 ArtifactRef 解析为节点的 input_bindings_snapshot。
 * <ul>
 *   <li>静态绑定（直接 value）：直接保留</li>
 *   <li>DAG 输入引用（${dag.inputs.xxx}）：从 instance.input_json 取值</li>
 *   <li>节点输出引用（${nodes.&lt;id&gt;.outputs.&lt;name&gt;}）：从上游 Artifact 表取值</li>
 * </ul>
 */
@Component
public class BindingResolver {

    private static final String PREFIX_DAG_INPUT = "${dag.inputs.";
    private static final String PREFIX_NODE_OUTPUT = "${nodes.";

    private final DagDefinitionService definitionService;
    private final DagArtifactService artifactService;
    private final DagEdgeMapper edgeMapper;
    private final ObjectMapper objectMapper;

    public BindingResolver(DagDefinitionService definitionService,
                            DagArtifactService artifactService,
                            DagEdgeMapper edgeMapper,
                            ObjectMapper objectMapper) {
        this.definitionService = definitionService;
        this.artifactService = artifactService;
        this.edgeMapper = objectMapper != null ? edgeMapper : null;
        this.objectMapper = objectMapper;
    }

    /**
     * 解析节点输入绑定，输出可直接写入 input_bindings_snapshot 的 JSON。
     */
    public String resolveBindings(DagInstanceEntity instance,
                                  DagDefinitionVersionLite versionLite,
                                  DagDefinition.DagNode node) {
        Map<String, Object> resolved = new HashMap<>();
        Map<String, Object> inputs = node.getInputs();
        if (inputs != null) {
            inputs.forEach((key, rawValue) -> resolved.put(key, resolveValue(rawValue, instance, versionLite)));
        }
        try {
            return objectMapper.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize bindings", e);
        }
    }

    private Object resolveValue(Object rawValue, DagInstanceEntity instance, DagDefinitionVersionLite versionLite) {
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
            return node != null ? objectMapper.treeToValue(node, Object.class) : null;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read dag input: " + key, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Object resolveNodeOutputRef(String ref, DagInstanceEntity instance) {
        // ${nodes.<nodeId>.outputs.<name>}
        String body = ref.substring(PREFIX_NODE_OUTPUT.length(), ref.length() - 1);
        String[] parts = body.split("\\.");
        if (parts.length < 4 || !"outputs".equals(parts[1])) {
            throw new IllegalArgumentException("Invalid node output reference: " + ref);
        }
        String fromNodeId = parts[0];
        String outputName = parts[2];
        List<DagArtifactEntity> artifacts = artifactService.findNodeArtifacts(instance.getId(), fromNodeId);
        return artifacts.stream()
            .filter(a -> outputName.equals(a.getOutputName()))
            .findFirst()
            .map(this::toArtifactRef)
            .orElseThrow(() -> new IllegalStateException(
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

    /**
     * 轻量版本信息载体，避免重复加载快照。
     */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class DagDefinitionVersionLite {
        private Long versionId;
        private DagDefinition snapshot;
    }
}
