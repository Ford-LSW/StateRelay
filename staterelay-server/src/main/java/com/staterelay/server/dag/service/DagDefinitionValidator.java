package com.staterelay.server.dag.service;

import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.algorithm.AlgorithmDataType;
import com.staterelay.contract.dag.algorithm.AlgorithmInputDef;
import com.staterelay.contract.dag.algorithm.AlgorithmOutputDef;
import com.staterelay.contract.dag.algorithm.OutputCardinality;
import com.staterelay.contract.dag.binding.InputBinding;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * DAG 定义拓扑校验器（对齐文档 §3.2、§7）。
 *
 * <p>发布（DRAFT → ENABLED）时强制执行，校验失败抛出 {@link IllegalArgumentException}，
 * 状态保持 DRAFT。
 *
 * <p>校验项：
 * <ol>
 *   <li>节点 code 唯一（无重复）</li>
 *   <li>edges 引用的 from / to 节点必须存在</li>
 *   <li>无环（Kahn 算法拓扑排序必须能完成）</li>
 *   <li>至少有一个根节点（入度为 0）</li>
 * </ol>
 *
 * <p>§7 inputBindings 校验（{@link #validateInputBindings}）：
 * <ol>
 *   <li>根据 algorithmCode 找到 AlgorithmDefinition</li>
 *   <li>required=true 的输入必须存在对应 inputBinding</li>
 *   <li>inputBindings 中不能出现算法未声明的输入名称</li>
 *   <li>NODE_OUTPUT 引用的 nodeCode 必须存在</li>
 *   <li>被引用节点必须是当前节点的传递上游节点</li>
 *   <li>outputKey 必须在上游算法输出契约中存在</li>
 *   <li>selector 只能用于允许选择的复合类型</li>
 *   <li>MAP_KEY 选择器结构必须合法</li>
 *   <li>上游输出类型必须与当前算法输入类型兼容（运行时 ParameterResolver 再做最终校验）</li>
 *   <li>CONST 值必须满足对应输入类型的结构校验</li>
 *   <li>不允许通过绑定引用其他 DagInstance 的输出（无 dagInstanceId 字段，结构性保证）</li>
 * </ol>
 *
 * <p>注：algorithmCode 跨表校验在 {@link com.staterelay.server.dag.repository.AlgorithmDefinitionRepository} 引入后补充。
 */
@Component
public class DagDefinitionValidator {

    /**
     * 校验 DAG 定义快照（拓扑结构），失败抛 {@link IllegalArgumentException}。
     */
    public void validate(DagDefinition snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("DAG definition snapshot is null");
        }
        List<DagDefinition.DagNode> nodes = snapshot.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("DAG definition has no nodes");
        }
        List<DagDefinition.DagEdge> edges = snapshot.getEdges() == null
            ? List.of() : snapshot.getEdges();

        // 1. 节点 code 唯一
        Set<String> nodeIds = new HashSet<>();
        for (DagDefinition.DagNode n : nodes) {
            if (n.getId() == null || n.getId().isBlank()) {
                throw new IllegalArgumentException("DAG node has blank id");
            }
            if (!nodeIds.add(n.getId())) {
                throw new IllegalArgumentException("Duplicate node id: " + n.getId());
            }
            // §6/§3.1 末：handler 与 algorithmCode 不能同时为空，也不能同时非空
            boolean hasHandler = n.getHandler() != null && !n.getHandler().isBlank();
            boolean hasAlgoCode = n.getAlgorithmCode() != null && !n.getAlgorithmCode().isBlank();
            if (!hasHandler && !hasAlgoCode) {
                throw new IllegalArgumentException(
                    "Node " + n.getId() + " must have either handler or algorithmCode");
            }
            if (hasHandler && hasAlgoCode) {
                throw new IllegalArgumentException(
                    "Node " + n.getId() + " cannot have both handler and algorithmCode");
            }
            // §6 双协议：inputs 与 inputBindings 不能同时非空
            boolean hasInputs = n.getInputs() != null && !n.getInputs().isEmpty();
            boolean hasBindings = n.getInputBindings() != null && !n.getInputBindings().isEmpty();
            if (hasAlgoCode && hasInputs) {
                throw new IllegalArgumentException(
                    "Node " + n.getId() + " uses algorithmCode and must use structured inputBindings, not inputs");
            }
            if (hasInputs && hasBindings) {
                throw new IllegalArgumentException(
                    "Node " + n.getId() + " cannot use both inputs (legacy) and inputBindings (new protocol)");
            }
            // 新协议必须配合 algorithmCode 使用
            if (hasBindings && !hasAlgoCode) {
                throw new IllegalArgumentException(
                    "Node " + n.getId() + " uses inputBindings but has no algorithmCode");
            }
        }

        // 2. edges 引用的 from / to 必须存在
        for (DagDefinition.DagEdge e : edges) {
            if (!nodeIds.contains(e.getFrom())) {
                throw new IllegalArgumentException(
                    "Edge references unknown from node: " + e.getFrom());
            }
            if (!nodeIds.contains(e.getTo())) {
                throw new IllegalArgumentException(
                    "Edge references unknown to node: " + e.getTo());
            }
            if (e.getFrom().equals(e.getTo())) {
                throw new IllegalArgumentException(
                    "Self-loop edge not allowed: " + e.getFrom());
            }
        }

        // 3. 无环检测（Kahn 算法）
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adjacency = new HashMap<>();
        for (String id : nodeIds) {
            inDegree.put(id, 0);
            adjacency.put(id, new ArrayList<>());
        }
        for (DagDefinition.DagEdge e : edges) {
            inDegree.merge(e.getTo(), 1, Integer::sum);
            adjacency.get(e.getFrom()).add(e.getTo());
        }
        Queue<String> ready = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.offer(entry.getKey());
            }
        }
        int sorted = 0;
        while (!ready.isEmpty()) {
            String cur = ready.poll();
            sorted++;
            for (String next : adjacency.get(cur)) {
                int remain = inDegree.merge(next, -1, Integer::sum);
                if (remain == 0) {
                    ready.offer(next);
                }
            }
        }
        if (sorted != nodeIds.size()) {
            throw new IllegalArgumentException(
                "DAG has cycle: only " + sorted + " of " + nodeIds.size() + " nodes topologically sorted");
        }

        // 4. 至少有一个根节点（入度为 0）
        boolean hasRoot = inDegree.values().stream().anyMatch(v -> v == 0);
        if (!hasRoot) {
            throw new IllegalArgumentException("DAG has no root node (in-degree = 0)");
        }
    }

    /**
     * 校验 §7 inputBindings 11 项校验（仅在 DAG 发布 DRAFT → ENABLED 时调用）。
     *
     * <p>本方法<b>不重复</b>执行 {@link #validate} 已做的拓扑校验，
     * 调用方应先调用 {@link #validate} 保证拓扑结构合法，再调用本方法做绑定校验。
     *
     * @param snapshot           DAG 定义快照（已通过 {@link #validate}）
     * @param algorithmContracts 算法契约集合，key = algorithmCode；
     *                           仅需包含本 DAG 引用的 algorithmCode 对应契约
     */
    public void validateInputBindings(
            DagDefinition snapshot,
            Map<String, AlgorithmContract> algorithmContracts) {
        if (snapshot == null) {
            throw new IllegalArgumentException("DAG definition snapshot is null");
        }
        if (algorithmContracts == null) {
            algorithmContracts = Map.of();
        }

        // 计算每个节点的传递上游集合（§7 第 5 项：被引用节点必须是当前节点的上游）
        Map<String, Set<String>> upstreams = computeTransitivePredecessors(snapshot);

        for (DagDefinition.DagNode node : snapshot.getNodes()) {
            // 仅校验使用新协议（algorithmCode + inputBindings）的节点
            if (node.getAlgorithmCode() == null || node.getAlgorithmCode().isBlank()) {
                continue;
            }
            AlgorithmContract contract = algorithmContracts.get(node.getAlgorithmCode());
            if (contract == null) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " references unknown algorithmCode: " + node.getAlgorithmCode());
            }
            validateNodeBindings(node, contract, upstreams.getOrDefault(node.getId(), Set.of()),
                algorithmContracts, snapshot);
        }
    }

    private void validateNodeBindings(
            DagDefinition.DagNode node,
            AlgorithmContract contract,
            Set<String> upstreams,
            Map<String, AlgorithmContract> algorithmContracts,
            DagDefinition snapshot) {

        Map<String, AlgorithmInputDef> inputDefs = contract.getInputs() == null
            ? Map.of() : contract.getInputs();
        Map<String, InputBinding> bindings = node.getInputBindings() == null
            ? Map.of() : node.getInputBindings();

        // §7 第 2 项：required=true 的输入必须存在对应 inputBinding
        for (Map.Entry<String, AlgorithmInputDef> entry : inputDefs.entrySet()) {
            if (entry.getValue() != null && entry.getValue().isRequired()
                    && !bindings.containsKey(entry.getKey())) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " missing required input binding: " + entry.getKey());
            }
        }

        // §7 第 3 项：inputBindings 中不能出现算法未声明的输入名称
        for (String inputName : bindings.keySet()) {
            if (!inputDefs.containsKey(inputName)) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " binds undeclared input: " + inputName
                        + " (algorithm " + contract.getAlgorithmCode() + ")");
            }
        }

        // 逐个校验每个 binding 的形态
        for (Map.Entry<String, InputBinding> entry : bindings.entrySet()) {
            String inputName = entry.getKey();
            InputBinding binding = entry.getValue();
            if (binding == null || binding.getSourceType() == null) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " input " + inputName + " has null binding/sourceType");
            }
            if (!binding.hasRequiredFields()) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " input " + inputName
                        + " binding missing required fields for sourceType=" + binding.getSourceType());
            }
            switch (binding.getSourceType()) {
                case DAG_INPUT:
                    // §7 第 10 项简化：DAG_INPUT 校验 key 存在性由 ParameterResolver 运行时执行
                    break;
                case NODE_OUTPUT:
                    validateNodeOutputBinding(node, inputName, binding, upstreams,
                        algorithmContracts, snapshot);
                    break;
                case CONST:
                    validateContractValue(node, inputName, binding.getValue(), inputDefs.get(inputName));
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Node " + node.getId() + " input " + inputName
                            + " has unsupported sourceType: " + binding.getSourceType());
            }
        }
    }

    /**
     * §7 第 4 / 5 / 6 / 7 / 8 项：NODE_OUTPUT 绑定校验。
     */
    private void validateNodeOutputBinding(
            DagDefinition.DagNode node,
            String inputName,
            InputBinding binding,
            Set<String> upstreams,
            Map<String, AlgorithmContract> algorithmContracts,
            DagDefinition snapshot) {

        // §7 第 4 项：NODE_OUTPUT 引用的 nodeCode 必须存在
        String refNodeCode = binding.getNodeCode();
        DagDefinition.DagNode refNode = findNode(snapshot, refNodeCode);
        if (refNode == null) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName
                    + " references unknown nodeCode: " + refNodeCode);
        }

        // §7 第 5 项：被引用节点必须是当前节点的传递上游节点
        if (!upstreams.contains(refNodeCode)) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName
                    + " references non-upstream node: " + refNodeCode
                    + " (must be transitive predecessor)");
        }

        // 上游节点必须使用新协议才能查 outputKey 契约
        if (refNode.getAlgorithmCode() == null || refNode.getAlgorithmCode().isBlank()) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName
                    + " references node " + refNodeCode
                    + " which uses legacy handler protocol (no output contract)");
        }
        AlgorithmContract refContract = algorithmContracts.get(refNode.getAlgorithmCode());
        if (refContract == null) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName
                    + " references node " + refNodeCode
                    + " with unknown algorithmCode: " + refNode.getAlgorithmCode());
        }

        // §7 第 6 项：outputKey 必须在上游算法输出契约中存在
        AlgorithmOutputDef outputDef = refContract.getOutput(binding.getOutputKey());
        if (outputDef == null) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName
                    + " references unknown outputKey: " + binding.getOutputKey()
                    + " on node " + refNodeCode + " (algorithm " + refContract.getAlgorithmCode() + ")");
        }

        // §7 第 7 项：selector 只能用于允许选择的复合类型
        if (binding.getSelector() != null) {
            OutputCardinality cardinality = outputDef.getCardinality();
            if (cardinality == OutputCardinality.SINGLE) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " input " + inputName
                        + " uses selector on SINGLE output " + binding.getOutputKey()
                        + " of node " + refNodeCode);
            }
            // §7 第 8 项：MAP_KEY 选择器结构必须合法
            if (binding.getSelector().getType() == null
                    || binding.getSelector().getKey() == null
                    || binding.getSelector().getKey().isBlank()) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " input " + inputName
                        + " has invalid MAP_KEY selector (missing key)");
            }
            if (cardinality != OutputCardinality.MAP) {
                throw new IllegalArgumentException(
                    "Node " + node.getId() + " input " + inputName
                        + " uses MAP_KEY selector on non-MAP output " + binding.getOutputKey());
            }
        }

        AlgorithmInputDef inputDef = algorithmContracts.get(node.getAlgorithmCode()).getInput(inputName);
        if (inputDef != null && !inputDef.getDataType().equals(outputDef.getDataType())) {
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName + " expects " + inputDef.getDataType()
                    + " but upstream output " + binding.getOutputKey() + " is " + outputDef.getDataType());
        }
    }

    private void validateContractValue(DagDefinition.DagNode node, String inputName, Object value,
                                       AlgorithmInputDef inputDef) {
        if (inputDef == null || value == null) {
            return;
        }
        String expectedType = inputDef.getDataType();
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
            throw new IllegalArgumentException(
                "Node " + node.getId() + " input " + inputName + " CONST expected " + expectedType);
        }
    }

    private DagDefinition.DagNode findNode(DagDefinition snapshot, String nodeCode) {
        if (snapshot.getNodes() == null) {
            return null;
        }
        return snapshot.getNodes().stream()
            .filter(n -> nodeCode.equals(n.getId()))
            .findFirst()
            .orElse(null);
    }

    /**
     * 计算每个节点的传递上游节点集合（§7 第 5 项校验依据）。
     *
     * <p>沿 edges 反向 BFS，得到每个节点可达的所有上游节点。
     */
    private Map<String, Set<String>> computeTransitivePredecessors(DagDefinition snapshot) {
        Map<String, Set<String>> directPreds = new HashMap<>();
        for (DagDefinition.DagNode n : snapshot.getNodes()) {
            directPreds.put(n.getId(), new HashSet<>());
        }
        if (snapshot.getEdges() != null) {
            for (DagDefinition.DagEdge e : snapshot.getEdges()) {
                directPreds.computeIfAbsent(e.getTo(), k -> new HashSet<>()).add(e.getFrom());
            }
        }
        // BFS 反向闭包
        Map<String, Set<String>> result = new HashMap<>();
        for (String nodeId : directPreds.keySet()) {
            Set<String> visited = new HashSet<>();
            Queue<String> queue = new LinkedList<>(directPreds.getOrDefault(nodeId, Set.of()));
            while (!queue.isEmpty()) {
                String cur = queue.poll();
                if (visited.add(cur)) {
                    queue.addAll(directPreds.getOrDefault(cur, Set.of()));
                }
            }
            result.put(nodeId, Collections.unmodifiableSet(visited));
        }
        return result;
    }
}
