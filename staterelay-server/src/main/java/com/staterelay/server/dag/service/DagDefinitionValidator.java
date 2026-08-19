package com.staterelay.server.dag.service;

import com.staterelay.contract.dag.DagDefinition;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * DAG 定义拓扑校验器（对齐文档 §3.2）。
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
 * <p>注：algorithmCode 跨表校验在 {@link com.staterelay.server.dag.repository.AlgorithmDefinitionRepository} 引入后补充。
 */
@Component
public class DagDefinitionValidator {

    /**
     * 校验 DAG 定义快照，失败抛 {@link IllegalArgumentException}。
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
}
