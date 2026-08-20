package com.staterelay.server.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * DAG 节点执行状态机，校验业务状态流转合法性。
 */
public final class NodeExecutionStateMachine {

    private static final Map<NodeExecutionStatus, Set<NodeExecutionStatus>> TRANSITIONS = transitions();

    public void requireTransition(NodeExecutionStatus from, NodeExecutionStatus to) {
        if (!TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalStateException(
                "Node execution transition is not allowed: " + from + " -> " + to);
        }
    }

    private static Map<NodeExecutionStatus, Set<NodeExecutionStatus>> transitions() {
        Map<NodeExecutionStatus, Set<NodeExecutionStatus>> t = new EnumMap<>(NodeExecutionStatus.class);
        t.put(NodeExecutionStatus.PENDING, EnumSet.of(
            NodeExecutionStatus.WAITING,
            NodeExecutionStatus.CANCELLED));
        t.put(NodeExecutionStatus.WAITING, EnumSet.of(
            NodeExecutionStatus.READY,
            NodeExecutionStatus.SKIPPED,
            NodeExecutionStatus.CANCELLED));
        t.put(NodeExecutionStatus.READY, EnumSet.of(
            NodeExecutionStatus.RUNNING,
            NodeExecutionStatus.CANCELLING,
            NodeExecutionStatus.CANCELLED));
        t.put(NodeExecutionStatus.RUNNING, EnumSet.of(
            NodeExecutionStatus.SUCCESS,
            NodeExecutionStatus.FAILED,
            NodeExecutionStatus.CANCELLING,
            NodeExecutionStatus.CANCELLED));
        t.put(NodeExecutionStatus.FAILED, EnumSet.of(NodeExecutionStatus.READY, NodeExecutionStatus.CANCELLED));
        t.put(NodeExecutionStatus.CANCELLING, EnumSet.of(
            NodeExecutionStatus.CANCELLED,
            NodeExecutionStatus.SUCCESS,
            NodeExecutionStatus.FAILED));
        return Map.copyOf(t);
    }
}
