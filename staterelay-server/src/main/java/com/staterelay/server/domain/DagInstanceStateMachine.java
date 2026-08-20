package com.staterelay.server.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * DAG 实例状态机，校验业务状态流转合法性。
 */
public final class DagInstanceStateMachine {

    private static final Map<DagInstanceStatus, Set<DagInstanceStatus>> TRANSITIONS = transitions();

    public void requireTransition(DagInstanceStatus from, DagInstanceStatus to) {
        if (!TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalStateException(
                "DAG instance transition is not allowed: " + from + " -> " + to);
        }
    }

    private static Map<DagInstanceStatus, Set<DagInstanceStatus>> transitions() {
        Map<DagInstanceStatus, Set<DagInstanceStatus>> t = new EnumMap<>(DagInstanceStatus.class);
        t.put(DagInstanceStatus.PENDING, EnumSet.of(DagInstanceStatus.RUNNING, DagInstanceStatus.CANCELLED));
        t.put(DagInstanceStatus.RUNNING, EnumSet.of(
            DagInstanceStatus.SUCCESS,
            DagInstanceStatus.FAILED,
            DagInstanceStatus.CANCELLING,
            DagInstanceStatus.CANCELLED));
        t.put(DagInstanceStatus.CANCELLING, EnumSet.of(
            DagInstanceStatus.CANCELLED,
            DagInstanceStatus.SUCCESS,
            DagInstanceStatus.FAILED));
        return Map.copyOf(t);
    }
}
