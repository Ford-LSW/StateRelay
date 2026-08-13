package com.staterelay.server.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class TaskInstanceStateMachine {

    private static final Map<TaskInstanceStatus, Set<TaskInstanceStatus>> TRANSITIONS = transitions();

    public void requireTransition(TaskInstanceStatus from, TaskInstanceStatus to) {
        if (!TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalStateException("Task instance transition is not allowed: " + from + " -> " + to);
        }
    }

    private static Map<TaskInstanceStatus, Set<TaskInstanceStatus>> transitions() {
        Map<TaskInstanceStatus, Set<TaskInstanceStatus>> transitions = new EnumMap<>(TaskInstanceStatus.class);
        transitions.put(TaskInstanceStatus.WAITING, EnumSet.of(TaskInstanceStatus.READY, TaskInstanceStatus.CANCELLED));
        transitions.put(TaskInstanceStatus.READY, EnumSet.of(TaskInstanceStatus.RUNNING, TaskInstanceStatus.CANCELLED));
        transitions.put(TaskInstanceStatus.RUNNING, EnumSet.of(
                TaskInstanceStatus.SUCCESS,
                TaskInstanceStatus.RETRY_WAIT,
                TaskInstanceStatus.FAILED,
                TaskInstanceStatus.CANCELLING,
                TaskInstanceStatus.CANCELLED));
        transitions.put(TaskInstanceStatus.RETRY_WAIT, EnumSet.of(TaskInstanceStatus.READY, TaskInstanceStatus.CANCELLED));
        transitions.put(TaskInstanceStatus.CANCELLING, EnumSet.of(
                TaskInstanceStatus.CANCELLED,
                TaskInstanceStatus.SUCCESS,
                TaskInstanceStatus.FAILED));
        return Map.copyOf(transitions);
    }
}
