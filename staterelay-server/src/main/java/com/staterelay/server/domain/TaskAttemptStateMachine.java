package com.staterelay.server.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class TaskAttemptStateMachine {

    private static final Map<TaskAttemptStatus, Set<TaskAttemptStatus>> TRANSITIONS = transitions();

    public void requireTransition(TaskAttemptStatus from, TaskAttemptStatus to) {
        if (!TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalStateException("Task attempt transition is not allowed: " + from + " -> " + to);
        }
    }

    private static Map<TaskAttemptStatus, Set<TaskAttemptStatus>> transitions() {
        Map<TaskAttemptStatus, Set<TaskAttemptStatus>> transitions = new EnumMap<>(TaskAttemptStatus.class);
        transitions.put(TaskAttemptStatus.CREATED, EnumSet.of(TaskAttemptStatus.ASSIGNED, TaskAttemptStatus.CANCELLED));
        transitions.put(TaskAttemptStatus.ASSIGNED, EnumSet.of(
                TaskAttemptStatus.ACCEPTED,
                TaskAttemptStatus.LOST,
                TaskAttemptStatus.CANCELLED));
        transitions.put(TaskAttemptStatus.ACCEPTED, EnumSet.of(
                TaskAttemptStatus.RUNNING,
                TaskAttemptStatus.LOST,
                TaskAttemptStatus.CANCELLED,
                TaskAttemptStatus.TIMED_OUT));
        transitions.put(TaskAttemptStatus.RUNNING, EnumSet.of(
                TaskAttemptStatus.SUCCESS,
                TaskAttemptStatus.FAILED,
                TaskAttemptStatus.CANCELLED,
                TaskAttemptStatus.LOST,
                TaskAttemptStatus.TIMED_OUT));
        return Map.copyOf(transitions);
    }
}
