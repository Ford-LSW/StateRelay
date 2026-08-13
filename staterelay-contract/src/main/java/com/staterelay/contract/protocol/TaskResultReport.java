package com.staterelay.contract.protocol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Terminal Worker report for one task attempt.
 *
 * <p>The scheduler accepts a terminal outcome only when the task instance,
 * attempt, lease version, Worker ID, and Worker epoch match its current fence;
 * this prevents stale results from completing a replacement attempt.</p>
 */
public record TaskResultReport(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        TerminalStatus terminalStatus,
        JsonNode result,
        String errorCode,
        String errorMessage,
        Instant startedAt,
        Instant finishedAt) {

    /**
     * Terminal state reported by a task execution.
     */
    public enum TerminalStatus {
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
