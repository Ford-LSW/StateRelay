package com.staterelay.contract.protocol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;

/**
 * Scheduler-to-Worker command that starts one concrete task attempt.
 *
 * <p>{@code dispatchId} supports idempotent retransmission, while
 * {@code leaseVersion} and {@code targetWorkerEpoch} fence the command to the
 * current execution owner. Workers must reject commands for another ID or epoch.</p>
 */
public record ExecuteTaskCommand(
        String taskInstanceId,
        String attemptId,
        int attemptNumber,
        String dispatchId,
        long leaseVersion,
        String application,
        String targetWorkerId,
        String targetWorkerEpoch,
        String handlerName,
        JsonNode parameter,
        String idempotencyKey,
        Duration leaseDuration,
        Instant dispatchedAt) {
}
