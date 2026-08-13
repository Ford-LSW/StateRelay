package com.staterelay.contract.protocol;

import java.time.Instant;

/**
 * Scheduler command asking one specific Worker epoch to cooperatively cancel a
 * fenced task attempt.
 *
 * <p>Recipients must match the task attempt, lease version, Worker ID, and Worker
 * epoch before acting so an old command cannot affect a later execution.</p>
 */
public record CancelTaskCommand(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String targetWorkerId,
        String targetWorkerEpoch,
        String reason,
        Instant requestedAt) {
}
