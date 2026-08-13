package com.staterelay.contract.protocol;

import java.time.Instant;

/**
 * Non-terminal progress emitted by a Worker for one fenced task attempt.
 *
 * <p>The scheduler associates a report only with the matching lease and Worker
 * epoch, preventing a prior execution from updating current progress.</p>
 */
public record TaskProgressReport(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        int percent,
        String message,
        Instant reportedAt) {
}
