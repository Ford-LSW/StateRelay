package com.staterelay.contract.protocol;

import java.time.Instant;

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
