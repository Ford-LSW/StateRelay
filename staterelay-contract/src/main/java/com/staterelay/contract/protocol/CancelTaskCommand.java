package com.staterelay.contract.protocol;

import java.time.Instant;

public record CancelTaskCommand(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String targetWorkerId,
        String targetWorkerEpoch,
        String reason,
        Instant requestedAt) {
}
