package com.staterelay.contract.protocol;

import java.time.Instant;

public record ActiveExecutionLease(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        Instant expiresAt) {
}
