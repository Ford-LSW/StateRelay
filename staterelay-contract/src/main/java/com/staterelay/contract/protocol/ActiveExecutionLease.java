package com.staterelay.contract.protocol;

import java.time.Instant;

/**
 * A Worker-reported active execution lease used by the scheduler to renew a
 * particular fenced attempt.
 *
 * <p>The {@code leaseVersion}, Worker ID, and Worker epoch identify the exact
 * execution that may be renewed; an older epoch must not renew a replacement.</p>
 */
public record ActiveExecutionLease(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        Instant expiresAt) {
}
