package com.staterelay.contract.protocol;

import java.time.Instant;

/**
 * Periodic Worker liveness and available-capacity report.
 *
 * <p>The Worker epoch distinguishes this process lifetime from a prior process
 * that reused the same Worker identity.</p>
 */
public record WorkerHeartbeatRequest(
        String application,
        String workerId,
        String workerEpoch,
        int availableCapacity,
        Instant sentAt) {
}
