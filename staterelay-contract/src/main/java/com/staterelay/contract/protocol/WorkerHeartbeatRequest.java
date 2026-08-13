package com.staterelay.contract.protocol;

import java.time.Instant;

public record WorkerHeartbeatRequest(
        String application,
        String workerId,
        String workerEpoch,
        int availableCapacity,
        Instant sentAt) {
}
