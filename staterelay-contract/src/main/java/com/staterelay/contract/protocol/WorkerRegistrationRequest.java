package com.staterelay.contract.protocol;

import java.time.Instant;
import java.util.Set;

public record WorkerRegistrationRequest(
        String application,
        String workerId,
        String workerEpoch,
        Set<String> handlerNames,
        int capacity,
        Instant registeredAt) {
}
