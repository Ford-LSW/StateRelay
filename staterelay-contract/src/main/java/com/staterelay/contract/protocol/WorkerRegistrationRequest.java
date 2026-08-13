package com.staterelay.contract.protocol;

import java.time.Instant;
import java.util.Set;

/**
 * Initial Worker registration request declaring identity, handlers, and capacity.
 *
 * <p>The Worker epoch represents one process lifetime and lets the scheduler
 * invalidate registrations and leases from an earlier process with the same Worker
 * identity.</p>
 */
public record WorkerRegistrationRequest(
        String application,
        String workerId,
        String workerEpoch,
        Set<String> handlerNames,
        int capacity,
        Instant registeredAt) {
}
