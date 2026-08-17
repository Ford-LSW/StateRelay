package com.staterelay.server.dispatch;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface WorkerRouter {

    Optional<WorkerCandidate> choose(
            List<WorkerCandidate> candidates, Requirements requirements);

    record Requirements(String handlerName, Instant now) {
    }

    record WorkerCandidate(
            String workerId,
            String status,
            Instant leaseExpiresAt,
            int reservedCapacity,
            int reportedActiveCount,
            int maxConcurrency,
            Set<String> handlerNames) {

        public int effectiveLoad() {
            return Math.max(reservedCapacity, reportedActiveCount);
        }

        public boolean isEligible(Requirements requirements) {
            return "READY".equals(status)
                    && leaseExpiresAt.isAfter(requirements.now())
                    && handlerNames.contains(requirements.handlerName())
                    && effectiveLoad() < maxConcurrency;
        }
    }
}
