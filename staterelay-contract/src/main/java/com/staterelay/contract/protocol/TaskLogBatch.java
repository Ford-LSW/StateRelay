package com.staterelay.contract.protocol;

import java.time.Instant;
import java.util.List;

public record TaskLogBatch(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        List<LogEntry> entries) {

    public record LogEntry(Instant timestamp, String level, String message) {
    }
}
