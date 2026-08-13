package com.staterelay.contract.protocol;

import java.time.Instant;
import java.util.List;

/**
 * A batch of Worker log entries associated with one fenced task attempt.
 *
 * <p>The task, attempt, lease, Worker ID, and epoch fields prevent logs from a
 * superseded execution from being attributed to the active attempt.</p>
 */
public record TaskLogBatch(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        List<LogEntry> entries) {

    /**
     * One timestamped task log entry.
     */
    public record LogEntry(Instant timestamp, String level, String message) {
    }
}
