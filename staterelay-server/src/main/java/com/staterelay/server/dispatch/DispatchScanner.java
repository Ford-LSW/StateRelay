package com.staterelay.server.dispatch;

import com.staterelay.server.persistence.TaskInstanceRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public final class DispatchScanner {

    private final TaskInstanceRepository taskInstances;
    private final DispatchService dispatchService;
    private final int assignmentBatchSize;
    private final int transportBatchSize;
    private final Duration claimRetryDelay;
    private final Duration claimTimeout;

    public DispatchScanner(
            TaskInstanceRepository taskInstances,
            DispatchService dispatchService,
            int assignmentBatchSize,
            int transportBatchSize) {
        this(taskInstances, dispatchService, assignmentBatchSize, transportBatchSize,
                Duration.ofSeconds(1), Duration.ofSeconds(30));
    }

    public DispatchScanner(
            TaskInstanceRepository taskInstances,
            DispatchService dispatchService,
            int assignmentBatchSize,
            int transportBatchSize,
            Duration claimRetryDelay,
            Duration claimTimeout) {
        this.taskInstances = Objects.requireNonNull(taskInstances, "taskInstances");
        this.dispatchService = Objects.requireNonNull(dispatchService, "dispatchService");
        this.assignmentBatchSize = requirePositive(assignmentBatchSize, "assignmentBatchSize");
        this.transportBatchSize = requirePositive(transportBatchSize, "transportBatchSize");
        this.claimRetryDelay = requirePositive(claimRetryDelay, "claimRetryDelay");
        this.claimTimeout = requirePositive(claimTimeout, "claimTimeout");
    }

    /** Claims and dispatches a bounded READY/RETRY_WAIT batch. */
    public int scanReadyInstances(Instant now) {
        Objects.requireNonNull(now, "now");
        int assigned = 0;
        List<TaskInstanceRepository.ClaimedTaskInstance> claimedBatch =
                taskInstances.claimReadyBatch(now, assignmentBatchSize, claimTimeout);
        for (int index = 0; index < claimedBatch.size(); index++) {
            TaskInstanceRepository.ClaimedTaskInstance claimed = claimedBatch.get(index);
            try {
                var assignment = dispatchService.assign(claimed);
                if (assignment.isEmpty()) {
                    taskInstances.deferClaim(claimed, now.plus(claimRetryDelay));
                    continue;
                }
                assigned++;
                dispatchService.deliver(assignment.get());
            } catch (RuntimeException | Error failure) {
                deferRemaining(claimedBatch, index, now, failure);
                throw failure;
            }
        }
        return assigned;
    }

    /** Recovers a bounded batch of due transport attempts, including expired send leases. */
    public int retryUncertainDispatches(Instant now) {
        return dispatchService.retryUncertain(now, transportBatchSize);
    }

    private int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private void deferRemaining(
            List<TaskInstanceRepository.ClaimedTaskInstance> claimedBatch,
            int first,
            Instant now,
            Throwable originalFailure) {
        for (int index = first; index < claimedBatch.size(); index++) {
            try {
                taskInstances.deferClaim(
                        claimedBatch.get(index), now.plus(claimRetryDelay));
            } catch (RuntimeException cleanupFailure) {
                originalFailure.addSuppressed(cleanupFailure);
            }
        }
    }
}
