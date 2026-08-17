package com.staterelay.server.dispatch;

import com.staterelay.server.persistence.TaskInstanceRepository;

import java.time.Instant;
import java.util.Objects;

public final class DispatchScanner {

    private final TaskInstanceRepository taskInstances;
    private final DispatchService dispatchService;
    private final int assignmentBatchSize;
    private final int transportBatchSize;

    public DispatchScanner(
            TaskInstanceRepository taskInstances,
            DispatchService dispatchService,
            int assignmentBatchSize,
            int transportBatchSize) {
        this.taskInstances = Objects.requireNonNull(taskInstances, "taskInstances");
        this.dispatchService = Objects.requireNonNull(dispatchService, "dispatchService");
        this.assignmentBatchSize = requirePositive(assignmentBatchSize, "assignmentBatchSize");
        this.transportBatchSize = requirePositive(transportBatchSize, "transportBatchSize");
    }

    /** Claims and dispatches a bounded READY/RETRY_WAIT batch. */
    public int scanReadyInstances(Instant now) {
        Objects.requireNonNull(now, "now");
        int assigned = 0;
        for (TaskInstanceRepository.ClaimedTaskInstance claimed
                : taskInstances.claimReadyBatch(now, assignmentBatchSize)) {
            var assignment = dispatchService.assign(claimed);
            if (assignment.isPresent()) {
                assigned++;
                dispatchService.deliver(assignment.get());
            }
        }
        return assigned;
    }

    /** Retransmits a bounded batch of due uncertain transport attempts. */
    public int retryUncertainDispatches(Instant now) {
        return dispatchService.retryUncertain(now, transportBatchSize);
    }

    private int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
