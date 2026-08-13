package com.staterelay.contract.protocol;

public record DispatchAck(
        String dispatchId,
        String attemptId,
        String workerId,
        String workerEpoch,
        AckStatus status,
        String message) {

    public enum AckStatus {
        ACCEPTED,
        DUPLICATE,
        REJECTED_CAPACITY,
        REJECTED_HANDLER,
        REJECTED_STALE_EPOCH
    }
}
