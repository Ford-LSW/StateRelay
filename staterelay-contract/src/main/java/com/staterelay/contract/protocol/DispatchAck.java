package com.staterelay.contract.protocol;

/**
 * Worker's response to an {@link ExecuteTaskCommand}.
 *
 * <p>The acknowledgement identifies the dispatch and fenced attempt so the
 * scheduler can distinguish acceptance, idempotent retransmission, capacity or
 * handler rejection, and commands addressed to a stale Worker epoch.</p>
 */
public record DispatchAck(
        String dispatchId,
        String attemptId,
        String workerId,
        String workerEpoch,
        AckStatus status,
        String message) {

    /**
     * Outcome of handling one dispatch command.
     */
    public enum AckStatus {
        ACCEPTED,
        DUPLICATE,
        REJECTED_CAPACITY,
        REJECTED_HANDLER,
        REJECTED_STALE_EPOCH
    }
}
