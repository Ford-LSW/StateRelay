package com.staterelay.contract.handler;

/**
 * Runtime metadata and cooperative control channel for a single task attempt.
 *
 * <p>The lease version identifies the scheduler-issued execution fence. A handler
 * must use the idempotency key for externally visible work and should periodically
 * observe cancellation for long-running execution.</p>
 */
public interface TaskContext {

    String taskInstanceId();

    String attemptId();

    long leaseVersion();

    String idempotencyKey();

    boolean isCancellationRequested();

    void reportProgress(int percent, String message);
}
