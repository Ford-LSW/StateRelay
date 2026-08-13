package com.staterelay.contract.handler;

public interface TaskContext {

    String taskInstanceId();

    String attemptId();

    long leaseVersion();

    String idempotencyKey();

    boolean isCancellationRequested();

    void reportProgress(int percent, String message);
}
