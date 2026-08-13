package com.staterelay.server.domain;

public enum TaskAttemptStatus {
    CREATED,
    ASSIGNED,
    ACCEPTED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    LOST,
    TIMED_OUT
}
