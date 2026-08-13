package com.staterelay.server.domain;

public enum TaskInstanceStatus {
    WAITING,
    READY,
    RUNNING,
    RETRY_WAIT,
    CANCELLING,
    SUCCESS,
    FAILED,
    CANCELLED
}
