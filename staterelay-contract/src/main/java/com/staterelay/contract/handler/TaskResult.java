package com.staterelay.contract.handler;

public record TaskResult<R>(boolean success, R value, String errorCode, String message) {

    public static <R> TaskResult<R> success(R value) {
        return new TaskResult<>(true, value, null, null);
    }

    public static <R> TaskResult<R> failure(String errorCode, String message) {
        return new TaskResult<>(false, null, errorCode, message);
    }
}
