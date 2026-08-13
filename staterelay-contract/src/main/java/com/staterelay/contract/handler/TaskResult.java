package com.staterelay.contract.handler;

/**
 * Typed terminal outcome returned by a {@link TaskHandler}.
 *
 * <p>Use {@link #success(Object)} for a completed value and
 * {@link #failure(String, String)} for a handler-declared failure; result reporting
 * converts this value to the scheduler wire protocol.</p>
 *
 * @param <R> successful result value type
 */
public record TaskResult<R>(boolean success, R value, String errorCode, String message) {

    public static <R> TaskResult<R> success(R value) {
        return new TaskResult<>(true, value, null, null);
    }

    public static <R> TaskResult<R> failure(String errorCode, String message) {
        return new TaskResult<>(false, null, errorCode, message);
    }
}
