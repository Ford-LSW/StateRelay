package com.staterelay.server.domain;

import java.time.Instant;

public record RetryDecision(boolean retry, Instant nextRunAt) {

    public static RetryDecision noRetry() {
        return new RetryDecision(false, null);
    }

    public static RetryDecision retryAt(Instant nextRunAt) {
        return new RetryDecision(true, nextRunAt);
    }
}
