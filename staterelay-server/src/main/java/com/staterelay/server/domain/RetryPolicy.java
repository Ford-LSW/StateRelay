package com.staterelay.server.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public final class RetryPolicy {

    private final int maxAttempts;
    private final Duration baseDelay;
    private final Duration maxDelay;
    private final double jitterRatio;
    private final DoubleSupplier jitterSource;

    public RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, double jitterRatio) {
        this(maxAttempts, baseDelay, maxDelay, jitterRatio, ThreadLocalRandom.current()::nextDouble);
    }

    public RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, double jitterRatio,
                       DoubleSupplier jitterSource) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least one");
        }
        if (baseDelay.isNegative() || baseDelay.isZero()) {
            throw new IllegalArgumentException("baseDelay must be positive");
        }
        if (maxDelay.isNegative() || maxDelay.isZero() || maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be positive and at least baseDelay");
        }
        if (jitterRatio < 0.0 || jitterRatio > 1.0) {
            throw new IllegalArgumentException("jitterRatio must be between zero and one");
        }
        this.maxAttempts = maxAttempts;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.jitterRatio = jitterRatio;
        this.jitterSource = Objects.requireNonNull(jitterSource, "jitterSource");
    }

    /**
     * Returns a retry time only for a transient failure before the configured attempt limit.
     */
    public RetryDecision decide(int attemptNo, ErrorClass errorClass, Instant now) {
        if (attemptNo < 1) {
            throw new IllegalArgumentException("attemptNo must be at least one");
        }
        Objects.requireNonNull(errorClass, "errorClass");
        Objects.requireNonNull(now, "now");
        if (errorClass != ErrorClass.TRANSIENT || attemptNo >= maxAttempts) {
            return RetryDecision.noRetry();
        }
        return RetryDecision.retryAt(now.plus(applyJitter(backoff(attemptNo))));
    }

    private Duration backoff(int attemptNo) {
        Duration delay = baseDelay.multipliedBy(1L << Math.min(attemptNo - 1, 20));
        return delay.compareTo(maxDelay) > 0 ? maxDelay : delay;
    }

    private Duration applyJitter(Duration delay) {
        if (jitterRatio == 0.0) {
            return delay;
        }
        double sample = Math.max(0.0, Math.min(1.0, jitterSource.getAsDouble()));
        double multiplier = 1.0 + ((sample * 2.0 - 1.0) * jitterRatio);
        return Duration.ofNanos(Math.round(delay.toNanos() * multiplier));
    }
}
