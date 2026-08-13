package com.staterelay.server.domain;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    void transientFailureUsesExponentialBackoffAndStopsAtMaxAttempts() {
        RetryPolicy policy = new RetryPolicy(3, Duration.ofSeconds(10), Duration.ofMinutes(2), 0.0);
        Instant now = Instant.parse("2026-08-13T10:00:00Z");

        assertThat(policy.decide(1, ErrorClass.TRANSIENT, now).nextRunAt())
                .isEqualTo(now.plusSeconds(10));
        assertThat(policy.decide(3, ErrorClass.TRANSIENT, now).retry()).isFalse();
        assertThat(policy.decide(1, ErrorClass.PERMANENT, now).retry()).isFalse();
    }

    @Test
    void unknownAndCancelledFailuresDoNotRetry() {
        RetryPolicy policy = new RetryPolicy(3, Duration.ofSeconds(10), Duration.ofMinutes(2), 0.0);
        Instant now = Instant.parse("2026-08-13T10:00:00Z");

        assertThat(policy.decide(1, ErrorClass.UNKNOWN, now).retry()).isFalse();
        assertThat(policy.decide(1, ErrorClass.CANCELLED, now).retry()).isFalse();
    }

    @Test
    void exponentialDelayIsCappedAtMaximumDelay() {
        RetryPolicy policy = new RetryPolicy(30, Duration.ofSeconds(10), Duration.ofMinutes(2), 0.0);
        Instant now = Instant.parse("2026-08-13T10:00:00Z");

        assertThat(policy.decide(20, ErrorClass.TRANSIENT, now).nextRunAt())
                .isEqualTo(now.plus(Duration.ofMinutes(2)));
    }
}
