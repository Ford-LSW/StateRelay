package com.staterelay.server.trigger;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public final class FixedRateScheduleCalculator {

    /**
     * Calculates the next occurrence from the previous planned occurrence.
     */
    public Instant next(Instant previousPlannedAt, Duration interval) {
        requirePositive(interval);
        return Objects.requireNonNull(previousPlannedAt, "previousPlannedAt").plus(interval);
    }

    /**
     * Advances arithmetically to the first planned occurrence strictly after the cutoff.
     */
    public Instant firstAfter(Instant previousPlannedAt, Duration interval, Instant cutoff) {
        requirePositive(interval);
        if (previousPlannedAt.isAfter(cutoff)) {
            return previousPlannedAt;
        }
        long elapsedIntervals = Duration.between(previousPlannedAt, cutoff).dividedBy(interval);
        return previousPlannedAt.plus(interval.multipliedBy(Math.addExact(elapsedIntervals, 1)));
    }

    private void requirePositive(Duration interval) {
        Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }
}
