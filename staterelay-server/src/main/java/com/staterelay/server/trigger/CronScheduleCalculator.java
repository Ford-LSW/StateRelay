package com.staterelay.server.trigger;

import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public final class CronScheduleCalculator {

    /**
     * Calculates the next cron occurrence from the previous planned time in its IANA zone.
     */
    public Instant next(Instant previousPlannedAt, String expression, ZoneId zone) {
        CronExpression cron = CronExpression.parse(expression);
        ZonedDateTime next = cron.next(
                Objects.requireNonNull(previousPlannedAt, "previousPlannedAt")
                        .atZone(Objects.requireNonNull(zone, "zone")));
        if (next == null) {
            throw new IllegalArgumentException("cron expression has no future occurrence");
        }
        return next.toInstant();
    }

    /**
     * Finds the first cron occurrence after a scanner cutoff without creating intermediate work.
     */
    public Instant firstAfter(String expression, ZoneId zone, Instant cutoff) {
        CronExpression cron = CronExpression.parse(expression);
        ZonedDateTime next = cron.next(cutoff.atZone(Objects.requireNonNull(zone, "zone")));
        if (next == null) {
            throw new IllegalArgumentException("cron expression has no future occurrence");
        }
        return next.toInstant();
    }
}
