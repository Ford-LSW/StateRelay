package com.staterelay.server.trigger;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class FixedDelayScheduleCoordinator {

    private final NamedParameterJdbcTemplate jdbc;

    public FixedDelayScheduleCoordinator(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Calculates fixed-delay recurrence from actual terminal time.
     */
    public Instant nextAfterCompletion(Instant terminalAt, Duration delay) {
        Objects.requireNonNull(terminalAt, "terminalAt");
        Objects.requireNonNull(delay, "delay");
        if (delay.isZero() || delay.isNegative()) {
            throw new IllegalArgumentException("delay must be positive");
        }
        return terminalAt.plus(delay);
    }

    /**
     * Schedules a fixed-delay trigger from its instance terminal timestamp in the caller's transaction.
     */
    public int scheduleAfterTerminal(UUID instanceId) {
        return jdbc.update("""
                UPDATE sr_trigger trigger
                SET next_fire_at = instance.terminal_at
                        + make_interval(secs => (trigger.schedule_config ->> 'delaySeconds')::double precision),
                    updated_at = clock_timestamp()
                FROM sr_task_instance instance
                WHERE instance.id = :instanceId
                  AND instance.trigger_id = trigger.id
                  AND instance.terminal_at IS NOT NULL
                  AND trigger.trigger_type = 'FIXED_DELAY'
                  AND trigger.status = 'ACTIVE'
                """, new MapSqlParameterSource("instanceId", instanceId));
    }
}
