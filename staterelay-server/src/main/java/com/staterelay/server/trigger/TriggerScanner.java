package com.staterelay.server.trigger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.persistence.TaskInstanceRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class TriggerScanner {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final TaskInstanceRepository instances;
    private final CronScheduleCalculator cron;
    private final FixedRateScheduleCalculator fixedRate;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public TriggerScanner(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            TaskInstanceRepository instances,
            CronScheduleCalculator cron,
            FixedRateScheduleCalculator fixedRate,
            int batchSize) {
        this(jdbc, transactions, instances, cron, fixedRate, new ObjectMapper(), batchSize);
    }

    public TriggerScanner(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            TaskInstanceRepository instances,
            CronScheduleCalculator cron,
            FixedRateScheduleCalculator fixedRate,
            ObjectMapper objectMapper,
            int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.instances = instances;
        this.cron = cron;
        this.fixedRate = fixedRate;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
    }

    /**
     * Locks a bounded due-trigger batch, creates at most one occurrence per trigger, and advances
     * every locked trigger in the same short PostgreSQL transaction.
     */
    public int scanDueTriggers(Instant now) {
        Objects.requireNonNull(now, "now");
        Integer created = transactions.execute(status -> {
            List<DueTrigger> due = jdbc.query("""
                    SELECT trigger.id, definition.current_published_version_id AS definition_version_id,
                        trigger.trigger_type, trigger.schedule_config, trigger.time_zone,
                        trigger.misfire_policy, trigger.next_fire_at
                    FROM sr_trigger trigger
                    JOIN sr_task_definition definition ON definition.id = trigger.task_definition_id
                    JOIN sr_task_definition_version version
                      ON version.id = definition.current_published_version_id
                     AND version.status = 'PUBLISHED'
                    WHERE trigger.status = 'ACTIVE'
                      AND trigger.next_fire_at <= :now
                    ORDER BY trigger.next_fire_at, trigger.id
                    FOR UPDATE OF trigger SKIP LOCKED
                    LIMIT :batchSize
                    """, new MapSqlParameterSource()
                    .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("batchSize", batchSize),
                    (resultSet, rowNumber) -> new DueTrigger(
                            resultSet.getObject("id", UUID.class),
                            resultSet.getObject("definition_version_id", UUID.class),
                            resultSet.getString("trigger_type"),
                            resultSet.getString("time_zone"),
                            MisfirePolicy.valueOf(resultSet.getString("misfire_policy")),
                            resultSet.getObject("next_fire_at", java.time.OffsetDateTime.class).toInstant(),
                            resultSet.getString("schedule_config")));

            int count = 0;
            for (DueTrigger trigger : due) {
                JsonNode schedule = readSchedule(trigger.scheduleJson());
                Instant nextFireAt = nextAfterNow(trigger, schedule, now);
                boolean create = trigger.misfirePolicy() == MisfirePolicy.FIRE_ONCE
                        || trigger.plannedAt().equals(now);
                if (create) {
                    instances.createScheduledInstance(trigger.id(), trigger.plannedAt(),
                            trigger.definitionVersionId(), JsonNodeFactory.instance.objectNode());
                    count++;
                }
                advance(trigger, nextFireAt, create);
            }
            return count;
        });
        return Objects.requireNonNull(created);
    }

    private JsonNode readSchedule(String scheduleJson) {
        try {
            return objectMapper.readTree(scheduleJson);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private Instant nextAfterNow(DueTrigger trigger, JsonNode schedule, Instant now) {
        return switch (trigger.type()) {
            case "FIXED_RATE" -> fixedRate.firstAfter(
                    trigger.plannedAt(), seconds(schedule, "intervalSeconds"), now);
            case "CRON" -> {
                ZoneId zone = ZoneId.of(requireText(trigger.timeZone(), "time_zone"));
                String expression = requireText(schedule.path("expression").textValue(),
                        "schedule_config.expression");
                Instant immediateNext = cron.next(trigger.plannedAt(), expression, zone);
                yield immediateNext.isAfter(now)
                        ? immediateNext : cron.firstAfter(expression, zone, now);
            }
            case "FIXED_DELAY" -> trigger.misfirePolicy() == MisfirePolicy.SKIP
                    && trigger.plannedAt().isBefore(now)
                    ? fixedRate.firstAfter(
                            trigger.plannedAt(), seconds(schedule, "delaySeconds"), now) : null;
            case "ONE_TIME" -> null;
            default -> throw new IllegalArgumentException("unsupported trigger type: " + trigger.type());
        };
    }

    private void advance(DueTrigger trigger, Instant nextFireAt, boolean created) {
        jdbc.update("""
                UPDATE sr_trigger
                SET last_scheduled_at = CASE WHEN :created THEN :plannedAt ELSE last_scheduled_at END,
                    next_fire_at = :nextFireAt,
                    updated_at = clock_timestamp()
                WHERE id = :id
                """, new MapSqlParameterSource()
                .addValue("id", trigger.id())
                .addValue("created", created)
                .addValue("plannedAt", trigger.plannedAt().atOffset(ZoneOffset.UTC),
                        Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("nextFireAt", nextFireAt == null ? null
                        : nextFireAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    private Duration seconds(JsonNode schedule, String field) {
        if (!schedule.has(field) || !schedule.path(field).canConvertToLong()) {
            throw new IllegalArgumentException("schedule_config." + field + " must be an integer");
        }
        long value = schedule.path(field).longValue();
        if (value <= 0) {
            throw new IllegalArgumentException("schedule_config." + field + " must be positive");
        }
        return Duration.ofSeconds(value);
    }

    private String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private record DueTrigger(
            UUID id,
            UUID definitionVersionId,
            String type,
            String timeZone,
            MisfirePolicy misfirePolicy,
            Instant plannedAt,
            String scheduleJson) {
    }
}
