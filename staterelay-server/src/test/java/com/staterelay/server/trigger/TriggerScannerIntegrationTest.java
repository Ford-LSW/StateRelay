package com.staterelay.server.trigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.definition.TaskDefinitionService;
import com.staterelay.server.domain.TaskAttemptStatus;
import com.staterelay.server.domain.TaskInstanceStatus;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.PostgresRepositoryTestSupport;
import com.staterelay.server.persistence.TaskAttemptRepository;
import com.staterelay.server.persistence.TaskInstanceRepository;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class TriggerScannerIntegrationTest extends PostgresRepositoryTestSupport {

    private static final Instant NOW = Instant.parse("2026-08-13T10:12:00Z");

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    private TriggerScanner scanner;
    private UUID definitionId;
    private UUID versionId;

    @BeforeEach
    void createScanner() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO sr_application(id, name) VALUES (?, ?)", applicationId, "scanner-app");
        TaskDefinitionService definitions = new TaskDefinitionService(jdbc, transactions, objectMapper);
        var draft = definitions.createDraft(applicationId, "scheduled", "scheduled",
                JsonNodeFactory.instance.objectNode().put("timeoutSeconds", 300),
                JsonNodeFactory.instance.objectNode());
        definitions.publish(draft.versionId());
        definitionId = draft.definitionId();
        versionId = draft.versionId();
        OutboxRepository outbox = new OutboxRepository(jdbc, objectMapper);
        TaskInstanceRepository instances = new TaskInstanceRepository(jdbc, transactions, outbox, objectMapper);
        scanner = new TriggerScanner(jdbc, transactions, instances,
                new CronScheduleCalculator(), new FixedRateScheduleCalculator(), 20);
    }

    @Test
    void concurrentScannersCreateOneInstanceAndAdvanceOnce() throws Exception {
        Instant planned = Instant.parse("2026-08-13T10:10:00Z");
        UUID triggerId = insertTrigger("FIXED_RATE", "{\"intervalSeconds\":300}",
                null, "FIRE_ONCE", planned);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return scanner.scanDueTriggers(NOW);
            });
            var second = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return scanner.scanDueTriggers(NOW);
            });
            start.countDown();
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(instanceCount(triggerId)).isOne();
        assertThat(instanceScheduledAt(triggerId)).isEqualTo(planned);
        assertThat(nextFireAt(triggerId)).isEqualTo(Instant.parse("2026-08-13T10:15:00Z"));
    }

    @Test
    void fixedRateUsesPreviousPlannedTimeInsteadOfScannerWallClock() {
        Instant planned = Instant.parse("2026-08-13T10:00:00Z");
        assertThat(new FixedRateScheduleCalculator().next(planned, Duration.ofMinutes(5)))
                .isEqualTo(Instant.parse("2026-08-13T10:05:00Z"));
    }

    @Test
    void fixedDelaySchedulesOnlyAfterPreviousInstanceTerminates() {
        Instant completed = Instant.parse("2026-08-13T10:03:20Z");
        assertThat(new FixedDelayScheduleCoordinator(jdbc).nextAfterCompletion(
                completed, Duration.ofMinutes(5)))
                .isEqualTo(Instant.parse("2026-08-13T10:08:20Z"));
    }

    @Test
    void cronUsesPreviousPlannedTimeInConfiguredIanaZoneAcrossDst() {
        Instant previous = Instant.parse("2026-03-07T14:30:00Z");

        assertThat(new CronScheduleCalculator().next(
                previous, "0 30 9 * * *", ZoneId.of("America/New_York")))
                .isEqualTo(Instant.parse("2026-03-08T13:30:00Z"));
    }

    @Test
    void fireOnceCreatesOldestMissedOccurrenceAndAdvancesBeyondNow() {
        UUID triggerId = insertTrigger("FIXED_RATE", "{\"intervalSeconds\":60}",
                null, "FIRE_ONCE", Instant.parse("2026-08-13T10:00:00Z"));

        assertThat(scanner.scanDueTriggers(NOW)).isOne();

        assertThat(instanceCount(triggerId)).isOne();
        assertThat(instanceScheduledAt(triggerId)).isEqualTo(Instant.parse("2026-08-13T10:00:00Z"));
        assertThat(nextFireAt(triggerId)).isEqualTo(Instant.parse("2026-08-13T10:13:00Z"));
    }

    @Test
    void skipCreatesNoMissedInstanceAndOnlyAdvances() {
        UUID triggerId = insertTrigger("FIXED_RATE", "{\"intervalSeconds\":60}",
                null, "SKIP", Instant.parse("2026-08-13T10:00:00Z"));

        assertThat(scanner.scanDueTriggers(NOW)).isZero();

        assertThat(instanceCount(triggerId)).isZero();
        assertThat(nextFireAt(triggerId)).isEqualTo(Instant.parse("2026-08-13T10:13:00Z"));
    }

    @Test
    void skipPolicyStillCreatesAnOccurrenceThatIsDueExactlyNow() {
        UUID triggerId = insertTrigger("FIXED_RATE", "{\"intervalSeconds\":60}",
                null, "SKIP", NOW);

        assertThat(scanner.scanDueTriggers(NOW)).isOne();

        assertThat(instanceCount(triggerId)).isOne();
        assertThat(instanceScheduledAt(triggerId)).isEqualTo(NOW);
        assertThat(nextFireAt(triggerId)).isEqualTo(NOW.plusSeconds(60));
    }

    @Test
    void fixedDelaySkipAdvancesMissedInitialOccurrenceWithoutCreatingAnInstance() {
        UUID triggerId = insertTrigger("FIXED_DELAY", "{\"delaySeconds\":300}",
                null, "SKIP", Instant.parse("2026-08-13T10:00:00Z"));

        assertThat(scanner.scanDueTriggers(NOW)).isZero();

        assertThat(instanceCount(triggerId)).isZero();
        assertThat(nextFireAt(triggerId)).isEqualTo(Instant.parse("2026-08-13T10:15:00Z"));
    }

    @Test
    void oneTimeCreatesOnceAndClearsNextFireTime() {
        UUID triggerId = insertTrigger("ONE_TIME", "{}", null, "FIRE_ONCE", NOW);

        assertThat(scanner.scanDueTriggers(NOW)).isOne();
        assertThat(instanceCount(triggerId)).isOne();
        assertThat(nextFireAt(triggerId)).isNull();
        assertThat(scanner.scanDueTriggers(NOW.plusSeconds(60))).isZero();
    }

    @Test
    void fixedDelayClearsDueTimeThenTerminalCompletionSchedulesFromTerminalTimeAtomically() {
        UUID triggerId = insertTrigger("FIXED_DELAY", "{\"delaySeconds\":300}",
                null, "FIRE_ONCE", NOW);
        scanner.scanDueTriggers(NOW);
        UUID instanceId = jdbcTemplate.queryForObject(
                "SELECT id FROM sr_task_instance WHERE trigger_id = ?", UUID.class, triggerId);
        assertThat(nextFireAt(triggerId)).isNull();

        WorkerFixture worker = workerFixture(applicationId(), 1);
        TaskInstanceRepository instances = instanceRepository();
        UUID claimToken = instances.claimReadyBatch(NOW.plusSeconds(1), 1).get(0).claimToken();
        OutboxRepository outbox = new OutboxRepository(jdbc, objectMapper);
        TaskAttemptRepository attempts = new TaskAttemptRepository(
                jdbc, transactions, outbox, objectMapper, new FixedDelayScheduleCoordinator(jdbc));
        var attempt = attempts.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(), NOW.plusSeconds(60)).orElseThrow();
        jdbcTemplate.update("UPDATE sr_task_attempt SET status = 'RUNNING' WHERE id = ?", attempt.attemptId());

        assertThat(attempts.casCompleteAttempt(instanceId, attempt.attemptId(), attempt.leaseVersion(),
                worker.workerId(), worker.workerEpoch(), TaskAttemptStatus.SUCCESS,
                TaskInstanceStatus.SUCCESS, null, JsonNodeFactory.instance.objectNode())).isTrue();

        Instant terminalAt = jdbcTemplate.queryForObject(
                "SELECT terminal_at FROM sr_task_instance WHERE id = ?",
                java.time.OffsetDateTime.class, instanceId).toInstant();
        assertThat(nextFireAt(triggerId)).isEqualTo(terminalAt.plusSeconds(300));
    }

    private UUID insertTrigger(
            String type, String scheduleConfig, String zone, String misfire, Instant nextFireAt) {
        UUID triggerId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_trigger(id, task_definition_id, definition_version_id, trigger_type,
                    schedule_config, time_zone, misfire_policy, next_fire_at)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?)
                """, triggerId, definitionId, versionId, type, scheduleConfig, zone, misfire,
                nextFireAt.atOffset(ZoneOffset.UTC));
        return triggerId;
    }

    private UUID applicationId() {
        return jdbcTemplate.queryForObject(
                "SELECT application_id FROM sr_task_definition WHERE id = ?", UUID.class, definitionId);
    }

    private TaskInstanceRepository instanceRepository() {
        return new TaskInstanceRepository(jdbc, transactions,
                new OutboxRepository(jdbc, objectMapper), objectMapper);
    }

    private int instanceCount(UUID triggerId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_instance WHERE trigger_id = ?", Integer.class, triggerId);
    }

    private Instant instanceScheduledAt(UUID triggerId) {
        return jdbcTemplate.queryForObject(
                "SELECT scheduled_at FROM sr_task_instance WHERE trigger_id = ?",
                java.time.OffsetDateTime.class, triggerId).toInstant();
    }

    private Instant nextFireAt(UUID triggerId) {
        var times = jdbcTemplate.query(
                "SELECT next_fire_at FROM sr_trigger WHERE id = ?",
                (resultSet, rowNumber) -> resultSet.getObject(1, java.time.OffsetDateTime.class), triggerId);
        return times.get(0) == null ? null : times.get(0).toInstant();
    }
}
