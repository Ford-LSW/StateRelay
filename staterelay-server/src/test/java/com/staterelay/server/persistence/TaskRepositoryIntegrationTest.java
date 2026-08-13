package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.DispatchStatus;
import com.staterelay.server.domain.TaskAttemptStatus;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class TaskRepositoryIntegrationTest extends PostgresRepositoryTestSupport {

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    private TaskInstanceRepository instanceRepository;
    private TaskAttemptRepository attemptRepository;
    private DispatchRepository dispatchRepository;

    @BeforeEach
    void createRepositories() {
        OutboxRepository outboxRepository = new OutboxRepository(jdbc, objectMapper);
        instanceRepository = new TaskInstanceRepository(jdbc, transactions, outboxRepository, objectMapper);
        attemptRepository = new TaskAttemptRepository(jdbc, transactions, outboxRepository, objectMapper);
        dispatchRepository = new DispatchRepository(jdbc, transactions, outboxRepository);
    }

    @Test
    void scheduledInstanceIsUniquePerTriggerAndScheduledTime() {
        DefinitionFixture fixture = definitionFixture();
        Instant scheduledAt = Instant.parse("2026-08-13T10:00:00Z");
        var payload = JsonNodeFactory.instance.objectNode().put("orderId", 42);

        UUID first = instanceRepository.createScheduledInstance(
                fixture.triggerId(), scheduledAt, fixture.versionId(), payload);
        UUID second = instanceRepository.createScheduledInstance(
                fixture.triggerId(), scheduledAt, fixture.versionId(), payload);

        assertThat(second).isEqualTo(first);
        assertThat(instanceRepository.countByTriggerAndTime(fixture.triggerId(), scheduledAt)).isOne();
        assertThat(outboxCount("TASK_INSTANCE_CREATED", first)).isOne();
    }

    @Test
    void manualInstanceIsUniqueWhenBusinessIdempotencyKeyIsPresent() {
        DefinitionFixture fixture = definitionFixture();
        Instant requestedAt = Instant.parse("2026-08-13T10:00:00Z");
        var payload = JsonNodeFactory.instance.objectNode().put("orderId", 43);

        UUID first = instanceRepository.createManualInstance(
                fixture.definitionId(), fixture.versionId(), "business-43", requestedAt, payload);
        UUID second = instanceRepository.createManualInstance(
                fixture.definitionId(), fixture.versionId(), "business-43", requestedAt.plusSeconds(1), payload);

        assertThat(second).isEqualTo(first);
        assertThat(instanceRepository.countByBusinessIdempotencyKey(
                fixture.definitionId(), "business-43")).isOne();
    }

    @Test
    void databaseRejectsPayloadLargerThan64KiB() {
        DefinitionFixture fixture = definitionFixture();
        var payload = JsonNodeFactory.instance.objectNode().put("data", "x".repeat(65_537));

        assertThatThrownBy(() -> instanceRepository.createManualInstance(
                fixture.definitionId(), fixture.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), payload))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void staleLeaseCannotCompleteAttempt() {
        AttemptFixture fixture = runningAttempt(7);

        boolean updated = attemptRepository.casCompleteAttempt(
                fixture.attemptId(), 6, TaskAttemptStatus.SUCCESS,
                JsonNodeFactory.instance.objectNode());

        assertThat(updated).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("RUNNING");
        assertThat(outboxCount("TASK_ATTEMPT_COMPLETED", fixture.attemptId())).isZero();
    }

    @Test
    void completionFencesOnInstanceWorkerEpochAndLeaseAndWritesOutboxAtomically() {
        AttemptFixture fixture = runningAttempt(7);

        boolean wrongEpoch = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), 7, fixture.workerId(), UUID.randomUUID(),
                TaskAttemptStatus.SUCCESS, JsonNodeFactory.instance.objectNode().put("ok", true));
        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), 7, fixture.workerId(), fixture.workerEpoch(),
                TaskAttemptStatus.SUCCESS, JsonNodeFactory.instance.objectNode().put("ok", true));

        assertThat(wrongEpoch).isFalse();
        assertThat(completed).isTrue();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("SUCCESS");
        assertThat(outboxCount("TASK_ATTEMPT_COMPLETED", fixture.attemptId())).isOne();
    }

    @Test
    void supersededInstanceLeaseCannotCompleteOrLoseAnOldActiveAttempt() {
        AttemptFixture fixture = runningAttempt(7);
        jdbcTemplate.update(
                "UPDATE sr_task_instance SET current_lease_version = 8 WHERE id = ?",
                fixture.instanceId());

        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), 7, fixture.workerId(), fixture.workerEpoch(),
                TaskAttemptStatus.SUCCESS, JsonNodeFactory.instance.objectNode());
        boolean lost = attemptRepository.markAttemptLost(
                fixture.instanceId(), fixture.attemptId(), 7, fixture.workerId(), fixture.workerEpoch());

        assertThat(completed).isFalse();
        assertThat(lost).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("RUNNING");
        assertThat(outboxCount("TASK_ATTEMPT_COMPLETED", fixture.attemptId())).isZero();
        assertThat(outboxCount("TASK_ATTEMPT_LOST", fixture.attemptId())).isZero();
    }

    @Test
    void capacityReservationAllocatesMonotonicLeaseVersionsPerInstance() {
        DefinitionFixture fixture = definitionFixture();
        WorkerFixture worker = workerFixture(fixture.applicationId(), 4);
        UUID instanceId = instanceRepository.createManualInstance(
                fixture.definitionId(), fixture.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());

        TaskAttemptRepository.AttemptLease first = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();
        attemptRepository.markAttemptLost(
                instanceId, first.attemptId(), first.leaseVersion(), worker.workerId(), worker.workerEpoch());
        jdbcTemplate.update("UPDATE sr_task_instance SET status = 'RETRY_WAIT' WHERE id = ?", instanceId);
        TaskAttemptRepository.AttemptLease second = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:10:00Z")).orElseThrow();

        assertThat(first.attemptNumber()).isEqualTo(1);
        assertThat(first.leaseVersion()).isEqualTo(1);
        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(second.leaseVersion()).isEqualTo(2);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT current_lease_version FROM sr_task_instance WHERE id = ?", Long.class, instanceId))
                .isEqualTo(2L);
    }

    @Test
    void logicalDispatchIsUniquePerAttemptAndCasIsFencedToWorkerEpoch() {
        DefinitionFixture fixture = definitionFixture();
        WorkerFixture worker = workerFixture(fixture.applicationId(), 4);
        UUID instanceId = instanceRepository.createManualInstance(
                fixture.definitionId(), fixture.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        TaskAttemptRepository.AttemptLease attempt = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();

        UUID dispatchId = dispatchRepository.createDispatch(
                attempt.attemptId(), worker.workerId(), worker.workerEpoch(), "http://worker:8080",
                Instant.parse("2026-08-13T10:00:00Z"), Instant.parse("2026-08-13T10:05:00Z"));
        boolean stale = dispatchRepository.casStatus(
                dispatchId, attempt.attemptId(), worker.workerId(), UUID.randomUUID(),
                DispatchStatus.PENDING, DispatchStatus.SENT, null);
        boolean sent = dispatchRepository.casStatus(
                dispatchId, attempt.attemptId(), worker.workerId(), worker.workerEpoch(),
                DispatchStatus.PENDING, DispatchStatus.SENT, null);

        assertThat(stale).isFalse();
        assertThat(sent).isTrue();
        assertThat(outboxCount("DISPATCH_CREATED", dispatchId)).isOne();
        assertThat(outboxCount("DISPATCH_STATUS_CHANGED", dispatchId)).isOne();
        assertThatThrownBy(() -> dispatchRepository.createDispatch(
                attempt.attemptId(), worker.workerId(), worker.workerEpoch(), "http://worker:8080",
                Instant.parse("2026-08-13T10:00:01Z"), Instant.parse("2026-08-13T10:05:00Z")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void databaseRejectsInvalidAttemptNumberProgressLeaseAndOversizedResult() {
        AttemptFixture fixture = runningAttempt(7);

        assertThatThrownBy(() -> insertAttemptInvariantViolation(
                fixture.instanceId(), fixture.workerId(), fixture.workerEpoch(), 0, 8, 0, "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insertAttemptInvariantViolation(
                fixture.instanceId(), fixture.workerId(), fixture.workerEpoch(), 2, 8, 101, "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insertAttemptInvariantViolation(
                fixture.instanceId(), fixture.workerId(), fixture.workerEpoch(), 2, -1, 0, "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);
        String oversizedResult = "{\"data\":\"" + "x".repeat(65_537) + "\"}";
        assertThatThrownBy(() -> insertAttemptInvariantViolation(
                fixture.instanceId(), fixture.workerId(), fixture.workerEpoch(), 2, 8, 0, oversizedResult))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE sr_task_instance SET current_lease_version = -1 WHERE id = ?", fixture.instanceId()))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private AttemptFixture runningAttempt(long leaseVersion) {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), 4);
        UUID instanceId = UUID.randomUUID();
        UUID attemptId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_task_instance(id, task_definition_id, definition_version_id, status,
                    scheduled_at, next_run_at, configuration_snapshot, payload, current_lease_version)
                VALUES (?, ?, ?, 'RUNNING', ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)
                """, instanceId, definition.definitionId(), definition.versionId(),
                Instant.parse("2026-08-13T10:00:00Z").atOffset(ZoneOffset.UTC),
                Instant.parse("2026-08-13T10:00:00Z").atOffset(ZoneOffset.UTC),
                "{}", "{}", leaseVersion);
        jdbcTemplate.update("""
                INSERT INTO sr_task_attempt(id, task_instance_id, attempt_no, lease_version, worker_id,
                    worker_epoch, status, lease_expires_at, accepted_at, started_at)
                VALUES (?, ?, 1, ?, ?, ?, 'RUNNING', ?, ?, ?)
                """, attemptId, instanceId, leaseVersion, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z").atOffset(ZoneOffset.UTC),
                Instant.parse("2026-08-13T10:00:01Z").atOffset(ZoneOffset.UTC),
                Instant.parse("2026-08-13T10:00:02Z").atOffset(ZoneOffset.UTC));
        return new AttemptFixture(instanceId, attemptId, worker.workerId(), worker.workerEpoch());
    }

    private String attemptStatus(UUID attemptId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_attempt WHERE id = ?", String.class, attemptId);
    }

    private void insertAttemptInvariantViolation(
            UUID instanceId,
            UUID workerId,
            UUID workerEpoch,
            int attemptNumber,
            long leaseVersion,
            int progress,
            String result) {
        jdbcTemplate.update("""
                INSERT INTO sr_task_attempt(id, task_instance_id, attempt_no, lease_version,
                    worker_id, worker_epoch, status, progress_percent, result)
                VALUES (?, ?, ?, ?, ?, ?, 'CREATED', ?, CAST(? AS jsonb))
                """, UUID.randomUUID(), instanceId, attemptNumber, leaseVersion,
                workerId, workerEpoch, progress, result);
    }

    private int outboxCount(String eventType, UUID aggregateId) {
        return jdbcTemplate.queryForObject("""
                SELECT count(*) FROM sr_outbox_event
                WHERE event_type = ? AND aggregate_id = ?
                """, Integer.class, eventType, aggregateId);
    }

    private record AttemptFixture(
            UUID instanceId, UUID attemptId, UUID workerId, UUID workerEpoch) {
    }
}
