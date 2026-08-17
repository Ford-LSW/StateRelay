package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.TaskAttemptStatus;
import com.staterelay.server.domain.TaskInstanceStatus;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    void successfulCompletionReleasesCapacityAndCompletesTheInstance() {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), 1);
        UUID instanceId = instanceRepository.createManualInstance(
                definition.definitionId(), definition.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        UUID claimToken = claimToken(instanceId);
        TaskAttemptRepository.AttemptLease attempt = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();
        jdbcTemplate.update("UPDATE sr_task_attempt SET status = 'RUNNING' WHERE id = ?", attempt.attemptId());

        boolean completed = attemptRepository.casCompleteAttempt(
                instanceId, attempt.attemptId(), attempt.leaseVersion(), worker.workerId(), worker.workerEpoch(),
                TaskAttemptStatus.SUCCESS, JsonNodeFactory.instance.objectNode().put("ok", true));

        assertThat(completed).isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?", Integer.class, worker.workerId()))
                .isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_instance WHERE id = ?", String.class, instanceId))
                .isEqualTo("SUCCESS");
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
        UUID firstClaimToken = claimToken(instanceId);

        TaskAttemptRepository.AttemptLease first = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, firstClaimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();
        attemptRepository.markAttemptLost(
                instanceId, first.attemptId(), first.leaseVersion(), worker.workerId(), worker.workerEpoch());
        UUID secondClaimToken = claimToken(instanceId);
        TaskAttemptRepository.AttemptLease second = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, secondClaimToken, worker.workerId(), worker.workerEpoch(),
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
        UUID claimToken = claimToken(instanceId);
        TaskAttemptRepository.AttemptLease attempt = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();

        UUID dispatchId = dispatchRepository.createDispatch(
                attempt.attemptId(), worker.workerId(), worker.workerEpoch(), "http://worker:8080",
                Instant.parse("2026-08-13T10:00:00Z"), Instant.parse("2026-08-13T10:05:00Z"));
        Instant sendAt = Instant.parse("2026-08-13T10:00:01Z");
        boolean stale = dispatchRepository.claimSending(
                dispatchId, attempt.attemptId(), worker.workerId(), UUID.randomUUID(),
                sendAt, sendAt.plusSeconds(5)).isPresent();
        boolean sent = dispatchRepository.claimSending(
                dispatchId, attempt.attemptId(), worker.workerId(), worker.workerEpoch(),
                sendAt, sendAt.plusSeconds(5)).isPresent();

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
    void attemptCreationRequiresAndConsumesTheDurableClaimToken() {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), 2);
        UUID instanceId = instanceRepository.createManualInstance(
                definition.definitionId(), definition.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        UUID claimToken = claimToken(instanceId);
        int outboxBefore = totalOutboxCount();

        assertThat(attemptRepository.createAttemptWithCapacityReservation(
                instanceId, UUID.randomUUID(), worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z"))).isEmpty();
        assertThatThrownBy(() -> attemptRepository.createAttemptWithCapacityReservation(
                instanceId, null, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")))
                .isInstanceOf(NullPointerException.class);
        assertReadyClaimState(instanceId, claimToken, worker.workerId(), 0, 0, outboxBefore);

        TaskAttemptRepository.AttemptLease attempt = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();

        assertThat(attempt.leaseVersion()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT claim_token IS NULL FROM sr_task_instance WHERE id = ?", Boolean.class, instanceId))
                .isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?", Integer.class, worker.workerId()))
                .isOne();
        assertThat(outboxCount("TASK_ATTEMPT_CREATED", attempt.attemptId())).isOne();
        assertThat(outboxCount("TASK_INSTANCE_STARTED", instanceId)).isOne();
        assertThat(attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z"))).isEmpty();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?", Integer.class, worker.workerId()))
                .isOne();
    }

    @Test
    void concurrentConsumersCannotSpendTheSameClaimTokenTwice() throws Exception {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), 2);
        UUID instanceId = instanceRepository.createManualInstance(
                definition.definitionId(), definition.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        UUID claimToken = claimToken(instanceId);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return attemptRepository.createAttemptWithCapacityReservation(
                        instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                        Instant.parse("2026-08-13T10:05:00Z"));
            });
            var second = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return attemptRepository.createAttemptWithCapacityReservation(
                        instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                        Instant.parse("2026-08-13T10:05:00Z"));
            });
            start.countDown();

            assertThat(java.util.stream.Stream.of(
                            first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS))
                    .filter(java.util.Optional::isPresent)).hasSize(1);
        } finally {
            executor.shutdownNow();
        }
        assertThat(reservedCapacity(worker.workerId())).isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_attempt WHERE task_instance_id = ?", Integer.class, instanceId))
                .isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_outbox_event WHERE event_type = 'TASK_ATTEMPT_CREATED'",
                Integer.class)).isOne();
    }

    @Test
    void unavailableCapacityDoesNotConsumeClaimOrAdvanceLeaseOrWriteOutbox() {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), 1);
        jdbcTemplate.update("UPDATE sr_worker SET reserved_capacity = 1 WHERE id = ?", worker.workerId());
        UUID instanceId = instanceRepository.createManualInstance(
                definition.definitionId(), definition.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        UUID claimToken = claimToken(instanceId);
        int outboxBefore = totalOutboxCount();

        assertThat(attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z"))).isEmpty();

        assertReadyClaimState(instanceId, claimToken, worker.workerId(), 0, 1, outboxBefore);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT current_lease_version FROM sr_task_instance WHERE id = ?", Long.class, instanceId))
                .isZero();
    }

    @ParameterizedTest
    @CsvSource({
            "SUCCESS, SUCCESS",
            "FAILED, FAILED",
            "CANCELLED, CANCELLED",
            "TIMED_OUT, FAILED"
    })
    void terminalCompletionMapsOutcomeReleasesCapacityExactlyOnceAndWritesBothOutboxEvents(
            TaskAttemptStatus attemptOutcome, TaskInstanceStatus instanceOutcome) {
        ClaimedAttempt fixture = claimedRunningAttempt(1);

        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch(), attemptOutcome, instanceOutcome, null,
                JsonNodeFactory.instance.objectNode());
        boolean duplicate = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch(), attemptOutcome, instanceOutcome, null,
                JsonNodeFactory.instance.objectNode());

        assertThat(completed).isTrue();
        assertThat(duplicate).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo(attemptOutcome.name());
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo(instanceOutcome.name());
        assertThat(reservedCapacity(fixture.workerId())).isZero();
        assertThat(capacityReleased(fixture.attemptId())).isTrue();
        assertThat(outboxCount("TASK_ATTEMPT_COMPLETED", fixture.attemptId())).isOne();
        assertThat(outboxCount("TASK_INSTANCE_STATE_CHANGED", fixture.instanceId())).isOne();
    }

    @Test
    void retryableFailureReturnsInstanceToRetryWaitAtTheSuppliedTime() {
        ClaimedAttempt fixture = claimedRunningAttempt(2);
        Instant nextRunAt = Instant.parse("2026-08-13T10:30:00Z");

        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch(), TaskAttemptStatus.FAILED,
                TaskInstanceStatus.RETRY_WAIT, nextRunAt, JsonNodeFactory.instance.objectNode());

        assertThat(completed).isTrue();
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo("RETRY_WAIT");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT next_run_at FROM sr_task_instance WHERE id = ?", java.time.OffsetDateTime.class,
                fixture.instanceId()).toInstant()).isEqualTo(nextRunAt);
        assertThat(reservedCapacity(fixture.workerId())).isZero();
    }

    @Test
    void failedCompletionFenceLeavesAttemptInstanceWorkerAndOutboxUnchanged() {
        ClaimedAttempt fixture = claimedRunningAttempt(1);
        int outboxBefore = totalOutboxCount();

        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), UUID.randomUUID(), TaskAttemptStatus.SUCCESS,
                TaskInstanceStatus.SUCCESS, null, JsonNodeFactory.instance.objectNode());

        assertThat(completed).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("RUNNING");
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo("RUNNING");
        assertThat(reservedCapacity(fixture.workerId())).isOne();
        assertThat(capacityReleased(fixture.attemptId())).isFalse();
        assertThat(totalOutboxCount()).isEqualTo(outboxBefore);
    }

    @Test
    void rotatedWorkerEpochInvalidatesCompletionWithoutAnySideEffect() {
        ClaimedAttempt fixture = claimedRunningAttempt(1);
        int outboxBefore = totalOutboxCount();
        jdbcTemplate.update(
                "UPDATE sr_worker SET worker_epoch = ? WHERE id = ?", UUID.randomUUID(), fixture.workerId());

        boolean completed = attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch(), TaskAttemptStatus.SUCCESS,
                TaskInstanceStatus.SUCCESS, null, JsonNodeFactory.instance.objectNode());

        assertThat(completed).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("RUNNING");
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo("RUNNING");
        assertThat(reservedCapacity(fixture.workerId())).isOne();
        assertThat(totalOutboxCount()).isEqualTo(outboxBefore);
    }

    @Test
    void completionRollsBackEveryMutationWhenResultViolatesDatabaseConstraint() {
        ClaimedAttempt fixture = claimedRunningAttempt(1);
        int outboxBefore = totalOutboxCount();
        String oversized = "x".repeat(65_537);

        assertThatThrownBy(() -> attemptRepository.casCompleteAttempt(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch(), TaskAttemptStatus.SUCCESS,
                TaskInstanceStatus.SUCCESS, null,
                JsonNodeFactory.instance.objectNode().put("result", oversized)))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("RUNNING");
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo("RUNNING");
        assertThat(reservedCapacity(fixture.workerId())).isOne();
        assertThat(capacityReleased(fixture.attemptId())).isFalse();
        assertThat(totalOutboxCount()).isEqualTo(outboxBefore);
    }

    @Test
    void lostAttemptReleasesCapacityExactlyOnceAndKeepsRetryTransitionAtomic() {
        ClaimedAttempt fixture = claimedRunningAttempt(1);

        boolean lost = attemptRepository.markAttemptLost(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch());
        boolean duplicate = attemptRepository.markAttemptLost(
                fixture.instanceId(), fixture.attemptId(), fixture.leaseVersion(),
                fixture.workerId(), fixture.workerEpoch());

        assertThat(lost).isTrue();
        assertThat(duplicate).isFalse();
        assertThat(attemptStatus(fixture.attemptId())).isEqualTo("LOST");
        assertThat(instanceStatus(fixture.instanceId())).isEqualTo("RETRY_WAIT");
        assertThat(reservedCapacity(fixture.workerId())).isZero();
        assertThat(capacityReleased(fixture.attemptId())).isTrue();
        assertThat(outboxCount("TASK_ATTEMPT_LOST", fixture.attemptId())).isOne();
        assertThat(outboxCount("TASK_INSTANCE_STATE_CHANGED", fixture.instanceId())).isOne();
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
        jdbcTemplate.update(
                "UPDATE sr_worker SET reserved_capacity = 1 WHERE id = ?", worker.workerId());
        return new AttemptFixture(instanceId, attemptId, worker.workerId(), worker.workerEpoch());
    }

    private String attemptStatus(UUID attemptId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_attempt WHERE id = ?", String.class, attemptId);
    }

    private ClaimedAttempt claimedRunningAttempt(int workerCapacity) {
        DefinitionFixture definition = definitionFixture();
        WorkerFixture worker = workerFixture(definition.applicationId(), workerCapacity);
        UUID instanceId = instanceRepository.createManualInstance(
                definition.definitionId(), definition.versionId(), null,
                Instant.parse("2026-08-13T10:00:00Z"), JsonNodeFactory.instance.objectNode());
        UUID claimToken = claimToken(instanceId);
        TaskAttemptRepository.AttemptLease attempt = attemptRepository.createAttemptWithCapacityReservation(
                instanceId, claimToken, worker.workerId(), worker.workerEpoch(),
                Instant.parse("2026-08-13T10:05:00Z")).orElseThrow();
        jdbcTemplate.update("UPDATE sr_task_attempt SET status = 'RUNNING' WHERE id = ?", attempt.attemptId());
        return new ClaimedAttempt(instanceId, attempt.attemptId(), attempt.leaseVersion(),
                worker.workerId(), worker.workerEpoch());
    }

    private UUID claimToken(UUID instanceId) {
        return instanceRepository.claimReadyBatch(Instant.now().plusSeconds(3_600), 10).stream()
                .filter(claim -> claim.instanceId().equals(instanceId))
                .findFirst()
                .orElseThrow()
                .claimToken();
    }

    private void assertReadyClaimState(
            UUID instanceId, UUID claimToken, UUID workerId, int attempts, int capacity, int outboxCount) {
        assertThat(instanceStatus(instanceId)).isEqualTo("READY");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT claim_token FROM sr_task_instance WHERE id = ?", UUID.class, instanceId))
                .isEqualTo(claimToken);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_attempt WHERE task_instance_id = ?", Integer.class, instanceId))
                .isEqualTo(attempts);
        assertThat(reservedCapacity(workerId)).isEqualTo(capacity);
        assertThat(totalOutboxCount()).isEqualTo(outboxCount);
    }

    private String instanceStatus(UUID instanceId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_instance WHERE id = ?", String.class, instanceId);
    }

    private int reservedCapacity(UUID workerId) {
        return jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?", Integer.class, workerId);
    }

    private boolean capacityReleased(UUID attemptId) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT capacity_released_at IS NOT NULL FROM sr_task_attempt WHERE id = ?",
                Boolean.class, attemptId));
    }

    private int totalOutboxCount() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM sr_outbox_event", Integer.class);
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

    private record ClaimedAttempt(
            UUID instanceId, UUID attemptId, long leaseVersion, UUID workerId, UUID workerEpoch) {
    }
}
