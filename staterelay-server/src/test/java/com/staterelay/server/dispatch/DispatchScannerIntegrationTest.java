package com.staterelay.server.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.PostgresRepositoryTestSupport;
import com.staterelay.server.persistence.TaskInstanceRepository;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class DispatchScannerIntegrationTest extends PostgresRepositoryTestSupport {

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void unavailableHandlerWorkerAndCapacityClaimsAreDeferredThenReclaimable() {
        DefinitionFixture withoutWorker = definitionFixture();
        UUID noWorker = insertReadyInstance(
                withoutWorker.definitionId(), withoutWorker.versionId());
        DefinitionFixture withoutHandler = definitionFixture();
        UUID noHandlerVersion = insertVersionWithoutHandler(withoutHandler.definitionId());
        UUID noHandler = insertReadyInstance(withoutHandler.definitionId(), noHandlerVersion);
        insertWorker(withoutHandler.applicationId(), 1, 0);
        DefinitionFixture atCapacity = definitionFixture();
        UUID fullWorker = insertWorker(atCapacity.applicationId(), 1, 1);
        UUID capacityRace = insertReadyInstance(
                atCapacity.definitionId(), atCapacity.versionId());
        Instant scanAt = databaseNow();

        DispatchScanner scanner = scanner(service(new PowerOfTwoChoicesRouter(
                new java.util.Random(3))), Duration.ofSeconds(2), Duration.ofSeconds(30));
        assertThat(scanner.scanReadyInstances(scanAt)).isZero();

        assertThat(taskInstances().claimReadyBatch(scanAt.plusSeconds(1), 10)).isEmpty();
        var reclaimed = taskInstances().claimReadyBatch(scanAt.plusSeconds(2), 10);
        assertThat(reclaimed).extracting(TaskInstanceRepository.ClaimedTaskInstance::instanceId)
                .containsExactlyInAnyOrder(noWorker, noHandler, capacityRace);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, fullWorker)).isZero();
    }

    @Test
    void exceptionDefersCurrentAndUnprocessedBatchClaims() {
        DefinitionFixture definition = definitionFixture();
        insertWorker(definition.applicationId(), 4, 0);
        UUID first = insertReadyInstance(definition.definitionId(), definition.versionId());
        UUID second = insertReadyInstance(definition.definitionId(), definition.versionId());
        Instant scanAt = databaseNow();
        WorkerRouter explodingRouter = (candidates, requirements) -> {
            throw new IllegalStateException("route failed");
        };
        DispatchScanner scanner = scanner(
                service(explodingRouter), Duration.ofSeconds(2), Duration.ofSeconds(30));

        assertThatThrownBy(() -> scanner.scanReadyInstances(scanAt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("route failed");

        var reclaimed = taskInstances().claimReadyBatch(scanAt.plusSeconds(2), 10);
        assertThat(reclaimed).extracting(TaskInstanceRepository.ClaimedTaskInstance::instanceId)
                .containsExactlyInAnyOrder(first, second);
    }

    @Test
    void expiredDurableClaimIsFencedAndRecoveredByLaterScanner() {
        DefinitionFixture definition = definitionFixture();
        UUID instanceId = insertReadyInstance(definition.definitionId(), definition.versionId());
        Instant claimedAt = databaseNow();
        var abandoned = taskInstances().claimReadyBatch(claimedAt, 1).get(0);
        DispatchScanner scanner = scanner(service((candidates, requirements) -> Optional.empty()),
                Duration.ofSeconds(2), Duration.ofSeconds(30));

        assertThat(scanner.scanReadyInstances(claimedAt.plusSeconds(31))).isZero();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT claim_token IS NULL FROM sr_task_instance WHERE id = ?",
                Boolean.class, instanceId)).isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT next_run_at FROM sr_task_instance WHERE id = ?",
                java.time.OffsetDateTime.class, instanceId).toInstant())
                .isEqualTo(claimedAt.plusSeconds(33).truncatedTo(ChronoUnit.MICROS));
        assertThat(taskInstances().deferClaim(
                abandoned, claimedAt.plusSeconds(60))).isFalse();
    }

    @Test
    void committedAssignmentConsumesClaimWithoutDeferredCleanup() {
        DefinitionFixture definition = definitionFixture();
        UUID workerId = insertWorker(definition.applicationId(), 1, 0);
        UUID instanceId = insertReadyInstance(definition.definitionId(), definition.versionId());
        DispatchScanner scanner = scanner(service(new PowerOfTwoChoicesRouter(
                new java.util.Random(3))), Duration.ofSeconds(2), Duration.ofSeconds(30));

        assertThat(scanner.scanReadyInstances(databaseNow())).isOne();

        assertThat(jdbcTemplate.queryForMap("""
                SELECT status, claim_token FROM sr_task_instance WHERE id = ?
                """, instanceId))
                .containsEntry("status", "RUNNING")
                .containsEntry("claim_token", null);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*) FROM sr_outbox_event
                WHERE aggregate_id = ? AND event_type = 'TASK_INSTANCE_CLAIM_DEFERRED'
                """, Integer.class, instanceId)).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, workerId)).isOne();
    }

    private DispatchScanner scanner(
            DispatchService service, Duration claimRetryDelay, Duration claimTimeout) {
        return new DispatchScanner(
                taskInstances(), service, 10, 10, claimRetryDelay, claimTimeout);
    }

    private DispatchService service(WorkerRouter router) {
        OutboxRepository outbox = new OutboxRepository(jdbc, objectMapper);
        return new DispatchService(
                new CapacityReservationService(jdbc, transactions, outbox), router,
                new ExecutorHttpClient(objectMapper), jdbc, transactions, outbox,
                Duration.ofSeconds(30), Duration.ofMillis(10));
    }

    private TaskInstanceRepository taskInstances() {
        return new TaskInstanceRepository(
                jdbc, transactions, new OutboxRepository(jdbc, objectMapper), objectMapper);
    }

    private UUID insertVersionWithoutHandler(UUID definitionId) {
        UUID versionId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_task_definition_version(
                    id, task_definition_id, version_no, status, configuration_snapshot)
                VALUES (?, ?, 2, 'DRAFT', '{}'::jsonb)
                """, versionId, definitionId);
        return versionId;
    }

    private UUID insertReadyInstance(UUID definitionId, UUID versionId) {
        UUID instanceId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_task_instance(
                    id, task_definition_id, definition_version_id, status,
                    scheduled_at, next_run_at, configuration_snapshot, payload)
                VALUES (?, ?, ?, 'READY', clock_timestamp(), clock_timestamp(),
                    '{}'::jsonb, '{}'::jsonb)
                """, instanceId, definitionId, versionId);
        return instanceId;
    }

    private UUID insertWorker(UUID applicationId, int capacity, int reportedActive) {
        UUID workerId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_worker(
                    id, application_id, worker_key, worker_epoch, pod_name, pod_ip,
                    executor_port, status, max_concurrency, reported_active_count,
                    handlers, lease_expires_at)
                VALUES (?, ?, ?, ?, ?, '127.0.0.1', 8080, 'READY', ?, ?,
                    '[{"name":"archiveOrders"}]'::jsonb,
                    clock_timestamp() + interval '10 minutes')
                """, workerId, applicationId, "worker-" + workerId, UUID.randomUUID(),
                "pod-" + workerId, capacity, reportedActive);
        return workerId;
    }

    private Instant databaseNow() {
        return jdbcTemplate.queryForObject(
                "SELECT clock_timestamp()", java.time.OffsetDateTime.class).toInstant();
    }
}
