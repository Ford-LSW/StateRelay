package com.staterelay.server.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.PostgresRepositoryTestSupport;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class WorkerRegistrationIntegrationTest extends PostgresRepositoryTestSupport {

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    private WorkerService service;

    @BeforeEach
    void createService() {
        service = new WorkerService(jdbc, transactions,
                new OutboxRepository(jdbc, objectMapper), objectMapper,
                Duration.ofSeconds(35));
    }

    @Test
    void replacingPodEpochOfflinesOldWorkerAndMakesItsAttemptRecoverable() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "order-service");
        UUID oldWorkerId = UUID.randomUUID();
        UUID oldEpoch = UUID.randomUUID();
        service.register(registration(oldWorkerId, oldEpoch));
        ActiveAttempt attempt = insertActiveAttempt(applicationId, oldWorkerId, oldEpoch);

        UUID newWorkerId = UUID.randomUUID();
        UUID newEpoch = UUID.randomUUID();
        service.register(registration(newWorkerId, newEpoch));

        assertThat(workerStatus(oldWorkerId)).isEqualTo("OFFLINE");
        assertThat(workerStatus(newWorkerId)).isEqualTo("READY");
        assertThat(attemptStatus(attempt.attemptId())).isEqualTo("RUNNING");
        assertThat(instanceStatus(attempt.instanceId())).isEqualTo("RUNNING");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT lease_expires_at <= clock_timestamp() FROM sr_task_attempt WHERE id = ?",
                Boolean.class, attempt.attemptId())).isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, oldWorkerId)).isOne();

        assertThatThrownBy(() -> service.heartbeat(oldWorkerId,
                heartbeat(oldWorkerId, oldEpoch, List.of())))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(exception -> assertThat(
                        ((ResponseStatusException) exception).getStatusCode())
                        .isEqualTo(HttpStatus.CONFLICT));
        assertConflict(() -> service.drain(oldWorkerId,
                new WorkerService.EpochRequest(oldEpoch)));
        assertConflict(() -> service.delete(oldWorkerId,
                new WorkerService.EpochRequest(oldEpoch)));
        assertThat(workerStatus(oldWorkerId)).isEqualTo("OFFLINE");
        assertThat(workerStatus(newWorkerId)).isEqualTo("READY");
    }

    @Test
    void heartbeatUsesServerClockAndPersistsEffectiveLoadMetadata() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "order-service");
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        service.register(registration(workerId, workerEpoch));
        ActiveAttempt attempt = insertActiveAttempt(applicationId, workerId, workerEpoch);
        Instant beforeHeartbeat = databaseNow();

        service.heartbeat(workerId, new WorkerService.HeartbeatRequest(
                "order-service", workerId, workerEpoch, "READY", 3, 7,
                20, 80, Duration.ofSeconds(41), "0.2.0",
                Set.of(new WorkerService.HandlerMetadata(
                "archiveOrders", FirstHandler.class.getName())),
                List.of(new WorkerService.ExecutionLease(
                        attempt.attemptId(), 7, workerEpoch))));

        Instant afterHeartbeat = databaseNow();
        assertThat(workerLeaseExpiry(workerId))
                .isBetween(beforeHeartbeat.plusSeconds(41), afterHeartbeat.plusSeconds(41));
        assertThat(attemptLeaseExpiry(attempt.attemptId()))
                .isBetween(beforeHeartbeat.plusSeconds(41), afterHeartbeat.plusSeconds(41));
        assertThat(jdbcTemplate.queryForMap("""
                SELECT reported_active_count, queue_depth, max_concurrency,
                    queue_capacity, starter_version,
                    GREATEST(reserved_capacity, reported_active_count) AS effective_load
                FROM sr_worker WHERE id = ?
                """, workerId))
                .containsEntry("reported_active_count", 3)
                .containsEntry("queue_depth", 7)
                .containsEntry("max_concurrency", 20)
                .containsEntry("queue_capacity", 80)
                .containsEntry("starter_version", "0.2.0")
                .containsEntry("effective_load", 3);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT handlers->0->>'name' FROM sr_worker WHERE id = ?",
                String.class, workerId)).isEqualTo("archiveOrders");
    }

    @Test
    void heartbeatPreservesUnknownReportedExecutionsThenConvergesAfterTheyExit() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "order-service");
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        service.register(registration(workerId, workerEpoch));
        insertActiveAttempt(applicationId, workerId, workerEpoch);
        jdbcTemplate.update("""
                UPDATE sr_worker
                SET reserved_capacity = 16, reported_active_count = 15
                WHERE id = ?
                """, workerId);

        service.heartbeat(workerId, new WorkerService.HeartbeatRequest(
                "order-service", workerId, workerEpoch, "READY", 15, 0,
                16, 64, Duration.ofSeconds(35), "0.2.0", Set.of(), List.of()));

        assertThat(jdbcTemplate.queryForMap("""
                SELECT reserved_capacity, reported_active_count,
                    GREATEST(reserved_capacity, reported_active_count) AS effective_load
                FROM sr_worker WHERE id = ?
                """, workerId))
                .containsEntry("reserved_capacity", 16)
                .containsEntry("reported_active_count", 15)
                .containsEntry("effective_load", 16);

        service.heartbeat(workerId, new WorkerService.HeartbeatRequest(
                "order-service", workerId, workerEpoch, "READY", 0, 0,
                16, 64, Duration.ofSeconds(35), "0.2.0", Set.of(), List.of()));

        assertThat(jdbcTemplate.queryForMap("""
                SELECT reserved_capacity, reported_active_count,
                    GREATEST(reserved_capacity, reported_active_count) AS effective_load
                FROM sr_worker WHERE id = ?
                """, workerId))
                .containsEntry("reserved_capacity", 1)
                .containsEntry("reported_active_count", 0)
                .containsEntry("effective_load", 1);
    }

    @Test
    void oneStaleActiveLeaseRollsBackEveryRenewalAndWorkerReport() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "order-service");
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        service.register(registration(workerId, workerEpoch));
        ActiveAttempt attempt = insertActiveAttempt(applicationId, workerId, workerEpoch);
        OffsetDateTime originalAttemptExpiry = jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_task_attempt WHERE id = ?",
                OffsetDateTime.class, attempt.attemptId());
        OffsetDateTime originalWorkerExpiry = jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_worker WHERE id = ?",
                OffsetDateTime.class, workerId);

        assertConflict(() -> service.heartbeat(workerId,
                new WorkerService.HeartbeatRequest(
                        "order-service", workerId, workerEpoch, "READY", 9, 5,
                        16, 64, Duration.ofSeconds(35), "0.2.0", Set.of(), List.of(
                        new WorkerService.ExecutionLease(attempt.attemptId(), 7, workerEpoch),
                        new WorkerService.ExecutionLease(UUID.randomUUID(), 99, workerEpoch)))));

        assertThat(jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_task_attempt WHERE id = ?",
                OffsetDateTime.class, attempt.attemptId())).isEqualTo(originalAttemptExpiry);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_worker WHERE id = ?",
                OffsetDateTime.class, workerId)).isEqualTo(originalWorkerExpiry);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reported_active_count FROM sr_worker WHERE id = ?",
                Integer.class, workerId)).isZero();
    }

    @Test
    void requestedLeaseDurationIsBoundedAndCannotProvideAnAbsoluteExpiry() {
        UUID applicationId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "order-service");
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();

        assertThatThrownBy(() -> service.register(new WorkerService.RegistrationRequest(
                "order-service", "default", workerId, workerEpoch,
                "order-service-0", "10.0.0.7", 8080, 16, 64,
                Duration.ofMinutes(6), "0.1.0", Set.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerLease must be between PT10S and PT5M");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_worker WHERE id = ?", Integer.class, workerId))
                .isZero();

        WorkerService.WorkerRegistration registered = service.register(
                new WorkerService.RegistrationRequest(
                        "order-service", "default", workerId, workerEpoch,
                        "order-service-0", "10.0.0.7", 8080, 16, 64,
                        null, "0.1.0", Set.of()));
        Instant databaseNow = databaseNow();
        assertThat(registered.leaseExpiresAt())
                .isBetween(databaseNow.plusSeconds(34), databaseNow.plusSeconds(36));
    }

    private WorkerService.RegistrationRequest registration(UUID workerId, UUID workerEpoch) {
        return new WorkerService.RegistrationRequest(
                "order-service", "default", workerId, workerEpoch,
                "order-service-0", "10.0.0.7", 8080, 16, 64,
                Duration.ofSeconds(35), "0.1.0", Set.of(new WorkerService.HandlerMetadata(
                        "closeExpiredOrders", FirstHandler.class.getName())));
    }

    private WorkerService.HeartbeatRequest heartbeat(
            UUID workerId, UUID workerEpoch, List<WorkerService.ExecutionLease> leases) {
        return new WorkerService.HeartbeatRequest(
                "order-service", workerId, workerEpoch, "READY", 1, 0,
                16, 64, Duration.ofSeconds(35), "0.1.0", Set.of(), leases);
    }

    private ActiveAttempt insertActiveAttempt(
            UUID applicationId, UUID workerId, UUID workerEpoch) {
        UUID definitionId = UUID.randomUUID();
        UUID versionId = UUID.randomUUID();
        UUID instanceId = UUID.randomUUID();
        UUID attemptId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_task_definition(id, application_id, name)
                VALUES (?, ?, ?)
                """, definitionId, applicationId, "definition-" + definitionId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_definition_version(
                    id, task_definition_id, version_no, status, configuration_snapshot)
                VALUES (?, ?, 1, 'PUBLISHED', '{}'::jsonb)
                """, versionId, definitionId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_instance(
                    id, task_definition_id, definition_version_id, status,
                    scheduled_at, next_run_at, configuration_snapshot, payload,
                    current_lease_version)
                VALUES (?, ?, ?, 'RUNNING', clock_timestamp(), clock_timestamp(),
                    '{}'::jsonb, '{}'::jsonb, 7)
                """, instanceId, definitionId, versionId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_attempt(
                    id, task_instance_id, attempt_no, lease_version, worker_id,
                    worker_epoch, status, lease_expires_at, assigned_at)
                VALUES (?, ?, 1, 7, ?, ?, 'RUNNING',
                    clock_timestamp() + interval '10 minutes', clock_timestamp())
                """, attemptId, instanceId, workerId, workerEpoch);
        jdbcTemplate.update(
                "UPDATE sr_worker SET reserved_capacity = 1 WHERE id = ?", workerId);
        return new ActiveAttempt(instanceId, attemptId);
    }

    private String workerStatus(UUID workerId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_worker WHERE id = ?", String.class, workerId);
    }

    private Instant workerLeaseExpiry(UUID workerId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_worker WHERE id = ?",
                OffsetDateTime.class, workerId).toInstant();
    }

    private Instant attemptLeaseExpiry(UUID attemptId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_expires_at FROM sr_task_attempt WHERE id = ?",
                OffsetDateTime.class, attemptId).toInstant();
    }

    private Instant databaseNow() {
        return jdbcTemplate.queryForObject(
                "SELECT clock_timestamp()", OffsetDateTime.class).toInstant();
    }

    private void assertConflict(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(exception -> assertThat(
                        ((ResponseStatusException) exception).getStatusCode())
                        .isEqualTo(HttpStatus.CONFLICT));
    }

    private String attemptStatus(UUID attemptId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_attempt WHERE id = ?", String.class, attemptId);
    }

    private String instanceStatus(UUID instanceId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_instance WHERE id = ?", String.class, instanceId);
    }

    private record ActiveAttempt(UUID instanceId, UUID attemptId) {
    }

    private static final class FirstHandler {
    }
}
