package com.staterelay.server.dispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.staterelay.server.persistence.DispatchRepository;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.TaskAttemptRepository;
import com.staterelay.server.persistence.TaskInstanceRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class CapacityReservationService {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final TaskAttemptRepository attempts;
    private final DispatchRepository dispatches;
    private final ObjectMapper objectMapper;

    public CapacityReservationService(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(outbox, "outbox");
        this.objectMapper = JsonMapper.builder().findAndAddModules().build();
        this.attempts = new TaskAttemptRepository(jdbc, transactions, outbox, objectMapper);
        this.dispatches = new DispatchRepository(jdbc, transactions, outbox);
    }

    /**
     * Locks and revalidates one selected Worker, then atomically reserves its capacity and creates
     * the fenced Attempt, stable logical Dispatch, RUNNING instance state, and their Outbox rows.
     */
    public Optional<Reservation> reserve(
            TaskInstanceRepository.ClaimedTaskInstance claimed,
            WorkerRouter.WorkerCandidate selected,
            String handlerName,
            Duration attemptLease) {
        Objects.requireNonNull(claimed, "claimed");
        Objects.requireNonNull(selected, "selected");
        Objects.requireNonNull(handlerName, "handlerName");
        Objects.requireNonNull(attemptLease, "attemptLease");
        if (attemptLease.isZero() || attemptLease.isNegative()) {
            throw new IllegalArgumentException("attemptLease must be positive");
        }
        UUID workerId = UUID.fromString(selected.workerId());
        return Objects.requireNonNull(transactions.execute(status -> {
            if (!lockClaimedInstance(claimed)) {
                return Optional.empty();
            }
            Optional<LockedAssignmentContext> locked = lockEligibleWorker(
                    claimed, workerId, handlerName);
            if (locked.isEmpty()) {
                return Optional.empty();
            }
            LockedAssignmentContext context = locked.get();
            Instant leaseExpiresAt = context.assignedAt().plus(attemptLease);
            Optional<TaskAttemptRepository.AttemptLease> attempt =
                    attempts.createAttemptWithCapacityReservation(
                            claimed.instanceId(), claimed.claimToken(), workerId,
                            context.workerEpoch(), handlerName, context.assignedAt(), leaseExpiresAt);
            if (attempt.isEmpty()) {
                return Optional.empty();
            }
            TaskAttemptRepository.AttemptLease lease = attempt.get();
            UUID dispatchId = dispatches.createDispatch(
                    lease.attemptId(), workerId, context.workerEpoch(), context.workerAddress(),
                    context.assignedAt(), leaseExpiresAt);
            return Optional.of(new Reservation(
                    dispatchId, lease.attemptId(), claimed.instanceId(), lease.attemptNumber(),
                    lease.leaseVersion(), workerId, context.workerEpoch(), context.workerAddress(),
                    context.application(), handlerName, context.parameter(),
                    context.idempotencyKey(), attemptLease, context.assignedAt()));
        }));
    }

    private boolean lockClaimedInstance(
            TaskInstanceRepository.ClaimedTaskInstance claimed) {
        return !jdbc.query("""
                SELECT id
                FROM sr_task_instance
                WHERE id = :instanceId
                  AND claim_token = :claimToken
                  AND status IN ('READY', 'RETRY_WAIT')
                FOR UPDATE
                """, new MapSqlParameterSource()
                .addValue("instanceId", claimed.instanceId())
                .addValue("claimToken", claimed.claimToken()),
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class)).isEmpty();
    }

    private Optional<LockedAssignmentContext> lockEligibleWorker(
            TaskInstanceRepository.ClaimedTaskInstance claimed,
            UUID workerId,
            String handlerName) {
        List<LockedAssignmentContext> contexts = jdbc.query("""
                SELECT worker.worker_epoch, host(worker.pod_ip) AS pod_ip, worker.executor_port,
                    application.name AS application_name, instance.payload::text AS payload,
                    COALESCE(instance.business_idempotency_key, instance.id::text) AS idempotency_key,
                    clock_timestamp() AS assigned_at
                FROM sr_task_instance instance
                JOIN sr_task_definition_version version
                  ON version.id = instance.definition_version_id
                JOIN sr_task_definition definition ON definition.id = instance.task_definition_id
                JOIN sr_application application ON application.id = definition.application_id
                JOIN sr_worker worker ON worker.application_id = application.id
                WHERE instance.id = :instanceId
                  AND instance.claim_token = :claimToken
                  AND instance.status IN ('READY', 'RETRY_WAIT')
                  AND version.handler_name = :handlerName
                  AND worker.id = :workerId
                  AND worker.status = 'READY'
                  AND worker.lease_expires_at > clock_timestamp()
                  AND GREATEST(worker.reserved_capacity, worker.reported_active_count)
                      < worker.max_concurrency
                  AND EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(worker.handlers) handler
                      WHERE handler ->> 'name' = :handlerName)
                  AND NOT EXISTS (
                      SELECT 1 FROM sr_task_attempt prior
                      WHERE prior.task_instance_id = instance.id
                        AND prior.worker_id = worker.id
                        AND prior.status = 'LOST')
                FOR UPDATE OF worker
                """, new MapSqlParameterSource()
                .addValue("instanceId", claimed.instanceId())
                .addValue("claimToken", claimed.claimToken())
                .addValue("workerId", workerId)
                .addValue("handlerName", handlerName),
                (resultSet, rowNumber) -> {
                    String host = resultSet.getString("pod_ip");
                    if (host.contains(":")) {
                        host = "[" + host + "]";
                    }
                    return new LockedAssignmentContext(
                            resultSet.getObject("worker_epoch", UUID.class),
                            "http://" + host + ":" + resultSet.getInt("executor_port"),
                            resultSet.getString("application_name"),
                            read(resultSet.getString("payload")),
                            resultSet.getString("idempotency_key"),
                            resultSet.getObject("assigned_at", OffsetDateTime.class).toInstant());
                });
        return contexts.stream().findFirst();
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public record Reservation(
            UUID dispatchId,
            UUID attemptId,
            UUID taskInstanceId,
            int attemptNumber,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch,
            String workerAddress,
            String application,
            String handlerName,
            JsonNode parameter,
            String idempotencyKey,
            Duration leaseDuration,
            Instant dispatchedAt) {
    }

    private record LockedAssignmentContext(
            UUID workerEpoch,
            String workerAddress,
            String application,
            JsonNode parameter,
            String idempotencyKey,
            Instant assignedAt) {
    }
}
