package com.staterelay.server.dispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
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
import java.time.ZoneOffset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class DispatchService {

    private final CapacityReservationService capacityReservations;
    private final WorkerRouter router;
    private final ExecutorHttpClient executorHttpClient;
    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final DispatchRepository dispatches;
    private final TaskAttemptRepository attempts;
    private final ObjectMapper objectMapper;
    private final Duration attemptLease;
    private final Duration transportRetryDelay;

    public DispatchService(
            CapacityReservationService capacityReservations,
            WorkerRouter router,
            ExecutorHttpClient executorHttpClient,
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            Duration attemptLease,
            Duration transportRetryDelay) {
        this.capacityReservations = Objects.requireNonNull(
                capacityReservations, "capacityReservations");
        this.router = Objects.requireNonNull(router, "router");
        this.executorHttpClient = Objects.requireNonNull(executorHttpClient, "executorHttpClient");
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(outbox, "outbox");
        this.objectMapper = JsonMapper.builder().findAndAddModules().build();
        this.dispatches = new DispatchRepository(jdbc, transactions, outbox);
        this.attempts = new TaskAttemptRepository(jdbc, transactions, outbox, objectMapper);
        this.attemptLease = requirePositive(attemptLease, "attemptLease");
        this.transportRetryDelay = requirePositive(transportRetryDelay, "transportRetryDelay");
    }

    /** Routes a durably claimed instance and commits its complete logical assignment. */
    public Optional<DispatchAssignment> assign(
            TaskInstanceRepository.ClaimedTaskInstance claimed) {
        Objects.requireNonNull(claimed, "claimed");
        Optional<String> handler = claimedHandler(claimed);
        if (handler.isEmpty()) {
            return Optional.empty();
        }
        String handlerName = handler.get();
        Instant now = Instant.now();
        List<WorkerRouter.WorkerCandidate> remaining = new ArrayList<>(
                workerCandidates(claimed, handlerName));
        WorkerRouter.Requirements requirements = new WorkerRouter.Requirements(handlerName, now);
        while (!remaining.isEmpty()) {
            Optional<WorkerRouter.WorkerCandidate> chosen = router.choose(remaining, requirements);
            if (chosen.isEmpty()) {
                return Optional.empty();
            }
            WorkerRouter.WorkerCandidate candidate = chosen.get();
            Optional<CapacityReservationService.Reservation> reservation =
                    capacityReservations.reserve(claimed, candidate, handlerName, attemptLease);
            if (reservation.isPresent()) {
                return Optional.of(toAssignment(reservation.get()));
            }
            remaining.remove(candidate);
        }
        return Optional.empty();
    }

    /** Performs one HTTP transmission after first committing the transport-attempt marker. */
    public void deliver(DispatchAssignment assignment) {
        Objects.requireNonNull(assignment, "assignment");
        if (!dispatches.markSending(
                assignment.dispatchId(), assignment.attemptId(),
                assignment.workerId(), assignment.workerEpoch())) {
            return;
        }
        try {
            DispatchAck ack = executorHttpClient.execute(
                    assignment.workerAddress(), assignment.command());
            handleAck(assignment, ack);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            markUncertain(assignment, "executor call interrupted");
        } catch (Exception exception) {
            markUncertain(assignment, exception.toString());
        }
    }

    /** Retransmits due uncertain Dispatch rows while retaining every logical execution ID. */
    public int retryUncertain(Instant now, int batchSize) {
        Objects.requireNonNull(now, "now");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        List<DispatchAssignment> due = jdbc.query("""
                SELECT dispatch.dispatch_id, attempt.id AS attempt_id,
                    attempt.task_instance_id, attempt.attempt_no, attempt.lease_version,
                    dispatch.target_worker_id, dispatch.target_worker_epoch,
                    dispatch.target_address, application.name AS application_name,
                    version.handler_name, instance.payload::text AS payload,
                    COALESCE(instance.business_idempotency_key, instance.id::text) AS idempotency_key,
                    attempt.assigned_at, attempt.lease_expires_at
                FROM sr_dispatch dispatch
                JOIN sr_task_attempt attempt ON attempt.id = dispatch.task_attempt_id
                JOIN sr_task_instance instance ON instance.id = attempt.task_instance_id
                JOIN sr_task_definition_version version
                  ON version.id = instance.definition_version_id
                JOIN sr_task_definition definition ON definition.id = instance.task_definition_id
                JOIN sr_application application ON application.id = definition.application_id
                WHERE dispatch.status = 'UNCERTAIN'
                  AND dispatch.next_transport_at <= :now
                  AND attempt.status = 'ASSIGNED'
                ORDER BY dispatch.next_transport_at, dispatch.id
                LIMIT :batchSize
                """, new MapSqlParameterSource()
                .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("batchSize", batchSize),
                (resultSet, rowNumber) -> {
                    Instant assignedAt = resultSet.getObject(
                            "assigned_at", OffsetDateTime.class).toInstant();
                    Instant leaseExpiresAt = resultSet.getObject(
                            "lease_expires_at", OffsetDateTime.class).toInstant();
                    return assignment(
                            resultSet.getObject("dispatch_id", UUID.class),
                            resultSet.getObject("attempt_id", UUID.class),
                            resultSet.getObject("task_instance_id", UUID.class),
                            resultSet.getInt("attempt_no"),
                            resultSet.getLong("lease_version"),
                            resultSet.getObject("target_worker_id", UUID.class),
                            resultSet.getObject("target_worker_epoch", UUID.class),
                            resultSet.getString("target_address"),
                            resultSet.getString("application_name"),
                            resultSet.getString("handler_name"),
                            read(resultSet.getString("payload")),
                            resultSet.getString("idempotency_key"),
                            Duration.between(assignedAt, leaseExpiresAt), assignedAt);
                });
        due.forEach(this::deliver);
        return due.size();
    }

    private Optional<String> claimedHandler(
            TaskInstanceRepository.ClaimedTaskInstance claimed) {
        List<String> handlers = jdbc.query("""
                SELECT version.handler_name
                FROM sr_task_instance instance
                JOIN sr_task_definition_version version
                  ON version.id = instance.definition_version_id
                WHERE instance.id = :instanceId
                  AND instance.claim_token = :claimToken
                  AND instance.status IN ('READY', 'RETRY_WAIT')
                  AND version.handler_name IS NOT NULL
                """, new MapSqlParameterSource()
                .addValue("instanceId", claimed.instanceId())
                .addValue("claimToken", claimed.claimToken()),
                (resultSet, rowNumber) -> resultSet.getString("handler_name"));
        return handlers.stream().filter(value -> !value.isBlank()).findFirst();
    }

    private List<WorkerRouter.WorkerCandidate> workerCandidates(
            TaskInstanceRepository.ClaimedTaskInstance claimed,
            String handlerName) {
        return jdbc.query("""
                SELECT worker.id, worker.status, worker.lease_expires_at,
                    worker.reserved_capacity, worker.reported_active_count,
                    worker.max_concurrency,
                    EXISTS (
                        SELECT 1 FROM jsonb_array_elements(worker.handlers) handler
                        WHERE handler ->> 'name' = :handlerName) AS compatible
                FROM sr_worker worker
                JOIN sr_task_definition definition
                  ON definition.application_id = worker.application_id
                JOIN sr_task_instance instance
                  ON instance.task_definition_id = definition.id
                WHERE instance.id = :instanceId
                  AND instance.claim_token = :claimToken
                  AND NOT EXISTS (
                      SELECT 1 FROM sr_task_attempt prior
                      WHERE prior.task_instance_id = instance.id
                        AND prior.worker_id = worker.id
                        AND prior.status = 'LOST')
                ORDER BY worker.id
                """, new MapSqlParameterSource()
                .addValue("instanceId", claimed.instanceId())
                .addValue("claimToken", claimed.claimToken())
                .addValue("handlerName", handlerName),
                (resultSet, rowNumber) -> new WorkerRouter.WorkerCandidate(
                        resultSet.getObject("id", UUID.class).toString(),
                        resultSet.getString("status"),
                        resultSet.getObject("lease_expires_at", OffsetDateTime.class).toInstant(),
                        resultSet.getInt("reserved_capacity"),
                        resultSet.getInt("reported_active_count"),
                        resultSet.getInt("max_concurrency"),
                        resultSet.getBoolean("compatible")
                                ? Set.of(handlerName) : Set.of()));
    }

    private DispatchAssignment toAssignment(CapacityReservationService.Reservation reservation) {
        return assignment(
                reservation.dispatchId(), reservation.attemptId(), reservation.taskInstanceId(),
                reservation.attemptNumber(), reservation.leaseVersion(), reservation.workerId(),
                reservation.workerEpoch(), reservation.workerAddress(), reservation.application(),
                reservation.handlerName(), reservation.parameter(), reservation.idempotencyKey(),
                reservation.leaseDuration(), reservation.dispatchedAt());
    }

    private DispatchAssignment assignment(
            UUID dispatchId,
            UUID attemptId,
            UUID taskInstanceId,
            int attemptNo,
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
        ExecuteTaskCommand command = new ExecuteTaskCommand(
                taskInstanceId.toString(), attemptId.toString(), attemptNo,
                dispatchId.toString(), leaseVersion, application, workerId.toString(),
                workerEpoch.toString(), handlerName, parameter, idempotencyKey,
                leaseDuration, dispatchedAt);
        return new DispatchAssignment(
                dispatchId, attemptId, taskInstanceId, attemptNo, leaseVersion,
                workerId, workerEpoch, workerAddress, command);
    }

    private void handleAck(DispatchAssignment assignment, DispatchAck ack) {
        if (!matchesFence(assignment, ack) || ack.status() == null) {
            markUncertain(assignment, "executor ACK did not match the dispatch fence");
            return;
        }
        switch (ack.status()) {
            case ACCEPTED, DUPLICATE -> markAccepted(assignment);
            case REJECTED_CAPACITY, REJECTED_HANDLER, REJECTED_STALE_EPOCH ->
                    markRejected(assignment, ack);
        }
    }

    private boolean matchesFence(DispatchAssignment assignment, DispatchAck ack) {
        return ack != null
                && Objects.equals(assignment.dispatchId().toString(), ack.dispatchId())
                && Objects.equals(assignment.attemptId().toString(), ack.attemptId())
                && Objects.equals(assignment.workerId().toString(), ack.workerId())
                && Objects.equals(assignment.workerEpoch().toString(), ack.workerEpoch());
    }

    private void markAccepted(DispatchAssignment assignment) {
        Boolean accepted = transactions.execute(status -> {
            boolean dispatchAcked = dispatches.markAcked(
                    assignment.dispatchId(), assignment.attemptId(),
                    assignment.workerId(), assignment.workerEpoch());
            boolean attemptAccepted = dispatchAcked && attempts.markAttemptAccepted(
                    assignment.taskInstanceId(), assignment.attemptId(), assignment.leaseVersion(),
                    assignment.workerId(), assignment.workerEpoch());
            if (!attemptAccepted) {
                status.setRollbackOnly();
                return false;
            }
            return true;
        });
        if (!Boolean.TRUE.equals(accepted)) {
            markUncertain(assignment, "acceptance fence changed before persistence");
        }
    }

    private void markRejected(DispatchAssignment assignment, DispatchAck ack) {
        transactions.executeWithoutResult(status -> {
            boolean dispatchExpired = dispatches.markExpired(
                    assignment.dispatchId(), assignment.attemptId(),
                    assignment.workerId(), assignment.workerEpoch(), ack.status().name());
            boolean attemptLost = dispatchExpired && attempts.markAttemptLost(
                    assignment.taskInstanceId(), assignment.attemptId(), assignment.leaseVersion(),
                    assignment.workerId(), assignment.workerEpoch());
            if (!attemptLost) {
                status.setRollbackOnly();
            }
        });
    }

    private void markUncertain(DispatchAssignment assignment, String error) {
        dispatches.markUncertain(
                assignment.dispatchId(), assignment.attemptId(),
                assignment.workerId(), assignment.workerEpoch(),
                Instant.now().plus(transportRetryDelay), error);
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    public record DispatchAssignment(
            UUID dispatchId,
            UUID attemptId,
            UUID taskInstanceId,
            int attemptNo,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch,
            String workerAddress,
            ExecuteTaskCommand command) {
    }
}
