package com.staterelay.server.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.TaskAttemptStatus;
import com.staterelay.server.domain.TaskInstanceStatus;
import com.staterelay.server.trigger.FixedDelayScheduleCoordinator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class TaskAttemptRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final FixedDelayScheduleCoordinator fixedDelaySchedules;

    public TaskAttemptRepository(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper) {
        this(jdbc, transactions, outbox, objectMapper, new FixedDelayScheduleCoordinator(jdbc));
    }

    public TaskAttemptRepository(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper,
            FixedDelayScheduleCoordinator fixedDelaySchedules) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.fixedDelaySchedules = fixedDelaySchedules;
    }

    /**
     * Consumes the durable claim, reserves one Worker slot, advances the instance lease fence,
     * and inserts its Attempt in one short transaction. A failed claim or capacity guard leaves
     * every write rolled back; PostgreSQL supplies the monotonic lease value copied to the Attempt.
     */
    public Optional<AttemptLease> createAttemptWithCapacityReservation(
            UUID taskInstanceId,
            UUID claimToken,
            UUID workerId,
            UUID workerEpoch,
            Instant leaseExpiresAt) {
        return createAttemptWithCapacityReservation(
                taskInstanceId, claimToken, workerId, workerEpoch, null, null, leaseExpiresAt);
    }

    /**
     * Creates an Attempt for a handler-compatible Worker while retaining the original durable
     * claim and lease fences. The caller may wrap this method in a wider short transaction that
     * also creates the logical Dispatch; Spring's required propagation keeps all writes atomic.
     */
    public Optional<AttemptLease> createAttemptWithCapacityReservation(
            UUID taskInstanceId,
            UUID claimToken,
            UUID workerId,
            UUID workerEpoch,
            String handlerName,
            Instant assignedAt,
            Instant leaseExpiresAt) {
        Objects.requireNonNull(claimToken, "claimToken");
        return Objects.requireNonNull(transactions.execute(status -> {
            List<Long> leases = jdbc.query("""
                    UPDATE sr_task_instance
                    SET current_lease_version = current_lease_version + 1,
                        status = 'RUNNING', claim_token = NULL, claimed_at = NULL,
                        updated_at = clock_timestamp()
                    WHERE id = :instanceId
                      AND status IN ('READY', 'RETRY_WAIT')
                      AND claim_token = :claimToken
                    RETURNING current_lease_version
                    """, new MapSqlParameterSource()
                    .addValue("instanceId", taskInstanceId)
                    .addValue("claimToken", claimToken),
                    (resultSet, rowNumber) -> resultSet.getLong("current_lease_version"));
            if (leases.isEmpty()) {
                return Optional.empty();
            }
            int reserved = jdbc.update("""
                    UPDATE sr_worker
                    SET reserved_capacity = GREATEST(
                            reserved_capacity, reported_active_count) + 1,
                        updated_at = clock_timestamp()
                    WHERE id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status = 'READY'
                      AND lease_expires_at > clock_timestamp()
                      AND GREATEST(reserved_capacity, reported_active_count) < max_concurrency
                      AND (:handlerName IS NULL OR EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(handlers) handler
                          WHERE handler ->> 'name' = :handlerName))
                    """, new MapSqlParameterSource()
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch)
                    .addValue("handlerName", handlerName, Types.VARCHAR));
            if (reserved == 0) {
                status.setRollbackOnly();
                return Optional.empty();
            }
            long leaseVersion = leases.get(0);
            int attemptNumber = jdbc.queryForObject("""
                    SELECT COALESCE(MAX(attempt_no), 0) + 1
                    FROM sr_task_attempt
                    WHERE task_instance_id = :instanceId
                    """, new MapSqlParameterSource("instanceId", taskInstanceId), Integer.class);
            UUID attemptId = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO sr_task_attempt(
                        id, task_instance_id, attempt_no, lease_version, worker_id, worker_epoch,
                        status, lease_expires_at, assigned_at)
                    VALUES (:id, :instanceId, :attemptNo, :leaseVersion, :workerId, :workerEpoch,
                        'ASSIGNED', :leaseExpiresAt,
                        COALESCE(CAST(:assignedAt AS timestamptz), clock_timestamp()))
                    """, new MapSqlParameterSource()
                    .addValue("id", attemptId)
                    .addValue("instanceId", taskInstanceId)
                    .addValue("attemptNo", attemptNumber)
                    .addValue("leaseVersion", leaseVersion)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch)
                    .addValue("assignedAt", assignedAt == null ? null
                            : assignedAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("leaseExpiresAt", leaseExpiresAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_CREATED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("attemptNumber", attemptNumber)
                            .put("leaseVersion", leaseVersion)
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString()));
            outbox.append("TASK_INSTANCE", taskInstanceId, "TASK_INSTANCE_STARTED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("status", TaskInstanceStatus.RUNNING.name()));
            return Optional.of(new AttemptLease(
                    attemptId, taskInstanceId, attemptNumber, leaseVersion, workerId, workerEpoch));
        }));
    }

    /** Marks only the exact assigned execution fence as accepted by its owning Worker epoch. */
    public boolean markAttemptAccepted(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_task_attempt attempt
                    SET status = 'ACCEPTED', accepted_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE attempt.id = :attemptId
                      AND attempt.task_instance_id = :instanceId
                      AND attempt.lease_version = :leaseVersion
                      AND attempt.worker_id = :workerId
                      AND attempt.worker_epoch = :workerEpoch
                      AND attempt.status = 'ASSIGNED'
                      AND EXISTS (
                          SELECT 1 FROM sr_task_instance instance
                          WHERE instance.id = attempt.task_instance_id
                            AND instance.current_lease_version = attempt.lease_version
                            AND instance.status = 'RUNNING')
                    """, fenceParameters(
                    taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch));
            if (updated == 0) {
                return false;
            }
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_ACCEPTED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString()));
            return true;
        }));
    }

    /**
     * Compatibility entry point for callers that only carry the original Attempt fence. The
     * remaining persisted identifiers are read, then revalidated by the fully fenced transaction.
     */
    public boolean casCompleteAttempt(
            UUID attemptId, long leaseVersion, TaskAttemptStatus terminalStatus, JsonNode result) {
        List<AttemptFence> fences = jdbc.query("""
                SELECT task_instance_id, worker_id, worker_epoch
                FROM sr_task_attempt
                WHERE id = :attemptId AND lease_version = :leaseVersion
                """, new MapSqlParameterSource()
                .addValue("attemptId", attemptId)
                .addValue("leaseVersion", leaseVersion),
                (resultSet, rowNumber) -> new AttemptFence(
                        resultSet.getObject("task_instance_id", UUID.class),
                        resultSet.getObject("worker_id", UUID.class),
                        resultSet.getObject("worker_epoch", UUID.class)));
        if (fences.isEmpty()) {
            return false;
        }
        AttemptFence fence = fences.get(0);
        return complete(fence.taskInstanceId(), attemptId, leaseVersion, fence.workerId(),
                fence.workerEpoch(), terminalStatus, defaultInstanceOutcome(terminalStatus), null, result);
    }

    /**
     * Completes only the exact execution fence supplied by the caller. When every protocol
     * identifier is known, all five are compared so an old Worker epoch cannot complete a
     * replacement attempt even if it retained an Attempt identifier.
     */
    public boolean casCompleteAttempt(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch,
            TaskAttemptStatus terminalStatus,
            JsonNode result) {
        return complete(
                taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch, terminalStatus,
                defaultInstanceOutcome(terminalStatus), null, result);
    }

    /**
     * Applies a caller-decided persistence outcome without classifying retry policy here. The
     * Attempt, Instance, Worker capacity, and both Outbox records commit or roll back together.
     */
    public boolean casCompleteAttempt(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch,
            TaskAttemptStatus terminalStatus,
            TaskInstanceStatus instanceOutcome,
            Instant nextRunAt,
            JsonNode result) {
        return complete(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch,
                terminalStatus, instanceOutcome, nextRunAt, result);
    }

    /**
     * Marks an active Attempt lost only when its complete fence still matches, releases its
     * Worker capacity once, and makes the instance retryable in the same transaction.
     */
    public boolean markAttemptLost(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            List<UUID> instances = jdbc.query("""
                    UPDATE sr_task_instance instance
                    SET status = 'RETRY_WAIT', next_run_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE instance.id = :instanceId
                      AND instance.current_lease_version = :leaseVersion
                      AND instance.status = 'RUNNING'
                      AND EXISTS (
                          SELECT 1 FROM sr_task_attempt attempt
                          WHERE attempt.id = :attemptId
                            AND attempt.task_instance_id = instance.id
                            AND attempt.lease_version = :leaseVersion
                            AND attempt.worker_id = :workerId
                            AND attempt.worker_epoch = :workerEpoch
                            AND attempt.status IN ('ASSIGNED', 'ACCEPTED', 'RUNNING')
                            AND attempt.capacity_released_at IS NULL)
                      AND EXISTS (
                          SELECT 1 FROM sr_worker worker
                          WHERE worker.id = :workerId
                            AND worker.worker_epoch = :workerEpoch)
                    RETURNING instance.id
                    """, fenceParameters(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (instances.isEmpty()) {
                return false;
            }
            List<UUID> releasedWorkers = jdbc.query("""
                    UPDATE sr_task_attempt
                    SET status = 'LOST', finished_at = clock_timestamp(),
                        capacity_released_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :attemptId
                      AND task_instance_id = :instanceId
                      AND lease_version = :leaseVersion
                      AND worker_id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status IN ('ASSIGNED', 'ACCEPTED', 'RUNNING')
                      AND capacity_released_at IS NULL
                    RETURNING worker_id
                    """, fenceParameters(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch),
                    (resultSet, rowNumber) -> resultSet.getObject("worker_id", UUID.class));
            if (releasedWorkers.isEmpty()) {
                status.setRollbackOnly();
                throw new IllegalStateException("attempt fence changed while marking instance retryable");
            }
            if (!releaseWorkerCapacity(workerId, workerEpoch)) {
                status.setRollbackOnly();
                throw new IllegalStateException("worker capacity fence changed while marking attempt lost");
            }
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_LOST",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString()));
            outbox.append("TASK_INSTANCE", taskInstanceId, "TASK_INSTANCE_STATE_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("status", TaskInstanceStatus.RETRY_WAIT.name()));
            return true;
        }));
    }

    private boolean complete(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch,
            TaskAttemptStatus terminalStatus,
            TaskInstanceStatus instanceOutcome,
            Instant nextRunAt,
            JsonNode result) {
        requireTerminal(terminalStatus);
        requireOutcome(terminalStatus, instanceOutcome, nextRunAt);
        return Boolean.TRUE.equals(transactions.execute(status -> {
            List<UUID> instances = jdbc.query("""
                    UPDATE sr_task_instance instance
                    SET status = :instanceOutcome,
                        next_run_at = CASE WHEN :instanceOutcome = 'RETRY_WAIT'
                            THEN :nextRunAt ELSE next_run_at END,
                        terminal_at = CASE WHEN :instanceOutcome IN ('SUCCESS', 'FAILED', 'CANCELLED')
                            THEN clock_timestamp() ELSE NULL END,
                        updated_at = clock_timestamp()
                    WHERE instance.id = :instanceId
                      AND instance.current_lease_version = :leaseVersion
                      AND instance.status = 'RUNNING'
                      AND EXISTS (
                          SELECT 1 FROM sr_task_attempt attempt
                          WHERE attempt.id = :attemptId
                            AND attempt.task_instance_id = instance.id
                            AND attempt.lease_version = :leaseVersion
                            AND attempt.worker_id = :workerId
                            AND attempt.worker_epoch = :workerEpoch
                            AND attempt.status IN ('ACCEPTED', 'RUNNING')
                            AND attempt.capacity_released_at IS NULL)
                      AND EXISTS (
                          SELECT 1 FROM sr_worker worker
                          WHERE worker.id = :workerId
                            AND worker.worker_epoch = :workerEpoch)
                    RETURNING instance.id
                    """, new MapSqlParameterSource()
                    .addValue("instanceId", taskInstanceId)
                    .addValue("attemptId", attemptId)
                    .addValue("leaseVersion", leaseVersion)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch)
                    .addValue("instanceOutcome", instanceOutcome.name())
                    .addValue("nextRunAt", nextRunAt == null ? null
                            : nextRunAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (instances.isEmpty()) {
                return false;
            }

            List<UUID> releasedWorkers = jdbc.query("""
                    UPDATE sr_task_attempt
                    SET status = :terminalStatus, result = CAST(:result AS jsonb),
                        progress_percent = CASE WHEN :terminalStatus = 'SUCCESS' THEN 100 ELSE progress_percent END,
                        capacity_released_at = clock_timestamp(),
                        finished_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :attemptId
                      AND task_instance_id = :instanceId
                      AND lease_version = :leaseVersion
                      AND worker_id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status IN ('ACCEPTED', 'RUNNING')
                      AND capacity_released_at IS NULL
                    RETURNING worker_id
                    """, new MapSqlParameterSource()
                    .addValue("instanceId", taskInstanceId)
                    .addValue("attemptId", attemptId)
                    .addValue("leaseVersion", leaseVersion)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch)
                    .addValue("terminalStatus", terminalStatus.name())
                    .addValue("result", write(result)),
                    (resultSet, rowNumber) -> resultSet.getObject("worker_id", UUID.class));
            if (releasedWorkers.isEmpty()) {
                status.setRollbackOnly();
                throw new IllegalStateException("attempt fence changed while completing instance");
            }
            if (!releaseWorkerCapacity(workerId, workerEpoch)) {
                status.setRollbackOnly();
                throw new IllegalStateException("worker capacity fence changed while completing attempt");
            }
            var payload = JsonNodeFactory.instance.objectNode()
                    .put("attemptId", attemptId.toString())
                    .put("leaseVersion", leaseVersion)
                    .put("status", terminalStatus.name());
            if (taskInstanceId != null) {
                payload.put("taskInstanceId", taskInstanceId.toString());
            }
            if (workerId != null) {
                payload.put("workerId", workerId.toString());
            }
            if (workerEpoch != null) {
                payload.put("workerEpoch", workerEpoch.toString());
            }
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_COMPLETED", payload);
            outbox.append("TASK_INSTANCE", taskInstanceId, "TASK_INSTANCE_STATE_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("status", instanceOutcome.name()));
            if (instanceOutcome == TaskInstanceStatus.SUCCESS
                    || instanceOutcome == TaskInstanceStatus.FAILED
                    || instanceOutcome == TaskInstanceStatus.CANCELLED) {
                fixedDelaySchedules.scheduleAfterTerminal(taskInstanceId);
            }
            return true;
        }));
    }

    private TaskInstanceStatus defaultInstanceOutcome(TaskAttemptStatus terminalStatus) {
        return switch (terminalStatus) {
            case SUCCESS -> TaskInstanceStatus.SUCCESS;
            case CANCELLED -> TaskInstanceStatus.CANCELLED;
            case FAILED, TIMED_OUT -> TaskInstanceStatus.FAILED;
            default -> throw new IllegalArgumentException("attempt completion requires a terminal report status");
        };
    }

    private void requireOutcome(
            TaskAttemptStatus attemptStatus, TaskInstanceStatus instanceOutcome, Instant nextRunAt) {
        Objects.requireNonNull(instanceOutcome, "instanceOutcome");
        boolean allowed = switch (attemptStatus) {
            case SUCCESS -> instanceOutcome == TaskInstanceStatus.SUCCESS;
            case CANCELLED -> instanceOutcome == TaskInstanceStatus.CANCELLED;
            case FAILED, TIMED_OUT -> instanceOutcome == TaskInstanceStatus.FAILED
                    || instanceOutcome == TaskInstanceStatus.RETRY_WAIT;
            default -> false;
        };
        if (!allowed) {
            throw new IllegalArgumentException("attempt and instance outcomes are inconsistent");
        }
        if ((instanceOutcome == TaskInstanceStatus.RETRY_WAIT) != (nextRunAt != null)) {
            throw new IllegalArgumentException("nextRunAt is required only for RETRY_WAIT");
        }
    }

    private MapSqlParameterSource fenceParameters(
            UUID taskInstanceId,
            UUID attemptId,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch) {
        return new MapSqlParameterSource()
                .addValue("instanceId", taskInstanceId)
                .addValue("attemptId", attemptId)
                .addValue("leaseVersion", leaseVersion)
                .addValue("workerId", workerId)
                .addValue("workerEpoch", workerEpoch);
    }

    private boolean releaseWorkerCapacity(UUID workerId, UUID workerEpoch) {
        return jdbc.update("""
                UPDATE sr_worker
                SET reserved_capacity = GREATEST(
                        reported_active_count, reserved_capacity - 1),
                    updated_at = clock_timestamp()
                WHERE id = :workerId
                  AND worker_epoch = :workerEpoch
                  AND reserved_capacity > 0
                """, new MapSqlParameterSource()
                .addValue("workerId", workerId)
                .addValue("workerEpoch", workerEpoch)) == 1;
    }

    private void requireTerminal(TaskAttemptStatus status) {
        if (status != TaskAttemptStatus.SUCCESS
                && status != TaskAttemptStatus.FAILED
                && status != TaskAttemptStatus.CANCELLED
                && status != TaskAttemptStatus.TIMED_OUT) {
            throw new IllegalArgumentException("attempt completion requires a terminal report status");
        }
    }

    private String write(JsonNode value) {
        try {
            return objectMapper.writeValueAsString(
                    value == null ? JsonNodeFactory.instance.objectNode() : value);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public record AttemptLease(
            UUID attemptId,
            UUID taskInstanceId,
            int attemptNumber,
            long leaseVersion,
            UUID workerId,
            UUID workerEpoch) {
    }

    private record AttemptFence(UUID taskInstanceId, UUID workerId, UUID workerEpoch) {
    }
}
