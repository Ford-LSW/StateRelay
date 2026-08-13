package com.staterelay.server.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.TaskAttemptStatus;
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

    public TaskAttemptRepository(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Reserves one Worker slot, advances the instance lease fence, and inserts its Attempt in
     * one short transaction. A failed capacity guard leaves every write rolled back; a lease
     * value returned by PostgreSQL is copied unchanged and is never synthesized or reused.
     */
    public Optional<AttemptLease> createAttemptWithCapacityReservation(
            UUID taskInstanceId,
            UUID workerId,
            UUID workerEpoch,
            Instant leaseExpiresAt) {
        return Objects.requireNonNull(transactions.execute(status -> {
            int reserved = jdbc.update("""
                    UPDATE sr_worker
                    SET reserved_capacity = reserved_capacity + 1, updated_at = clock_timestamp()
                    WHERE id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status = 'READY'
                      AND lease_expires_at > clock_timestamp()
                      AND GREATEST(reserved_capacity, reported_active_count) < max_concurrency
                    """, new MapSqlParameterSource()
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch));
            if (reserved == 0) {
                return Optional.empty();
            }

            List<Long> leases = jdbc.query("""
                    UPDATE sr_task_instance
                    SET current_lease_version = current_lease_version + 1,
                        status = 'RUNNING', claim_token = NULL, claimed_at = NULL,
                        updated_at = clock_timestamp()
                    WHERE id = :instanceId
                      AND status IN ('READY', 'RETRY_WAIT')
                    RETURNING current_lease_version
                    """, new MapSqlParameterSource("instanceId", taskInstanceId),
                    (resultSet, rowNumber) -> resultSet.getLong("current_lease_version"));
            if (leases.isEmpty()) {
                status.setRollbackOnly();
                throw new IllegalStateException("task instance is not eligible for an attempt");
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
                        'ASSIGNED', :leaseExpiresAt, clock_timestamp())
                    """, new MapSqlParameterSource()
                    .addValue("id", attemptId)
                    .addValue("instanceId", taskInstanceId)
                    .addValue("attemptNo", attemptNumber)
                    .addValue("leaseVersion", leaseVersion)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch)
                    .addValue("leaseExpiresAt", leaseExpiresAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_CREATED",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("attemptNumber", attemptNumber)
                            .put("leaseVersion", leaseVersion)
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString()));
            return Optional.of(new AttemptLease(
                    attemptId, taskInstanceId, attemptNumber, leaseVersion, workerId, workerEpoch));
        }));
    }

    public boolean casCompleteAttempt(
            UUID attemptId, long leaseVersion, TaskAttemptStatus terminalStatus, JsonNode result) {
        return complete(null, attemptId, leaseVersion, null, null, terminalStatus, result);
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
                taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch, terminalStatus, result);
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
                      AND EXISTS (
                          SELECT 1 FROM sr_task_instance instance
                          WHERE instance.id = :instanceId
                            AND instance.current_lease_version = :leaseVersion
                            AND instance.status = 'RUNNING')
                    RETURNING worker_id
                    """, fenceParameters(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch),
                    (resultSet, rowNumber) -> resultSet.getObject("worker_id", UUID.class));
            if (releasedWorkers.isEmpty()) {
                return false;
            }
            jdbc.update("""
                    UPDATE sr_worker
                    SET reserved_capacity = GREATEST(reserved_capacity - 1, 0),
                        updated_at = clock_timestamp()
                    WHERE id = :workerId AND worker_epoch = :workerEpoch
                    """, new MapSqlParameterSource()
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch));
            jdbc.update("""
                    UPDATE sr_task_instance
                    SET status = 'RETRY_WAIT', next_run_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :instanceId
                      AND current_lease_version = :leaseVersion
                      AND status = 'RUNNING'
                    """, new MapSqlParameterSource()
                    .addValue("instanceId", taskInstanceId)
                    .addValue("leaseVersion", leaseVersion));
            outbox.append("TASK_ATTEMPT", attemptId, "TASK_ATTEMPT_LOST",
                    JsonNodeFactory.instance.objectNode()
                            .put("taskInstanceId", taskInstanceId.toString())
                            .put("attemptId", attemptId.toString())
                            .put("leaseVersion", leaseVersion)
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString()));
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
            JsonNode result) {
        requireTerminal(terminalStatus);
        return Boolean.TRUE.equals(transactions.execute(status -> {
            StringBuilder sql = new StringBuilder("""
                    UPDATE sr_task_attempt
                    SET status = :terminalStatus, result = CAST(:result AS jsonb),
                        progress_percent = CASE WHEN :terminalStatus = 'SUCCESS' THEN 100 ELSE progress_percent END,
                        finished_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :attemptId
                      AND lease_version = :leaseVersion
                      AND status IN ('ACCEPTED', 'RUNNING')
                    """);
            MapSqlParameterSource parameters = new MapSqlParameterSource()
                    .addValue("attemptId", attemptId)
                    .addValue("leaseVersion", leaseVersion)
                    .addValue("terminalStatus", terminalStatus.name())
                    .addValue("result", write(result));
            if (taskInstanceId != null) {
                sql.append("""
                         AND task_instance_id = :instanceId
                         AND EXISTS (
                             SELECT 1 FROM sr_task_instance instance
                             WHERE instance.id = :instanceId
                               AND instance.current_lease_version = :leaseVersion
                               AND instance.status = 'RUNNING')
                        """);
                parameters.addValue("instanceId", taskInstanceId);
            }
            if (workerId != null) {
                sql.append(" AND worker_id = :workerId");
                parameters.addValue("workerId", workerId);
            }
            if (workerEpoch != null) {
                sql.append(" AND worker_epoch = :workerEpoch");
                parameters.addValue("workerEpoch", workerEpoch);
            }
            int updated = jdbc.update(sql.toString(), parameters);
            if (updated == 0) {
                return false;
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
            return true;
        }));
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

    private void requireTerminal(TaskAttemptStatus status) {
        if (status != TaskAttemptStatus.SUCCESS
                && status != TaskAttemptStatus.FAILED
                && status != TaskAttemptStatus.CANCELLED) {
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
}
