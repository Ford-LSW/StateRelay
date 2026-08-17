package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.sql.Types;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class DispatchRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final OutboxRepository outbox;

    public DispatchRepository(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.outbox = outbox;
    }

    /**
     * Persists one stable logical dispatch and its transactional Outbox evidence. Later transport
     * retries update this row and must reuse its returned dispatch ID.
     */
    public UUID createDispatch(
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            String targetAddress,
            Instant nextTransportAt,
            Instant expiresAt) {
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID id = UUID.randomUUID();
            UUID dispatchId = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO sr_dispatch(
                        id, dispatch_id, task_attempt_id, target_worker_id, target_worker_epoch,
                        target_address, status, next_transport_at, expires_at)
                    VALUES (:id, :dispatchId, :attemptId, :workerId, :workerEpoch,
                        :targetAddress, 'PENDING', :nextTransportAt, :expiresAt)
                    """, new MapSqlParameterSource()
                    .addValue("id", id)
                    .addValue("dispatchId", dispatchId)
                    .addValue("attemptId", taskAttemptId)
                    .addValue("workerId", targetWorkerId)
                    .addValue("workerEpoch", targetWorkerEpoch)
                    .addValue("targetAddress", targetAddress)
                    .addValue("nextTransportAt", nextTransportAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("expiresAt", expiresAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
            outbox.append("DISPATCH", dispatchId, "DISPATCH_CREATED",
                    JsonNodeFactory.instance.objectNode()
                            .put("dispatchId", dispatchId.toString())
                            .put("attemptId", taskAttemptId.toString())
                            .put("workerId", targetWorkerId.toString())
                            .put("workerEpoch", targetWorkerEpoch.toString()));
            return dispatchId;
        }));
    }

    /**
     * Claims a due PENDING, UNCERTAIN, or expired SENT row and returns the only token allowed to
     * write a non-acceptance transport result for that bounded HTTP send.
     */
    public Optional<SendLease> claimSending(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            Instant now,
            Instant sendLeaseExpiresAt) {
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(sendLeaseExpiresAt, "sendLeaseExpiresAt");
        if (!sendLeaseExpiresAt.isAfter(now)) {
            throw new IllegalArgumentException("sendLeaseExpiresAt must be after now");
        }
        return Objects.requireNonNull(transactions.execute(status -> {
            List<Long> claimed = jdbc.query("""
                    UPDATE sr_dispatch
                    SET status = 'SENT',
                        transport_attempts = transport_attempts + 1,
                        transport_generation = transport_generation + 1,
                        next_transport_at = :sendLeaseExpiresAt,
                        sent_at = clock_timestamp(), last_error = NULL,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status IN ('PENDING', 'UNCERTAIN', 'SENT')
                      AND next_transport_at <= :now
                    RETURNING transport_generation
                    """, new MapSqlParameterSource()
                    .addValue("dispatchId", dispatchId)
                    .addValue("attemptId", taskAttemptId)
                    .addValue("workerId", targetWorkerId)
                    .addValue("workerEpoch", targetWorkerEpoch)
                    .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("sendLeaseExpiresAt",
                            sendLeaseExpiresAt.atOffset(ZoneOffset.UTC),
                            Types.TIMESTAMP_WITH_TIMEZONE),
                    (resultSet, rowNumber) -> resultSet.getLong("transport_generation"));
            if (claimed.isEmpty()) {
                return Optional.empty();
            }
            long generation = claimed.get(0);
            outbox.append("DISPATCH", dispatchId, "DISPATCH_STATUS_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("dispatchId", dispatchId.toString())
                            .put("attemptId", taskAttemptId.toString())
                            .put("workerId", targetWorkerId.toString())
                            .put("workerEpoch", targetWorkerEpoch.toString())
                            .put("status", "SENT")
                            .put("transportGeneration", generation)
                            .put("sendLeaseExpiresAt", sendLeaseExpiresAt.toString()));
            return Optional.of(new SendLease(generation, sendLeaseExpiresAt));
        }));
    }

    /** Schedules a stable-ID transport retry after an uncertain transmission result. */
    public boolean markUncertain(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            long transportGeneration,
            Instant nextTransportAt,
            String lastError) {
        return transitionOwned(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                transportGeneration, "UNCERTAIN", nextTransportAt, lastError);
    }

    /** Records proof that the Worker accepted this exact logical Dispatch. */
    public boolean markAcked(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_dispatch
                    SET status = 'ACKED', next_transport_at = NULL,
                        acknowledged_at = clock_timestamp(), last_error = NULL,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status = 'SENT'
                    """, dispatchFence(
                    dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch));
            if (updated == 0) {
                return false;
            }
            appendStatusChanged(
                    dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch, "ACKED");
            return true;
        }));
    }

    /** Closes a Dispatch after the addressed Worker explicitly proves non-acceptance. */
    public boolean markExpired(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            long transportGeneration,
            String reason) {
        return transitionOwned(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                transportGeneration, "EXPIRED", null, reason);
    }

    private boolean transitionOwned(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            long transportGeneration,
            String next,
            Instant nextTransportAt,
            String lastError) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_dispatch
                    SET status = :next,
                        next_transport_at = :nextTransportAt,
                        last_error = :lastError,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status = 'SENT'
                      AND transport_generation = :transportGeneration
                    """, dispatchFence(
                            dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch)
                    .addValue("transportGeneration", transportGeneration)
                    .addValue("next", next)
                    .addValue("nextTransportAt", nextTransportAt == null ? null
                            : nextTransportAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("lastError", lastError));
            if (updated == 0) {
                return false;
            }
            appendStatusChanged(
                    dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch, next);
            return true;
        }));
    }

    private MapSqlParameterSource dispatchFence(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch) {
        return new MapSqlParameterSource()
                .addValue("dispatchId", dispatchId)
                .addValue("attemptId", taskAttemptId)
                .addValue("workerId", targetWorkerId)
                .addValue("workerEpoch", targetWorkerEpoch);
    }

    private void appendStatusChanged(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            String status) {
        outbox.append("DISPATCH", dispatchId, "DISPATCH_STATUS_CHANGED",
                JsonNodeFactory.instance.objectNode()
                        .put("dispatchId", dispatchId.toString())
                        .put("attemptId", taskAttemptId.toString())
                        .put("workerId", targetWorkerId.toString())
                        .put("workerEpoch", targetWorkerEpoch.toString())
                        .put("status", status));
    }

    public record SendLease(long generation, Instant expiresAt) {
    }
}
