package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.DispatchStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.sql.Types;
import java.util.List;
import java.util.Objects;
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
     * Advances the exact logical dispatch addressed to one Attempt and Worker epoch. Transport
     * retries reuse the stable dispatch ID; this method never creates a replacement Attempt.
     */
    public boolean casStatus(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            DispatchStatus expected,
            DispatchStatus next,
            String lastError) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_dispatch
                    SET status = :next, last_error = :lastError,
                        transport_attempts = transport_attempts + 1,
                        sent_at = CASE WHEN :next IN ('SENT', 'UNCERTAIN', 'ACKED')
                            THEN COALESCE(sent_at, clock_timestamp()) ELSE sent_at END,
                        acknowledged_at = CASE WHEN :next = 'ACKED' THEN clock_timestamp()
                            ELSE acknowledged_at END,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status = :expected
                    """, new MapSqlParameterSource()
                    .addValue("dispatchId", dispatchId)
                    .addValue("attemptId", taskAttemptId)
                    .addValue("workerId", targetWorkerId)
                    .addValue("workerEpoch", targetWorkerEpoch)
                    .addValue("expected", expected.name())
                    .addValue("next", next.name())
                    .addValue("lastError", lastError));
            if (updated == 0) {
                return false;
            }
            outbox.append("DISPATCH", dispatchId, "DISPATCH_STATUS_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("dispatchId", dispatchId.toString())
                            .put("attemptId", taskAttemptId.toString())
                            .put("workerId", targetWorkerId.toString())
                            .put("workerEpoch", targetWorkerEpoch.toString())
                            .put("status", next.name()));
            return true;
        }));
    }

    /** Claims one pending or uncertain logical Dispatch for a single HTTP transmission. */
    public boolean markSending(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch) {
        Instant now = Instant.now();
        return markSending(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                now, now.plus(Duration.ofSeconds(5)));
    }

    /**
     * Claims a due PENDING, UNCERTAIN, or expired SENT row for one bounded HTTP send lease.
     * The compare-and-set is the single-sender fence shared by direct and scanner delivery.
     */
    public boolean markSending(
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
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_dispatch
                    SET status = 'SENT',
                        transport_attempts = transport_attempts + 1,
                        next_transport_at = :sendLeaseExpiresAt,
                        sent_at = clock_timestamp(), last_error = NULL,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status IN ('PENDING', 'UNCERTAIN', 'SENT')
                      AND next_transport_at <= :now
                    """, new MapSqlParameterSource()
                    .addValue("dispatchId", dispatchId)
                    .addValue("attemptId", taskAttemptId)
                    .addValue("workerId", targetWorkerId)
                    .addValue("workerEpoch", targetWorkerEpoch)
                    .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("sendLeaseExpiresAt",
                            sendLeaseExpiresAt.atOffset(ZoneOffset.UTC),
                            Types.TIMESTAMP_WITH_TIMEZONE));
            if (updated == 0) {
                return false;
            }
            outbox.append("DISPATCH", dispatchId, "DISPATCH_STATUS_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("dispatchId", dispatchId.toString())
                            .put("attemptId", taskAttemptId.toString())
                            .put("workerId", targetWorkerId.toString())
                            .put("workerEpoch", targetWorkerEpoch.toString())
                            .put("status", DispatchStatus.SENT.name())
                            .put("sendLeaseExpiresAt", sendLeaseExpiresAt.toString()));
            return true;
        }));
    }

    /** Schedules a stable-ID transport retry after an uncertain transmission result. */
    public boolean markUncertain(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            Instant nextTransportAt,
            String lastError) {
        return transition(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                List.of(DispatchStatus.SENT.name()), DispatchStatus.UNCERTAIN,
                nextTransportAt, lastError, false);
    }

    /** Records proof that the Worker accepted this exact logical Dispatch. */
    public boolean markAcked(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch) {
        return transition(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                List.of(DispatchStatus.SENT.name()), DispatchStatus.ACKED, null, null, false);
    }

    /** Closes a Dispatch after the addressed Worker explicitly proves non-acceptance. */
    public boolean markExpired(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            String reason) {
        return transition(dispatchId, taskAttemptId, targetWorkerId, targetWorkerEpoch,
                List.of(DispatchStatus.SENT.name()), DispatchStatus.EXPIRED, null, reason, false);
    }

    private boolean transition(
            UUID dispatchId,
            UUID taskAttemptId,
            UUID targetWorkerId,
            UUID targetWorkerEpoch,
            List<String> expectedStatuses,
            DispatchStatus next,
            Instant nextTransportAt,
            String lastError,
            boolean transmission) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_dispatch
                    SET status = :next,
                        transport_attempts = transport_attempts + CASE WHEN :transmission THEN 1 ELSE 0 END,
                        next_transport_at = :nextTransportAt,
                        sent_at = CASE WHEN :transmission THEN clock_timestamp() ELSE sent_at END,
                        acknowledged_at = CASE WHEN :next = 'ACKED' THEN clock_timestamp()
                            ELSE acknowledged_at END,
                        last_error = :lastError,
                        updated_at = clock_timestamp()
                    WHERE dispatch_id = :dispatchId
                      AND task_attempt_id = :attemptId
                      AND target_worker_id = :workerId
                      AND target_worker_epoch = :workerEpoch
                      AND status IN (:expectedStatuses)
                    """, new MapSqlParameterSource()
                    .addValue("dispatchId", dispatchId)
                    .addValue("attemptId", taskAttemptId)
                    .addValue("workerId", targetWorkerId)
                    .addValue("workerEpoch", targetWorkerEpoch)
                    .addValue("expectedStatuses", expectedStatuses)
                    .addValue("next", next.name())
                    .addValue("nextTransportAt", nextTransportAt == null ? null
                            : nextTransportAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("lastError", lastError)
                    .addValue("transmission", transmission));
            if (updated == 0) {
                return false;
            }
            outbox.append("DISPATCH", dispatchId, "DISPATCH_STATUS_CHANGED",
                    JsonNodeFactory.instance.objectNode()
                            .put("dispatchId", dispatchId.toString())
                            .put("attemptId", taskAttemptId.toString())
                            .put("workerId", targetWorkerId.toString())
                            .put("workerEpoch", targetWorkerEpoch.toString())
                            .put("status", next.name()));
            return true;
        }));
    }
}
