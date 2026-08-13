package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.domain.DispatchStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.sql.Types;
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
}
