package com.staterelay.server.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Repository
public final class TaskInstanceRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public TaskInstanceRepository(
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
     * Creates the planned occurrence once. Concurrent schedulers converge on the row protected
     * by the partial trigger/time unique index; only the transaction that inserts it emits Outbox.
     */
    public UUID createScheduledInstance(
            UUID triggerId, Instant scheduledAt, UUID definitionVersionId, JsonNode payload) {
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID candidateId = UUID.randomUUID();
            List<UUID> inserted = jdbc.query("""
                    INSERT INTO sr_task_instance(
                        id, task_definition_id, definition_version_id, trigger_id, status,
                        scheduled_at, next_run_at, configuration_snapshot, payload)
                    SELECT :id, version.task_definition_id, version.id, :triggerId, 'READY',
                        :scheduledAt, :scheduledAt, version.configuration_snapshot, CAST(:payload AS jsonb)
                    FROM sr_task_definition_version version
                    WHERE version.id = :definitionVersionId
                    ON CONFLICT (trigger_id, scheduled_at) WHERE trigger_id IS NOT NULL DO NOTHING
                    RETURNING id
                    """, new MapSqlParameterSource()
                    .addValue("id", candidateId)
                    .addValue("triggerId", triggerId)
                    .addValue("scheduledAt", scheduledAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("definitionVersionId", definitionVersionId)
                    .addValue("payload", write(payload)),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (!inserted.isEmpty()) {
                outbox.append("TASK_INSTANCE", candidateId, "TASK_INSTANCE_CREATED",
                        JsonNodeFactory.instance.objectNode()
                                .put("taskInstanceId", candidateId.toString())
                                .put("triggerId", triggerId.toString()));
                return candidateId;
            }
            return jdbc.queryForObject("""
                    SELECT id FROM sr_task_instance
                    WHERE trigger_id = :triggerId AND scheduled_at = :scheduledAt
                    """, new MapSqlParameterSource()
                    .addValue("triggerId", triggerId)
                    .addValue("scheduledAt", scheduledAt.atOffset(ZoneOffset.UTC),
                            Types.TIMESTAMP_WITH_TIMEZONE), UUID.class);
        }));
    }

    /**
     * Creates an API-triggered instance and, when a business key is supplied, returns the
     * existing row on a duplicate request without emitting a second event.
     */
    public UUID createManualInstance(
            UUID taskDefinitionId,
            UUID definitionVersionId,
            String businessIdempotencyKey,
            Instant requestedAt,
            JsonNode payload) {
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID candidateId = UUID.randomUUID();
            List<UUID> inserted = jdbc.query("""
                    INSERT INTO sr_task_instance(
                        id, task_definition_id, definition_version_id, business_idempotency_key,
                        status, scheduled_at, next_run_at, configuration_snapshot, payload)
                    SELECT :id, :taskDefinitionId, version.id, :idempotencyKey,
                        'READY', :requestedAt, :requestedAt, version.configuration_snapshot, CAST(:payload AS jsonb)
                    FROM sr_task_definition_version version
                    WHERE version.id = :definitionVersionId
                      AND version.task_definition_id = :taskDefinitionId
                    ON CONFLICT (task_definition_id, business_idempotency_key)
                        WHERE business_idempotency_key IS NOT NULL DO NOTHING
                    RETURNING id
                    """, new MapSqlParameterSource()
                    .addValue("id", candidateId)
                    .addValue("taskDefinitionId", taskDefinitionId)
                    .addValue("definitionVersionId", definitionVersionId)
                    .addValue("idempotencyKey", businessIdempotencyKey)
                    .addValue("requestedAt", requestedAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("payload", write(payload)),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (!inserted.isEmpty()) {
                outbox.append("TASK_INSTANCE", candidateId, "TASK_INSTANCE_CREATED",
                        JsonNodeFactory.instance.objectNode().put("taskInstanceId", candidateId.toString()));
                return candidateId;
            }
            if (businessIdempotencyKey == null) {
                throw new IllegalArgumentException("definition version does not belong to task definition");
            }
            return jdbc.queryForObject("""
                    SELECT id FROM sr_task_instance
                    WHERE task_definition_id = :taskDefinitionId
                      AND business_idempotency_key = :idempotencyKey
                    """, new MapSqlParameterSource()
                    .addValue("taskDefinitionId", taskDefinitionId)
                    .addValue("idempotencyKey", businessIdempotencyKey), UUID.class);
        }));
    }

    /**
     * Claims rows and persists a claim token before committing. Row locks prevent overlap during
     * selection; the token is the durable ownership proof after those locks are released.
     */
    public List<ClaimedTaskInstance> claimReadyBatch(Instant now, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID claimToken = UUID.randomUUID();
            List<UUID> ids = jdbc.query("""
                    SELECT id
                    FROM sr_task_instance
                    WHERE status IN ('READY', 'RETRY_WAIT')
                      AND next_run_at <= :now
                      AND claim_token IS NULL
                    ORDER BY priority DESC, next_run_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :batchSize
                    """, new MapSqlParameterSource()
                    .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("batchSize", batchSize),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (ids.isEmpty()) {
                return List.of();
            }
            int updated = jdbc.update("""
                    UPDATE sr_task_instance
                    SET claim_token = :claimToken, claimed_at = :now, updated_at = clock_timestamp()
                    WHERE id IN (:ids) AND claim_token IS NULL
                    """, new MapSqlParameterSource()
                    .addValue("claimToken", claimToken)
                    .addValue("now", now.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("ids", ids));
            if (updated != ids.size()) {
                throw new IllegalStateException("claim update did not preserve selected ownership");
            }
            for (UUID id : ids) {
                outbox.append("TASK_INSTANCE", id, "TASK_INSTANCE_CLAIMED",
                        JsonNodeFactory.instance.objectNode()
                                .put("taskInstanceId", id.toString())
                                .put("claimToken", claimToken.toString()));
            }
            return ids.stream().map(id -> new ClaimedTaskInstance(id, claimToken)).toList();
        }));
    }

    public int countByTriggerAndTime(UUID triggerId, Instant scheduledAt) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM sr_task_instance
                WHERE trigger_id = :triggerId AND scheduled_at = :scheduledAt
                """, new MapSqlParameterSource()
                .addValue("triggerId", triggerId)
                .addValue("scheduledAt", scheduledAt.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE), Integer.class);
    }

    public int countByBusinessIdempotencyKey(UUID taskDefinitionId, String idempotencyKey) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM sr_task_instance
                WHERE task_definition_id = :taskDefinitionId
                  AND business_idempotency_key = :idempotencyKey
                """, new MapSqlParameterSource()
                .addValue("taskDefinitionId", taskDefinitionId)
                .addValue("idempotencyKey", idempotencyKey), Integer.class);
    }

    private String write(JsonNode value) {
        try {
            return objectMapper.writeValueAsString(
                    value == null ? JsonNodeFactory.instance.objectNode() : value);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public record ClaimedTaskInstance(UUID instanceId, UUID claimToken) {
    }
}
