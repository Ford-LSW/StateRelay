package com.staterelay.server.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

@Repository
public final class OutboxRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OutboxRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Adds an event using the caller's transaction. State-mutating repositories call this
     * before their {@code TransactionTemplate} callback returns so state and evidence commit together.
     */
    public UUID append(String aggregateType, UUID aggregateId, String eventType, JsonNode payload) {
        UUID eventId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO sr_outbox_event(id, aggregate_type, aggregate_id, event_type, payload, occurred_at)
                VALUES (:id, :aggregateType, :aggregateId, :eventType, CAST(:payload AS jsonb), :occurredAt)
                """, new MapSqlParameterSource()
                .addValue("id", eventId)
                .addValue("aggregateType", aggregateType)
                .addValue("aggregateId", aggregateId)
                .addValue("eventType", eventType)
                .addValue("payload", write(payload == null ? JsonNodeFactory.instance.objectNode() : payload))
                .addValue("occurredAt", Instant.now().atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
        return eventId;
    }

    private String write(JsonNode payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
