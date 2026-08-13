package com.staterelay.server.trigger;

import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.server.persistence.TaskInstanceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

@Service
public final class TriggerService {

    private final NamedParameterJdbcTemplate jdbc;
    private final TaskInstanceRepository instances;
    private final Clock clock;

    @Autowired
    public TriggerService(
            NamedParameterJdbcTemplate jdbc, TaskInstanceRepository instances) {
        this(jdbc, instances, Clock.systemUTC());
    }

    public TriggerService(
            NamedParameterJdbcTemplate jdbc, TaskInstanceRepository instances, Clock clock) {
        this.jdbc = jdbc;
        this.instances = instances;
        this.clock = clock;
    }

    /**
     * Immediately creates an instance from the current published version, deduplicating by business key when present.
     */
    public UUID triggerNow(UUID definitionId, String idempotencyKey, JsonNode payload) {
        String normalizedKey = idempotencyKey == null || idempotencyKey.isBlank()
                ? null : idempotencyKey;
        List<UUID> versions = jdbc.query("""
                SELECT version.id
                FROM sr_task_definition definition
                JOIN sr_task_definition_version version
                  ON version.id = definition.current_published_version_id
                WHERE definition.id = :definitionId
                  AND definition.status = 'PUBLISHED'
                  AND version.status = 'PUBLISHED'
                """, new MapSqlParameterSource("definitionId", definitionId),
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
        if (versions.isEmpty()) {
            throw new IllegalStateException("task definition has no published version");
        }
        return instances.createManualInstance(definitionId, versions.get(0), normalizedKey,
                clock.instant(), payload);
    }
}
