package com.staterelay.server.definition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
public final class TaskDefinitionService {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper objectMapper;

    public TaskDefinitionService(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.objectMapper = objectMapper;
    }

    /**
     * Creates a task definition and its first mutable draft version atomically.
     */
    public DefinitionVersion createDraft(
            UUID applicationId,
            String name,
            String handlerName,
            JsonNode configuration,
            JsonNode parameterSchema) {
        requireText(name, "name");
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID definitionId = UUID.randomUUID();
            UUID versionId = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO sr_task_definition(id, application_id, name)
                    VALUES (:id, :applicationId, :name)
                    """, new MapSqlParameterSource()
                    .addValue("id", definitionId)
                    .addValue("applicationId", Objects.requireNonNull(applicationId, "applicationId"))
                    .addValue("name", name));
            insertDraft(versionId, definitionId, 1, handlerName, configuration, parameterSchema);
            return new DefinitionVersion(definitionId, versionId, 1, "DRAFT");
        }));
    }

    /**
     * Creates the next draft without modifying any earlier version, including the published one.
     */
    public DefinitionVersion createRevision(
            UUID definitionId,
            String handlerName,
            JsonNode configuration,
            JsonNode parameterSchema) {
        return Objects.requireNonNull(transactions.execute(status -> {
            List<UUID> definitions = jdbc.query("""
                    SELECT id FROM sr_task_definition
                    WHERE id = :definitionId
                    FOR UPDATE
                    """, new MapSqlParameterSource("definitionId", definitionId),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (definitions.isEmpty()) {
                throw new IllegalArgumentException("task definition does not exist");
            }
            int versionNumber = jdbc.queryForObject("""
                    SELECT COALESCE(MAX(version_no), 0) + 1
                    FROM sr_task_definition_version
                    WHERE task_definition_id = :definitionId
                    """, new MapSqlParameterSource("definitionId", definitionId), Integer.class);
            UUID versionId = UUID.randomUUID();
            insertDraft(versionId, definitionId, versionNumber,
                    handlerName, configuration, parameterSchema);
            return new DefinitionVersion(definitionId, versionId, versionNumber, "DRAFT");
        }));
    }

    /**
     * Publishes a draft and makes it the definition's current version in the same transaction.
     */
    public DefinitionVersion publish(UUID versionId) {
        return Objects.requireNonNull(transactions.execute(status -> {
            List<DefinitionVersion> versions = jdbc.query("""
                    SELECT task_definition_id, id, version_no, status
                    FROM sr_task_definition_version
                    WHERE id = :versionId
                    FOR UPDATE
                    """, new MapSqlParameterSource("versionId", versionId),
                    (resultSet, rowNumber) -> new DefinitionVersion(
                            resultSet.getObject("task_definition_id", UUID.class),
                            resultSet.getObject("id", UUID.class),
                            resultSet.getInt("version_no"),
                            resultSet.getString("status")));
            if (versions.isEmpty()) {
                throw new IllegalArgumentException("task definition version does not exist");
            }
            DefinitionVersion version = versions.get(0);
            if (!"DRAFT".equals(version.status())) {
                if ("PUBLISHED".equals(version.status())) {
                    return version;
                }
                throw new IllegalStateException("only draft versions can be published");
            }
            jdbc.update("""
                    UPDATE sr_task_definition_version
                    SET status = 'PUBLISHED', published_at = clock_timestamp()
                    WHERE id = :versionId AND status = 'DRAFT'
                    """, new MapSqlParameterSource("versionId", versionId));
            jdbc.update("""
                    UPDATE sr_task_definition
                    SET status = 'PUBLISHED', current_published_version_id = :versionId,
                        updated_at = clock_timestamp()
                    WHERE id = :definitionId
                    """, new MapSqlParameterSource()
                    .addValue("versionId", versionId)
                    .addValue("definitionId", version.definitionId()));
            return new DefinitionVersion(
                    version.definitionId(), version.versionId(), version.versionNumber(), "PUBLISHED");
        }));
    }

    private void insertDraft(
            UUID versionId,
            UUID definitionId,
            int versionNumber,
            String handlerName,
            JsonNode configuration,
            JsonNode parameterSchema) {
        requireText(handlerName, "handlerName");
        jdbc.update("""
                INSERT INTO sr_task_definition_version(
                    id, task_definition_id, version_no, handler_name,
                    configuration_snapshot, parameter_schema)
                VALUES (:id, :definitionId, :versionNumber, :handlerName,
                    CAST(:configuration AS jsonb), CAST(:parameterSchema AS jsonb))
                """, new MapSqlParameterSource()
                .addValue("id", versionId)
                .addValue("definitionId", definitionId)
                .addValue("versionNumber", versionNumber)
                .addValue("handlerName", handlerName)
                .addValue("configuration", write(configuration))
                .addValue("parameterSchema", write(parameterSchema)));
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
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

    public record DefinitionVersion(
            UUID definitionId, UUID versionId, int versionNumber, String status) {
    }
}
