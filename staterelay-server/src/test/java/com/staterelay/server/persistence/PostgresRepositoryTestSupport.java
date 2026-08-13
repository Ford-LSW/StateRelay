package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Instant;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.UUID;

public abstract class PostgresRepositoryTestSupport {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected NamedParameterJdbcTemplate jdbc;

    @BeforeEach
    protected void truncateDatabase() {
        jdbcTemplate.execute("""
                TRUNCATE TABLE sr_audit_event, sr_outbox_event, sr_dispatch, sr_task_attempt,
                    sr_task_instance, sr_trigger, sr_task_definition_version, sr_task_definition,
                    sr_worker, sr_application CASCADE
                """);
    }

    protected DefinitionFixture definitionFixture() {
        UUID applicationId = UUID.randomUUID();
        UUID definitionId = UUID.randomUUID();
        UUID versionId = UUID.randomUUID();
        UUID triggerId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO sr_application(id, name) VALUES (:id, :name)
                """, new MapSqlParameterSource()
                .addValue("id", applicationId)
                .addValue("name", "app-" + applicationId));
        jdbc.update("""
                INSERT INTO sr_task_definition(id, application_id, name)
                VALUES (:id, :applicationId, :name)
                """, new MapSqlParameterSource()
                .addValue("id", definitionId)
                .addValue("applicationId", applicationId)
                .addValue("name", "definition-" + definitionId));
        jdbc.update("""
                INSERT INTO sr_task_definition_version(id, task_definition_id, version_no, status,
                    configuration_snapshot, published_at)
                VALUES (:id, :definitionId, 1, 'PUBLISHED', CAST(:configuration AS jsonb), :publishedAt)
                """, new MapSqlParameterSource()
                .addValue("id", versionId)
                .addValue("definitionId", definitionId)
                .addValue("configuration", JsonNodeFactory.instance.objectNode().put("timeoutSeconds", 300).toString())
                .addValue("publishedAt", Instant.parse("2026-08-13T09:00:00Z").atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
        jdbc.update("""
                INSERT INTO sr_trigger(id, task_definition_id, definition_version_id, trigger_type, status,
                    schedule_config, next_fire_at)
                VALUES (:id, :definitionId, :versionId, 'FIXED_RATE', 'ACTIVE',
                    CAST(:scheduleConfig AS jsonb), :nextFireAt)
                """, new MapSqlParameterSource()
                .addValue("id", triggerId)
                .addValue("definitionId", definitionId)
                .addValue("versionId", versionId)
                .addValue("scheduleConfig", "{\"intervalSeconds\":60}")
                .addValue("nextFireAt", Instant.parse("2026-08-13T10:00:00Z").atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
        return new DefinitionFixture(applicationId, definitionId, versionId, triggerId);
    }

    protected WorkerFixture workerFixture(UUID applicationId, int capacity) {
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO sr_worker(id, application_id, worker_key, worker_epoch, pod_name, pod_ip,
                    executor_port, status, max_concurrency, lease_expires_at)
                VALUES (:id, :applicationId, :workerKey, :workerEpoch, :podName, '127.0.0.1',
                    8080, 'READY', :capacity, :leaseExpiresAt)
                """, new MapSqlParameterSource()
                .addValue("id", workerId)
                .addValue("applicationId", applicationId)
                .addValue("workerKey", "worker-" + workerId)
                .addValue("workerEpoch", workerEpoch)
                .addValue("podName", "pod-" + workerId)
                .addValue("capacity", capacity)
                .addValue("leaseExpiresAt", Instant.now().plusSeconds(3_600).atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE));
        return new WorkerFixture(workerId, workerEpoch);
    }

    protected record DefinitionFixture(
            UUID applicationId, UUID definitionId, UUID versionId, UUID triggerId) {
    }

    protected record WorkerFixture(UUID workerId, UUID workerEpoch) {
    }
}
