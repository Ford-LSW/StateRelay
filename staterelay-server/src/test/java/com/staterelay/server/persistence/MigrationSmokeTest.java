package com.staterelay.server.persistence;

import com.staterelay.server.support.PostgresTestConfiguration;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class MigrationSmokeTest {

    private static final Set<String> EXPECTED_TABLES = Set.of(
            "sr_application", "sr_worker", "sr_task_definition", "sr_task_definition_version",
            "sr_trigger", "sr_task_instance", "sr_task_attempt", "sr_dispatch",
            "sr_outbox_event", "sr_audit_event");

    private static final Set<String> EXPECTED_INDEXES = Set.of(
            "uk_sr_instance_trigger_time", "uk_sr_instance_business_idempotency",
            "uk_sr_attempt_number", "uk_sr_dispatch_id", "uk_sr_attempt_dispatch",
            "ix_sr_instance_ready", "ix_sr_attempt_expired_lease");

    @Autowired
    private Flyway flyway;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @Test
    void migrationBuildsAllPersistenceInvariantsAndIsIdempotent() {
        flyway.clean();
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(4);
        flyway.validate();

        assertThat(jdbcTemplate.queryForList("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name LIKE 'sr_%'
                """, String.class)).containsExactlyInAnyOrderElementsOf(EXPECTED_TABLES);
        assertThat(jdbcTemplate.queryForList("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public' AND indexname IN (
                    'uk_sr_instance_trigger_time', 'uk_sr_instance_business_idempotency',
                    'uk_sr_attempt_number', 'uk_sr_dispatch_id', 'uk_sr_attempt_dispatch',
                    'ix_sr_instance_ready', 'ix_sr_attempt_expired_lease')
                """, String.class)).containsExactlyInAnyOrderElementsOf(EXPECTED_INDEXES);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*) FROM pg_constraint
                WHERE conname IN ('ck_sr_instance_payload_size', 'ck_sr_attempt_result_size',
                    'ck_sr_attempt_number', 'ck_sr_attempt_progress', 'ck_sr_attempt_lease_version',
                    'ck_sr_instance_lease_version', 'ck_sr_dispatch_transport_generation')
                """, Integer.class)).isEqualTo(7);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*) FROM information_schema.columns
                WHERE (table_name = 'sr_worker'
                        AND column_name = 'reported_active_attempt_ids'
                        AND data_type = 'ARRAY' AND udt_name = '_uuid')
                   OR (table_name = 'sr_dispatch'
                        AND column_name = 'transport_generation'
                        AND data_type = 'bigint')
                """, Integer.class)).isEqualTo(2);

        assertThat(flyway.info().pending()).isEmpty();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
    }

    @Test
    void v4UpgradesExistingWorkerAndDispatchRowsWithSafeDefaults() {
        Flyway throughV3 = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .cleanDisabled(false)
                .target("3")
                .load();
        throughV3.clean();
        assertThat(throughV3.migrate().migrationsExecuted).isEqualTo(3);
        UUID applicationId = UUID.randomUUID();
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        UUID definitionId = UUID.randomUUID();
        UUID versionId = UUID.randomUUID();
        UUID instanceId = UUID.randomUUID();
        UUID attemptId = UUID.randomUUID();
        UUID dispatchRowId = UUID.randomUUID();
        UUID dispatchId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO sr_application(id, name) VALUES (?, ?)",
                applicationId, "existing-app");
        jdbcTemplate.update("""
                INSERT INTO sr_worker(
                    id, application_id, worker_key, worker_epoch, pod_name, pod_ip,
                    executor_port, status, max_concurrency, lease_expires_at)
                VALUES (?, ?, ?, ?, ?, '127.0.0.1', 8080, 'READY', 1,
                    clock_timestamp() + interval '1 minute')
                """, workerId, applicationId, "existing-worker", workerEpoch, "existing-pod");
        jdbcTemplate.update("""
                INSERT INTO sr_task_definition(id, application_id, name)
                VALUES (?, ?, 'existing-definition')
                """, definitionId, applicationId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_definition_version(
                    id, task_definition_id, version_no, status, handler_name,
                    configuration_snapshot, published_at)
                VALUES (?, ?, 1, 'PUBLISHED', 'archiveOrders', '{}'::jsonb, clock_timestamp())
                """, versionId, definitionId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_instance(
                    id, task_definition_id, definition_version_id, status,
                    scheduled_at, next_run_at, configuration_snapshot, payload,
                    current_lease_version)
                VALUES (?, ?, ?, 'RUNNING', clock_timestamp(), clock_timestamp(),
                    '{}'::jsonb, '{}'::jsonb, 1)
                """, instanceId, definitionId, versionId);
        jdbcTemplate.update("""
                INSERT INTO sr_task_attempt(
                    id, task_instance_id, attempt_no, lease_version, worker_id,
                    worker_epoch, status, lease_expires_at)
                VALUES (?, ?, 1, 1, ?, ?, 'ASSIGNED',
                    clock_timestamp() + interval '1 minute')
                """, attemptId, instanceId, workerId, workerEpoch);
        jdbcTemplate.update("""
                INSERT INTO sr_dispatch(
                    id, dispatch_id, task_attempt_id, target_worker_id,
                    target_worker_epoch, target_address, status, next_transport_at)
                VALUES (?, ?, ?, ?, ?, 'http://127.0.0.1:8080', 'PENDING', clock_timestamp())
                """, dispatchRowId, dispatchId, attemptId, workerId, workerEpoch);

        assertThat(flyway.migrate().migrationsExecuted).isOne();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT cardinality(reported_active_attempt_ids) FROM sr_worker WHERE id = ?",
                Integer.class, workerId)).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT transport_generation FROM sr_dispatch WHERE dispatch_id = ?",
                Long.class, dispatchId)).isZero();
        flyway.validate();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
    }
}
