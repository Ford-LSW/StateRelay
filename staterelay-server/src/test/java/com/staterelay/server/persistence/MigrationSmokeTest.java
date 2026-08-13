package com.staterelay.server.persistence;

import com.staterelay.server.support.PostgresTestConfiguration;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Set;

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

    @Test
    void migrationBuildsAllPersistenceInvariantsAndIsIdempotent() {
        flyway.clean();
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(3);
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
                    'ck_sr_instance_lease_version')
                """, Integer.class)).isEqualTo(6);

        assertThat(flyway.info().pending()).isEmpty();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
    }
}
