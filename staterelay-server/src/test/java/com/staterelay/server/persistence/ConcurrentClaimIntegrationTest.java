package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class ConcurrentClaimIntegrationTest extends PostgresRepositoryTestSupport {

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    @RepeatedTest(20)
    void concurrentClaimersNeverOwnTheSameReadyInstance() throws Exception {
        DefinitionFixture fixture = definitionFixture();
        insertReadyInstances(fixture, 200);
        OutboxRepository outboxRepository = new OutboxRepository(jdbc, objectMapper);
        TaskInstanceRepository repository = new TaskInstanceRepository(
                jdbc, transactions, outboxRepository, objectMapper);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<TaskInstanceRepository.ClaimedTaskInstance>> futureA = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return repository.claimReadyBatch(Instant.parse("2026-08-13T10:00:00Z"), 100);
            });
            Future<List<TaskInstanceRepository.ClaimedTaskInstance>> futureB = executor.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                return repository.claimReadyBatch(Instant.parse("2026-08-13T10:00:00Z"), 100);
            });
            start.countDown();

            Set<UUID> claimedByA = ids(futureA.get(30, TimeUnit.SECONDS));
            Set<UUID> claimedByB = ids(futureB.get(30, TimeUnit.SECONDS));

            assertThat(claimedByA).doesNotContainAnyElementsOf(claimedByB);
            assertThat(Stream.concat(claimedByA.stream(), claimedByB.stream()).distinct()).hasSize(200);
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM sr_task_instance WHERE claim_token IS NOT NULL", Integer.class))
                    .isEqualTo(200);
        } finally {
            executor.shutdownNow();
        }
    }

    private void insertReadyInstances(DefinitionFixture fixture, int count) {
        for (int index = 0; index < count; index++) {
            jdbcTemplate.update("""
                    INSERT INTO sr_task_instance(id, task_definition_id, definition_version_id, status,
                        priority, scheduled_at, next_run_at, configuration_snapshot, payload)
                    VALUES (?, ?, ?, 'READY', ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb))
                    """, UUID.randomUUID(), fixture.definitionId(), fixture.versionId(), index % 5,
                    Instant.parse("2026-08-13T09:00:00Z").plusSeconds(index).atOffset(ZoneOffset.UTC),
                    Instant.parse("2026-08-13T09:00:00Z").atOffset(ZoneOffset.UTC), "{}", "{}");
        }
    }

    private Set<UUID> ids(List<TaskInstanceRepository.ClaimedTaskInstance> claims) {
        return claims.stream()
                .map(TaskInstanceRepository.ClaimedTaskInstance::instanceId)
                .collect(Collectors.toSet());
    }
}
