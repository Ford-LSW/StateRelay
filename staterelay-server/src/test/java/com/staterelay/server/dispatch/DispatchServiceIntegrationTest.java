package com.staterelay.server.dispatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.PostgresRepositoryTestSupport;
import com.staterelay.server.persistence.TaskInstanceRepository;
import com.staterelay.server.support.PostgresTestConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER;
import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class DispatchServiceIntegrationTest extends PostgresRepositoryTestSupport {

    private static final String EXECUTE_PATH = "/staterelay/internal/v1/executions";

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    private WireMockServer executor;

    @BeforeEach
    void startExecutor() {
        executor = new WireMockServer(wireMockConfig().dynamicPort());
        executor.start();
    }

    @AfterEach
    void stopExecutor() {
        executor.stop();
    }

    @Test
    void ackLossRetriesTheSameLogicalDispatchAndAttempt() throws Exception {
        Fixture fixture = insertFixture(1, 0);
        executor.stubFor(post(urlEqualTo(EXECUTE_PATH)).inScenario("ACK loss")
                .whenScenarioStateIs("Started")
                .willSetStateTo("accepted")
                .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER)));
        executor.stubFor(post(urlEqualTo(EXECUTE_PATH)).inScenario("ACK loss")
                .whenScenarioStateIs("accepted")
                .willReturn(aResponse().withHeader("Content-Type", "application/json")
                        .withTransformers("response-template")
                        .withBody("""
                                {
                                  "dispatchId": "{{jsonPath request.body '$.dispatchId'}}",
                                  "attemptId": "{{jsonPath request.body '$.attemptId'}}",
                                  "workerId": "{{jsonPath request.body '$.targetWorkerId'}}",
                                  "workerEpoch": "{{jsonPath request.body '$.targetWorkerEpoch'}}",
                                  "status": "DUPLICATE",
                                  "message": "already accepted"
                                }
                                """)));
        DispatchService service = service();

        var claimed = taskInstances().claimReadyBatch(Instant.now(), 1).get(0);
        DispatchService.DispatchAssignment assignment = service.assign(claimed).orElseThrow();
        service.deliver(assignment);
        service.retryUncertain(Instant.now().plusSeconds(2), 10);

        List<JsonNode> bodies = executor.getAllServeEvents().stream()
                .map(event -> read(event.getRequest().getBodyAsString()))
                .toList();
        assertThat(bodies).hasSize(2);
        assertThat(bodies).extracting(node -> node.path("dispatchId").asText())
                .containsOnly(assignment.dispatchId().toString());
        assertThat(bodies).extracting(node -> node.path("attemptId").asText())
                .containsOnly(assignment.attemptId().toString());
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_attempt WHERE task_instance_id = ?",
                Integer.class, fixture.instanceId())).isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_dispatch WHERE task_attempt_id = ?",
                Integer.class, assignment.attemptId())).isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM sr_dispatch WHERE dispatch_id = ?",
                String.class, assignment.dispatchId())).isEqualTo("ACKED");
    }

    @Test
    void explicitCapacityRejectionReleasesOnceAndNextAttemptUsesHigherLease() {
        Fixture first = insertFixture(1, 0);
        stubAck("REJECTED_CAPACITY", first.workerId(), first.workerEpoch());
        DispatchService service = service();

        var claim = taskInstances().claimReadyBatch(Instant.now(), 1).get(0);
        DispatchService.DispatchAssignment rejected = service.assign(claim).orElseThrow();
        service.deliver(rejected);
        service.deliver(rejected);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, first.workerId())).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_attempt WHERE id = ?",
                String.class, rejected.attemptId())).isEqualTo("LOST");

        UUID replacementWorker = insertWorker(first.applicationId(), 1, 0);
        var retryClaim = taskInstances().claimReadyBatch(Instant.now().plusSeconds(1), 1).get(0);
        DispatchService.DispatchAssignment replacement = service.assign(retryClaim).orElseThrow();
        assertThat(replacement.attemptId()).isNotEqualTo(rejected.attemptId());
        assertThat(replacement.workerId()).isEqualTo(replacementWorker);
        assertThat(replacement.leaseVersion()).isGreaterThan(rejected.leaseVersion());
    }

    @Test
    void staleEpochAckCannotAcknowledgeOrReleaseTheDispatch() {
        Fixture fixture = insertFixture(1, 0);
        executor.stubFor(post(urlEqualTo(EXECUTE_PATH)).willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"dispatchId":"ignored","attemptId":"ignored","workerId":"ignored",
                         "workerEpoch":"00000000-0000-0000-0000-000000000000",
                         "status":"REJECTED_STALE_EPOCH","message":"stale"}
                        """)));
        DispatchService service = service();
        var claim = taskInstances().claimReadyBatch(Instant.now(), 1).get(0);
        DispatchService.DispatchAssignment assignment = service.assign(claim).orElseThrow();

        service.deliver(assignment);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM sr_dispatch WHERE dispatch_id = ?",
                String.class, assignment.dispatchId())).isEqualTo("UNCERTAIN");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM sr_task_attempt WHERE id = ?",
                String.class, assignment.attemptId())).isEqualTo("ASSIGNED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, fixture.workerId())).isOne();
    }

    @Test
    void databaseReservationUsesEffectiveLoadAndNeverOverbooksConcurrently() throws Exception {
        Fixture fixture = insertFixture(1, 1);
        DispatchService service = service();
        UUID secondInstance = insertReadyInstance(fixture.definitionId(), fixture.versionId());
        List<TaskInstanceRepository.ClaimedTaskInstance> claims =
                taskInstances().claimReadyBatch(Instant.now(), 2);

        var results = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = results.submit(() -> service.assign(claims.get(0)));
            var second = results.submit(() -> service.assign(claims.get(1)));
            assertThat(List.of(first.get(), second.get())).allMatch(java.util.Optional::isEmpty);
        } finally {
            results.shutdownNow();
        }
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, fixture.workerId())).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_attempt WHERE task_instance_id IN (?, ?)",
                Integer.class, fixture.instanceId(), secondInstance)).isZero();
    }

    @Test
    void concurrentReservationsForOneFreeSlotCreateOnlyOneAttemptAndDispatch() throws Exception {
        Fixture fixture = insertFixture(1, 0);
        DispatchService service = service();
        UUID secondInstance = insertReadyInstance(fixture.definitionId(), fixture.versionId());
        List<TaskInstanceRepository.ClaimedTaskInstance> claims =
                taskInstances().claimReadyBatch(Instant.now(), 2);

        var executorService = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = executorService.submit(() -> service.assign(claims.get(0)));
            var second = executorService.submit(() -> service.assign(claims.get(1)));
            assertThat(List.of(first.get(), second.get()))
                    .filteredOn(java.util.Optional::isPresent).hasSize(1);
        } finally {
            executorService.shutdownNow();
        }
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reserved_capacity FROM sr_worker WHERE id = ?",
                Integer.class, fixture.workerId())).isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_attempt WHERE task_instance_id IN (?, ?)",
                Integer.class, fixture.instanceId(), secondInstance)).isOne();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_dispatch", Integer.class)).isOne();
    }

    private DispatchService service() {
        OutboxRepository outbox = new OutboxRepository(jdbc, objectMapper);
        return new DispatchService(
                new CapacityReservationService(jdbc, transactions, outbox),
                new PowerOfTwoChoicesRouter(new java.util.Random(3)),
                new ExecutorHttpClient(objectMapper), jdbc, transactions, outbox,
                Duration.ofSeconds(30), Duration.ofMillis(10));
    }

    private TaskInstanceRepository taskInstances() {
        return new TaskInstanceRepository(
                jdbc, transactions, new OutboxRepository(jdbc, objectMapper), objectMapper);
    }

    private Fixture insertFixture(int capacity, int reportedActive) {
        DefinitionFixture definition = definitionFixture();
        UUID instanceId = insertReadyInstance(definition.definitionId(), definition.versionId());
        UUID workerId = insertWorker(definition.applicationId(), capacity, reportedActive);
        UUID epoch = jdbcTemplate.queryForObject(
                "SELECT worker_epoch FROM sr_worker WHERE id = ?", UUID.class, workerId);
        return new Fixture(definition.applicationId(), definition.definitionId(),
                definition.versionId(), instanceId, workerId, epoch);
    }

    private UUID insertReadyInstance(UUID definitionId, UUID versionId) {
        UUID instanceId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_task_instance(
                    id, task_definition_id, definition_version_id, status,
                    scheduled_at, next_run_at, configuration_snapshot, payload)
                VALUES (?, ?, ?, 'READY', clock_timestamp(), clock_timestamp(),
                    '{}'::jsonb, '{"orderId":42}'::jsonb)
                """, instanceId, definitionId, versionId);
        return instanceId;
    }

    private UUID insertWorker(UUID applicationId, int capacity, int reportedActive) {
        UUID workerId = UUID.randomUUID();
        UUID workerEpoch = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO sr_worker(
                    id, application_id, worker_key, worker_epoch, pod_name, pod_ip,
                    executor_port, status, max_concurrency, reserved_capacity,
                    reported_active_count, handlers, lease_expires_at)
                VALUES (?, ?, ?, ?, ?, '127.0.0.1', ?, 'READY', ?, 0, ?,
                    '[{"name":"archiveOrders"}]'::jsonb,
                    clock_timestamp() + interval '10 minutes')
                """, workerId, applicationId, "worker-" + workerId, workerEpoch,
                "pod-" + workerId, executor.port(), capacity, reportedActive);
        return workerId;
    }

    private void stubAck(String status, UUID workerId, UUID workerEpoch) {
        executor.stubFor(post(urlEqualTo(EXECUTE_PATH)).willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withTransformers("response-template")
                .withBody("""
                        {
                          "dispatchId": "{{jsonPath request.body '$.dispatchId'}}",
                          "attemptId": "{{jsonPath request.body '$.attemptId'}}",
                          "workerId": "%s",
                          "workerEpoch": "%s",
                          "status": "%s",
                          "message": "explicit"
                        }
                        """.formatted(workerId, workerEpoch, status))));
    }

    private JsonNode read(String body) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private record Fixture(
            UUID applicationId,
            UUID definitionId,
            UUID versionId,
            UUID instanceId,
            UUID workerId,
            UUID workerEpoch) {
    }
}
