package com.staterelay.server.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.persistence.OutboxRepository;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Service
public final class WorkerService {

    private static final Duration DEFAULT_WORKER_LEASE = Duration.ofSeconds(35);
    private static final Duration MIN_WORKER_LEASE = Duration.ofSeconds(10);
    private static final Duration MAX_WORKER_LEASE = Duration.ofMinutes(5);
    private static final Set<String> ACTIVE_ATTEMPT_STATUSES =
            Set.of("ASSIGNED", "ACCEPTED", "RUNNING");
    private static final Set<String> HEARTBEAT_STATUSES = Set.of("READY", "DRAINING");

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final Duration workerLease;

    public WorkerService(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper) {
        this(jdbc, transactions, outbox, objectMapper, DEFAULT_WORKER_LEASE);
    }

    public WorkerService(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper,
            Duration workerLease) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.workerLease = requireLeaseDuration(workerLease);
    }

    /**
     * Registers one process lifetime and expires all active Attempt leases from older
     * process lifetimes on the same Pod. The server clock owns both expiry values.
     */
    public WorkerRegistration register(RegistrationRequest request) {
        validateRegistration(request);
        return Objects.requireNonNull(transactions.execute(status -> {
            UUID applicationId = requireApplication(request.application(), request.environment());
            jdbc.query(
                    "SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))",
                    new MapSqlParameterSource("identity",
                            applicationId + "/" + request.podName()),
                    resultSet -> null);
            assertWorkerIdNotReused(request);

            List<WorkerFence> replaced = jdbc.query("""
                    SELECT id, worker_epoch
                    FROM sr_worker
                    WHERE application_id = :applicationId
                      AND pod_name = :podName
                      AND id <> :workerId
                      AND status <> 'OFFLINE'
                    ORDER BY id
                    """, new MapSqlParameterSource()
                    .addValue("applicationId", applicationId)
                    .addValue("podName", request.podName())
                    .addValue("workerId", request.workerId()),
                    (resultSet, rowNumber) -> new WorkerFence(
                            resultSet.getObject("id", UUID.class),
                            resultSet.getObject("worker_epoch", UUID.class)));
            for (WorkerFence oldWorker : replaced) {
                expireOldProcess(oldWorker);
            }

            OffsetDateTime leaseExpiresAt = serverLeaseExpiry(request.workerLease());
            int updated = jdbc.update("""
                    UPDATE sr_worker
                    SET pod_name = :podName, pod_ip = CAST(:podIp AS inet),
                        executor_port = :executorPort, status = 'READY',
                        max_concurrency = :maxConcurrency,
                        queue_capacity = :queueCapacity, queue_depth = 0,
                        reported_active_count = 0, handlers = CAST(:handlers AS jsonb),
                        starter_version = :starterVersion,
                        lease_expires_at = :leaseExpiresAt,
                        last_heartbeat_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :workerId AND application_id = :applicationId
                      AND worker_epoch = :workerEpoch
                    """, workerParameters(request, applicationId, leaseExpiresAt));
            if (updated == 0) {
                jdbc.update("""
                        INSERT INTO sr_worker(
                            id, application_id, worker_key, worker_epoch, pod_name, pod_ip,
                            executor_port, status, max_concurrency, queue_capacity,
                            queue_depth, handlers, starter_version, lease_expires_at,
                            last_heartbeat_at)
                        VALUES (:workerId, :applicationId, :workerKey, :workerEpoch,
                            :podName, CAST(:podIp AS inet), :executorPort, 'READY',
                            :maxConcurrency, :queueCapacity, 0, CAST(:handlers AS jsonb),
                            :starterVersion, :leaseExpiresAt, clock_timestamp())
                        """, workerParameters(request, applicationId, leaseExpiresAt)
                        .addValue("workerKey", request.workerId().toString()));
            }
            outbox.append("WORKER", request.workerId(), "WORKER_REGISTERED",
                    JsonNodeFactory.instance.objectNode()
                            .put("workerId", request.workerId().toString())
                            .put("workerEpoch", request.workerEpoch().toString())
                            .put("podName", request.podName())
                            .put("status", "READY"));
            return new WorkerRegistration(
                    request.workerId(), request.workerEpoch(), "READY",
                    leaseExpiresAt.toInstant());
        }));
    }

    /**
     * Atomically records the Worker report and renews every reported active execution.
     * Any stale identity or Attempt fence rolls back all renewals and the Worker report.
     */
    public void heartbeat(UUID workerId, HeartbeatRequest request) {
        requireMatchingWorkerId(workerId, request.workerId());
        validateHeartbeat(request);
        Boolean accepted = transactions.execute(status -> {
            UUID liveWorkerId = requireLiveWorker(
                    workerId, request.workerEpoch(), request.application());
            OffsetDateTime leaseExpiresAt = serverLeaseExpiry(request.workerLease());
            for (ExecutionLease lease : request.activeLeases()) {
                if (!request.workerEpoch().equals(lease.workerEpoch())) {
                    throw conflict("active execution carries a stale Worker epoch");
                }
                int renewed = jdbc.update("""
                        UPDATE sr_task_attempt
                        SET lease_expires_at = :leaseExpiresAt,
                            updated_at = clock_timestamp()
                        WHERE id = :attemptId
                          AND lease_version = :leaseVersion
                          AND worker_id = :workerId
                          AND worker_epoch = :workerEpoch
                          AND status IN (:activeStatuses)
                        """, new MapSqlParameterSource()
                        .addValue("leaseExpiresAt", leaseExpiresAt, Types.TIMESTAMP_WITH_TIMEZONE)
                        .addValue("attemptId", lease.attemptId())
                        .addValue("leaseVersion", lease.leaseVersion())
                        .addValue("workerId", liveWorkerId)
                        .addValue("workerEpoch", request.workerEpoch())
                        .addValue("activeStatuses", ACTIVE_ATTEMPT_STATUSES));
                if (renewed != 1) {
                    throw conflict("active execution lease is stale");
                }
            }
            List<UUID> lockedWorkers = jdbc.query("""
                    SELECT id
                    FROM sr_worker
                    WHERE id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status <> 'OFFLINE'
                    FOR UPDATE
                    """, new MapSqlParameterSource()
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", request.workerEpoch()),
                    (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
            if (lockedWorkers.isEmpty()) {
                throw conflict("Worker identity is stale");
            }
            int unreleasedAttempts = jdbc.queryForObject("""
                    SELECT count(*)::integer
                    FROM sr_task_attempt
                    WHERE worker_id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND status IN (:activeStatuses)
                      AND capacity_released_at IS NULL
                    """, new MapSqlParameterSource()
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", request.workerEpoch())
                    .addValue("activeStatuses", ACTIVE_ATTEMPT_STATUSES), Integer.class);
            int knownReportedAttempts = Math.toIntExact(request.activeLeases().stream()
                    .map(ExecutionLease::attemptId)
                    .distinct()
                    .count());
            int reported = jdbc.update("""
                    UPDATE sr_worker
                    SET status = CASE WHEN status = 'DRAINING' THEN 'DRAINING'
                                      ELSE :reportedStatus END,
                        reserved_capacity = LEAST(:maxConcurrency,
                            :unreleasedAttempts + GREATEST(
                                0, :activeCount - :knownReportedAttempts)),
                        reported_active_count = :activeCount,
                        queue_depth = :queueDepth,
                        max_concurrency = :maxConcurrency,
                        queue_capacity = :queueCapacity,
                        handlers = CAST(:handlers AS jsonb),
                        starter_version = :starterVersion,
                        lease_expires_at = :leaseExpiresAt,
                        last_heartbeat_at = clock_timestamp(), updated_at = clock_timestamp()
                    WHERE id = :workerId AND worker_epoch = :workerEpoch
                      AND status <> 'OFFLINE'
                    """, new MapSqlParameterSource()
                    .addValue("reportedStatus", request.status())
                    .addValue("activeCount", request.activeCount())
                    .addValue("unreleasedAttempts", unreleasedAttempts)
                    .addValue("knownReportedAttempts", knownReportedAttempts)
                    .addValue("queueDepth", request.queueDepth())
                    .addValue("maxConcurrency", request.maxConcurrency())
                    .addValue("queueCapacity", request.queueCapacity())
                    .addValue("handlers", write(request.handlers()))
                    .addValue("starterVersion", request.starterVersion())
                    .addValue("leaseExpiresAt", leaseExpiresAt, Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", request.workerEpoch()));
            if (reported != 1) {
                throw conflict("Worker identity is stale");
            }
            return true;
        });
        if (!Boolean.TRUE.equals(accepted)) {
            throw new IllegalStateException("heartbeat transaction returned no result");
        }
    }

    /** Marks only the current live Worker epoch as draining. */
    public void drain(UUID workerId, EpochRequest request) {
        changeStatus(workerId, request.workerEpoch(), "DRAINING", "WORKER_DRAINING");
    }

    /** Offlines only the current live Worker epoch; stale deletion cannot affect a replacement. */
    public void delete(UUID workerId, EpochRequest request) {
        changeStatus(workerId, request.workerEpoch(), "OFFLINE", "WORKER_OFFLINE");
    }

    private void changeStatus(
            UUID workerId, UUID workerEpoch, String targetStatus, String eventType) {
        Boolean changed = transactions.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE sr_worker
                    SET status = :targetStatus,
                        lease_expires_at = CASE WHEN :targetStatus = 'OFFLINE'
                            THEN clock_timestamp() ELSE lease_expires_at END,
                        updated_at = clock_timestamp()
                    WHERE id = :workerId AND worker_epoch = :workerEpoch
                      AND status <> 'OFFLINE'
                    """, new MapSqlParameterSource()
                    .addValue("targetStatus", targetStatus)
                    .addValue("workerId", workerId)
                    .addValue("workerEpoch", workerEpoch));
            if (updated != 1) {
                throw conflict("Worker identity is stale");
            }
            outbox.append("WORKER", workerId, eventType,
                    JsonNodeFactory.instance.objectNode()
                            .put("workerId", workerId.toString())
                            .put("workerEpoch", workerEpoch.toString())
                            .put("status", targetStatus));
            return true;
        });
        if (!Boolean.TRUE.equals(changed)) {
            throw new IllegalStateException("Worker status transaction returned no result");
        }
    }

    private void expireOldProcess(WorkerFence oldWorker) {
        jdbc.update("""
                UPDATE sr_task_attempt
                SET lease_expires_at = clock_timestamp(), updated_at = clock_timestamp()
                WHERE worker_id = :workerId AND worker_epoch = :workerEpoch
                  AND status IN (:activeStatuses)
                """, new MapSqlParameterSource()
                .addValue("workerId", oldWorker.workerId())
                .addValue("workerEpoch", oldWorker.workerEpoch())
                .addValue("activeStatuses", ACTIVE_ATTEMPT_STATUSES));
        int offlined = jdbc.update("""
                UPDATE sr_worker
                SET status = 'OFFLINE', lease_expires_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE id = :workerId AND worker_epoch = :workerEpoch
                  AND status <> 'OFFLINE'
                """, new MapSqlParameterSource()
                .addValue("workerId", oldWorker.workerId())
                .addValue("workerEpoch", oldWorker.workerEpoch()));
        if (offlined != 1) {
            throw conflict("replaced Worker identity changed during registration");
        }
        outbox.append("WORKER", oldWorker.workerId(), "WORKER_REPLACED",
                JsonNodeFactory.instance.objectNode()
                        .put("workerId", oldWorker.workerId().toString())
                        .put("workerEpoch", oldWorker.workerEpoch().toString())
                        .put("status", "OFFLINE"));
    }

    private UUID requireLiveWorker(UUID workerId, UUID workerEpoch, String application) {
        List<UUID> workers = jdbc.query("""
                SELECT worker.id
                FROM sr_worker worker
                JOIN sr_application application ON application.id = worker.application_id
                WHERE worker.id = :workerId
                  AND worker.worker_epoch = :workerEpoch
                  AND worker.status <> 'OFFLINE'
                  AND application.name = :application
                """, new MapSqlParameterSource()
                .addValue("workerId", workerId)
                .addValue("workerEpoch", workerEpoch)
                .addValue("application", application),
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
        if (workers.isEmpty()) {
            throw conflict("Worker identity is stale");
        }
        return workers.get(0);
    }

    private UUID requireApplication(String application, String environment) {
        List<UUID> applications = jdbc.query("""
                SELECT id FROM sr_application
                WHERE name = :application AND environment = :environment
                """, new MapSqlParameterSource()
                .addValue("application", application)
                .addValue("environment", environment),
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
        if (applications.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "application is not registered");
        }
        return applications.get(0);
    }

    private void assertWorkerIdNotReused(RegistrationRequest request) {
        List<UUID> epochs = jdbc.query("""
                SELECT worker_epoch FROM sr_worker WHERE id = :workerId
                """, new MapSqlParameterSource("workerId", request.workerId()),
                (resultSet, rowNumber) -> resultSet.getObject("worker_epoch", UUID.class));
        if (!epochs.isEmpty() && !epochs.get(0).equals(request.workerEpoch())) {
            throw conflict("Worker ID is already bound to another epoch");
        }
    }

    private MapSqlParameterSource workerParameters(
            RegistrationRequest request, UUID applicationId, OffsetDateTime leaseExpiresAt) {
        return new MapSqlParameterSource()
                .addValue("workerId", request.workerId())
                .addValue("workerEpoch", request.workerEpoch())
                .addValue("applicationId", applicationId)
                .addValue("podName", request.podName())
                .addValue("podIp", request.podIp())
                .addValue("executorPort", request.executorPort())
                .addValue("maxConcurrency", request.maxConcurrency())
                .addValue("queueCapacity", request.queueCapacity())
                .addValue("handlers", write(request.handlers()))
                .addValue("starterVersion", request.starterVersion())
                .addValue("leaseExpiresAt", leaseExpiresAt, Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private OffsetDateTime serverLeaseExpiry(Duration requestedLease) {
        Duration lease = requestedLease == null ? workerLease : requireLeaseDuration(requestedLease);
        Instant now = jdbc.queryForObject(
                        "SELECT clock_timestamp()",
                        new MapSqlParameterSource(), java.sql.Timestamp.class)
                .toInstant();
        return now.plus(lease).atOffset(ZoneOffset.UTC);
    }

    private String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private void validateRegistration(RegistrationRequest request) {
        Objects.requireNonNull(request, "request");
        requireText(request.application(), "application");
        requireText(request.environment(), "environment");
        Objects.requireNonNull(request.workerId(), "workerId");
        Objects.requireNonNull(request.workerEpoch(), "workerEpoch");
        requireText(request.podName(), "podName");
        requireText(request.podIp(), "podIp");
        requirePort(request.executorPort());
        requirePositive(request.maxConcurrency(), "maxConcurrency");
        requireNonNegative(request.queueCapacity(), "queueCapacity");
        if (request.workerLease() != null) {
            requireLeaseDuration(request.workerLease());
        }
        Objects.requireNonNull(request.handlers(), "handlers");
    }

    private void validateHeartbeat(HeartbeatRequest request) {
        Objects.requireNonNull(request, "request");
        requireText(request.application(), "application");
        Objects.requireNonNull(request.workerId(), "workerId");
        Objects.requireNonNull(request.workerEpoch(), "workerEpoch");
        if (!HEARTBEAT_STATUSES.contains(request.status())) {
            throw new IllegalArgumentException("heartbeat status must be READY or DRAINING");
        }
        requireNonNegative(request.activeCount(), "activeCount");
        requireNonNegative(request.queueDepth(), "queueDepth");
        requirePositive(request.maxConcurrency(), "maxConcurrency");
        requireNonNegative(request.queueCapacity(), "queueCapacity");
        if (request.workerLease() != null) {
            requireLeaseDuration(request.workerLease());
        }
        Objects.requireNonNull(request.handlers(), "handlers");
        Objects.requireNonNull(request.activeLeases(), "activeLeases");
    }

    private void requireMatchingWorkerId(UUID pathWorkerId, UUID bodyWorkerId) {
        if (!Objects.equals(pathWorkerId, bodyWorkerId)) {
            throw conflict("path Worker ID does not match heartbeat identity");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requirePort(int value) {
        if (value < 1 || value > 65_535) {
            throw new IllegalArgumentException("executorPort must be between 1 and 65535");
        }
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static Duration requireLeaseDuration(Duration value) {
        Objects.requireNonNull(value, "workerLease");
        if (value.compareTo(MIN_WORKER_LEASE) < 0
                || value.compareTo(MAX_WORKER_LEASE) > 0) {
            throw new IllegalArgumentException(
                    "workerLease must be between " + MIN_WORKER_LEASE
                            + " and " + MAX_WORKER_LEASE);
        }
        return value;
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static ResponseStatusException conflict(String reason) {
        return new ResponseStatusException(HttpStatus.CONFLICT, reason);
    }

    public record RegistrationRequest(
            String application,
            String environment,
            UUID workerId,
            UUID workerEpoch,
            String podName,
            String podIp,
            int executorPort,
            int maxConcurrency,
            int queueCapacity,
            Duration workerLease,
            String starterVersion,
            Set<HandlerMetadata> handlers) {
    }

    public record HeartbeatRequest(
            String application,
            UUID workerId,
            UUID workerEpoch,
            String status,
            int activeCount,
            int queueDepth,
            int maxConcurrency,
            int queueCapacity,
            Duration workerLease,
            String starterVersion,
            Set<HandlerMetadata> handlers,
            List<ExecutionLease> activeLeases) {
    }

    public record EpochRequest(UUID workerEpoch) {
    }

    public record HandlerMetadata(String name, String implementationType) {
    }

    public record ExecutionLease(UUID attemptId, long leaseVersion, UUID workerEpoch) {
    }

    public record WorkerRegistration(
            UUID workerId, UUID workerEpoch, String status, Instant leaseExpiresAt) {
    }

    private record WorkerFence(UUID workerId, UUID workerEpoch) {
    }
}
