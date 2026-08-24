package com.staterelay.server.dag.dispatch;

import com.staterelay.contract.protocol.DispatchAck;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** 基于 PostgreSQL 的统一 DAG 调度存储实现。 */
@Repository
public class JdbcDagDispatchStore implements DagDispatchStore {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public JdbcDagDispatchStore(
            NamedParameterJdbcTemplate jdbc, TransactionTemplate transactions) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    @Override
    public Optional<Assignment> reserve(ReservationRequest request, Instant now) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(now, "now");
        return Objects.requireNonNull(transactions.execute(status -> reserveInTransaction(request, now)));
    }

    private Optional<Assignment> reserveInTransaction(ReservationRequest request, Instant now) {
        List<NodeFence> fences = jdbc.query("""
                SELECT node.current_attempt_id, node.dispatch_generation, node.dispatch_token
                FROM sr_dag_node_instance node
                JOIN sr_dag_instance dag ON dag.id = node.dag_instance_id
                WHERE node.id = :nodeInstanceId
                  AND node.dag_instance_id = :dagInstanceId
                  AND node.status = 20
                  AND dag.status = 10
                FOR UPDATE OF node
                """, new MapSqlParameterSource()
                .addValue("nodeInstanceId", request.getNodeInstanceId())
                .addValue("dagInstanceId", request.getDagInstanceId()),
                (resultSet, rowNumber) -> new NodeFence(
                        resultSet.getObject("current_attempt_id", Long.class),
                        resultSet.getObject("dispatch_generation", Long.class),
                        resultSet.getString("dispatch_token")));
        if (fences.isEmpty()) {
            return Optional.empty();
        }
        NodeFence fence = fences.get(0);
        if (fence.getCurrentAttemptId() != null) {
            Optional<Assignment> existing = findCurrentUncertain(request, fence.getCurrentAttemptId());
            if (existing.isPresent()) {
                return existing;
            }
        }

        Optional<Worker> selected = lockEligibleWorker(request);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        Worker worker = selected.get();
        int reserved = jdbc.update("""
                UPDATE sr_worker
                SET reserved_capacity = reserved_capacity + 1,
                    updated_at = clock_timestamp()
                WHERE id = :workerId
                  AND worker_epoch = :workerEpoch
                  AND status = 'READY'
                  AND lease_expires_at > clock_timestamp()
                  AND GREATEST(reserved_capacity, reported_active_count) < max_concurrency
                """, new MapSqlParameterSource()
                    .addValue("workerId", worker.getWorkerId())
                    .addValue("workerEpoch", worker.getWorkerEpoch()));
        if (reserved != 1) {
            return Optional.empty();
        }

        Integer attemptNo = jdbc.queryForObject("""
                SELECT COALESCE(MAX(attempt_no), 0) + 1
                FROM sr_dag_node_attempt
                WHERE node_instance_id = :nodeInstanceId
                """, new MapSqlParameterSource("nodeInstanceId", request.getNodeInstanceId()),
                Integer.class);
        long dispatchGeneration = fence.getDispatchGeneration() == null
                ? 1L : fence.getDispatchGeneration() + 1L;
        String dispatchToken = UUID.randomUUID().toString();
        String requestId = UUID.randomUUID().toString();
        Instant leaseExpiresAt = now.plus(request.getAttemptLease());
        Instant executionDeadlineAt = now.plus(request.getExecutionTimeout());
        Long attemptId = jdbc.queryForObject("""
                INSERT INTO sr_dag_node_attempt(
                    dag_instance_id, node_instance_id, attempt_no, request_id, request_checksum,
                    algorithm_code, worker_id, worker_address, worker_epoch, status, request_json,
                    attempt_lease_version, attempt_lease_expire_time, execution_deadline_at,
                    dispatch_generation, dispatch_token, transport_generation, transport_attempts,
                    next_dispatch_at, dispatched_at, created_at, updated_at)
                VALUES (
                    :dagInstanceId, :nodeInstanceId, :attemptNo, :requestId, :requestChecksum,
                    :executionCode, :workerIdText, :workerAddress, :workerEpochText, 10,
                    CAST(:requestJson AS jsonb), 1, :leaseExpiresAt, :executionDeadlineAt,
                    :dispatchGeneration, :dispatchToken, 0, 0, :now, :now, :now, :now)
                RETURNING id
                """, new MapSqlParameterSource()
                .addValue("dagInstanceId", request.getDagInstanceId())
                .addValue("nodeInstanceId", request.getNodeInstanceId())
                .addValue("attemptNo", attemptNo)
                .addValue("requestId", requestId)
                .addValue("requestChecksum", request.getRequestChecksum())
                .addValue("executionCode", request.getExecutionCode())
                .addValue("workerIdText", worker.getWorkerId().toString())
                .addValue("workerAddress", worker.getAddress())
                .addValue("workerEpochText", worker.getWorkerEpoch().toString())
                .addValue("requestJson", request.getRequestJson())
                .addValue("leaseExpiresAt", timestamp(leaseExpiresAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("executionDeadlineAt", timestamp(executionDeadlineAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("dispatchGeneration", dispatchGeneration)
                .addValue("dispatchToken", dispatchToken)
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE),
                Long.class);
        int fenced = jdbc.update("""
                UPDATE sr_dag_node_instance
                SET current_attempt_id = :attemptId,
                    current_attempt_no = :attemptNo,
                    dispatch_generation = :dispatchGeneration,
                    dispatch_token = :dispatchToken,
                    updated_at = :now
                WHERE id = :nodeInstanceId
                  AND dag_instance_id = :dagInstanceId
                  AND status = 20
                """, new MapSqlParameterSource()
                .addValue("attemptId", attemptId)
                .addValue("attemptNo", attemptNo)
                .addValue("dispatchGeneration", dispatchGeneration)
                .addValue("dispatchToken", dispatchToken)
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("nodeInstanceId", request.getNodeInstanceId())
                .addValue("dagInstanceId", request.getDagInstanceId()));
        if (fenced != 1) {
            throw new IllegalStateException("Node current-attempt fence changed during reservation");
        }
        return Optional.of(new Assignment(
                request.getDagInstanceId(), request.getNodeInstanceId(), request.getNodeCode(), attemptId,
                attemptNo, requestId, request.getRequestChecksum(), dispatchGeneration,
                dispatchToken, 1L, worker.getWorkerId().toString(), worker.getWorkerEpoch().toString(),
                worker.getAddress(), request.getExecutorGroupCode(), request.getExecutionCode(),
                request.getRequestJson(), request.getAttemptLease(), now));
    }

    private Optional<Assignment> findCurrentUncertain(
            ReservationRequest request, Long currentAttemptId) {
        List<Assignment> assignments = jdbc.query("""
                SELECT attempt.*, node.node_id
                FROM sr_dag_node_attempt attempt
                JOIN sr_dag_node_instance node ON node.id = attempt.node_instance_id
                WHERE attempt.id = :attemptId
                  AND attempt.node_instance_id = :nodeInstanceId
                  AND attempt.dag_instance_id = :dagInstanceId
                  AND attempt.status = 10
                  AND attempt.request_checksum = :requestChecksum
                  AND node.current_attempt_id = attempt.id
                  AND node.dispatch_generation = attempt.dispatch_generation
                  AND node.dispatch_token = attempt.dispatch_token
                """, new MapSqlParameterSource()
                .addValue("attemptId", currentAttemptId)
                .addValue("nodeInstanceId", request.getNodeInstanceId())
                .addValue("dagInstanceId", request.getDagInstanceId())
                .addValue("requestChecksum", request.getRequestChecksum()),
                (resultSet, rowNumber) -> assignment(
                        resultSet, request.getExecutorGroupCode(), request.getAttemptLease()));
        return assignments.stream().findFirst();
    }

    private Optional<Worker> lockEligibleWorker(ReservationRequest request) {
        List<Worker> workers = jdbc.query("""
                SELECT worker.id, worker.worker_epoch, host(worker.pod_ip) AS host,
                    worker.executor_port
                FROM sr_worker worker
                JOIN sr_application application ON application.id = worker.application_id
                WHERE application.name = :executorGroupCode
                  AND application.environment = 'default'
                  AND worker.status = 'READY'
                  AND worker.lease_expires_at > clock_timestamp()
                  AND GREATEST(worker.reserved_capacity, worker.reported_active_count)
                      < worker.max_concurrency
                  AND EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(worker.handlers) handler
                      WHERE (
                          :algorithmProtocol
                          AND handler ->> 'algorithmCode' = :executionCode
                          AND handler ->> 'contractVersion' = :contractVersion
                          AND (NOT :requiresChecksum
                              OR handler ->> 'contractChecksum' = :contractChecksum)
                      ) OR (
                          NOT :algorithmProtocol
                          AND handler ->> 'name' = :executionCode
                      ))
                ORDER BY GREATEST(worker.reserved_capacity, worker.reported_active_count), worker.id
                LIMIT 1
                FOR UPDATE OF worker SKIP LOCKED
                """, new MapSqlParameterSource()
                .addValue("executorGroupCode", request.getExecutorGroupCode())
                .addValue("algorithmProtocol", request.isAlgorithmProtocol())
                .addValue("executionCode", request.getExecutionCode())
                .addValue("contractVersion", request.getContractVersion())
                .addValue("requiresChecksum", hasText(request.getContractChecksum()))
                .addValue("contractChecksum", request.getContractChecksum()),
                (resultSet, rowNumber) -> {
                    String host = resultSet.getString("host");
                    if (host.contains(":")) {
                        host = "[" + host + "]";
                    }
                    return new Worker(
                            resultSet.getObject("id", UUID.class),
                            resultSet.getObject("worker_epoch", UUID.class),
                            host + ":" + resultSet.getInt("executor_port"));
                });
        return workers.stream().findFirst();
    }

    @Override
    public Optional<SendClaim> claimSend(
            Assignment candidate, Instant now, Instant leaseExpiresAt) {
        Objects.requireNonNull(candidate, "candidate");
        List<Long> generations = Objects.requireNonNull(transactions.execute(status -> jdbc.query("""
                UPDATE sr_dag_node_attempt attempt
                SET transport_generation = transport_generation + 1,
                    transport_attempts = transport_attempts + 1,
                    next_dispatch_at = :leaseExpiresAt,
                    last_dispatch_error = NULL,
                    updated_at = :now
                WHERE attempt.id = :attemptId
                  AND attempt.dag_instance_id = :dagInstanceId
                  AND attempt.node_instance_id = :nodeInstanceId
                  AND attempt.attempt_no = :attemptNo
                  AND attempt.status = 10
                  AND attempt.request_id = :requestId
                  AND attempt.request_checksum = :requestChecksum
                  AND attempt.dispatch_generation = :dispatchGeneration
                  AND attempt.dispatch_token = :dispatchToken
                  AND attempt.attempt_lease_version = :attemptLeaseVersion
                  AND attempt.worker_id = :workerId
                  AND attempt.worker_epoch = :workerEpoch
                  AND attempt.next_dispatch_at IS NOT NULL
                  AND attempt.next_dispatch_at <= :now
                  AND EXISTS (
                      SELECT 1
                      FROM sr_dag_node_instance node
                      JOIN sr_dag_instance dag ON dag.id = node.dag_instance_id
                      WHERE node.id = :nodeInstanceId
                        AND node.dag_instance_id = :dagInstanceId
                        AND node.status = 20
                        AND node.current_attempt_id = attempt.id
                        AND node.current_attempt_no = attempt.attempt_no
                        AND node.dispatch_generation = attempt.dispatch_generation
                        AND node.dispatch_token = attempt.dispatch_token
                        AND dag.status = 10)
                RETURNING transport_generation
                """, sendFence(candidate)
                .addValue("dagInstanceId", candidate.getDagInstanceId())
                .addValue("nodeInstanceId", candidate.getNodeInstanceId())
                .addValue("attemptNo", candidate.getAttemptNo())
                .addValue("requestChecksum", candidate.getRequestChecksum())
                .addValue("attemptLeaseVersion", candidate.getAttemptLeaseVersion())
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("leaseExpiresAt", timestamp(leaseExpiresAt), Types.TIMESTAMP_WITH_TIMEZONE),
                (resultSet, rowNumber) -> resultSet.getLong("transport_generation"))));
        return generations.stream().findFirst().map(generation -> new SendClaim(candidate, generation));
    }

    @Override
    public boolean markAccepted(SendClaim claim, Instant now) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            Assignment assignment = claim.getAssignment();
            int accepted = jdbc.update("""
                    UPDATE sr_dag_node_attempt
                    SET accepted_at = COALESCE(accepted_at, :now),
                        next_dispatch_at = NULL,
                        last_dispatch_error = NULL,
                        updated_at = :now
                    WHERE id = :attemptId
                      AND status = 10
                      AND request_id = :requestId
                      AND dispatch_generation = :dispatchGeneration
                      AND dispatch_token = :dispatchToken
                      AND worker_id = :workerId
                      AND worker_epoch = :workerEpoch
                    """, sendFence(assignment)
                    .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE));
            if (accepted != 1) {
                return false;
            }
            int running = jdbc.update("""
                    UPDATE sr_dag_node_instance node
                    SET status = 40,
                        dispatch_owner = NULL,
                        dispatch_lease_expire_time = NULL,
                        started_at = COALESCE(started_at, :now),
                        updated_at = :now
                    WHERE node.id = :nodeInstanceId
                      AND node.dag_instance_id = :dagInstanceId
                      AND node.status = 20
                      AND node.current_attempt_id = :attemptId
                      AND node.current_attempt_no = :attemptNo
                      AND node.dispatch_generation = :dispatchGeneration
                      AND node.dispatch_token = :dispatchToken
                      AND EXISTS (
                          SELECT 1 FROM sr_dag_instance dag
                          WHERE dag.id = node.dag_instance_id AND dag.status = 10)
                    """, sendFence(assignment)
                .addValue("dagInstanceId", assignment.getDagInstanceId())
                .addValue("nodeInstanceId", assignment.getNodeInstanceId())
                .addValue("attemptNo", assignment.getAttemptNo())
                    .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE));
            if (running != 1) {
                status.setRollbackOnly();
                return false;
            }
            return true;
        }));
    }

    @Override
    public boolean markRejected(
            SendClaim claim,
            DispatchAck.AckStatus ackStatus,
            String message,
            Instant now,
            Instant nextScheduleTime) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            Assignment assignment = claim.getAssignment();
            MapSqlParameterSource rejectionFence = sendFence(assignment)
                .addValue("dagInstanceId", assignment.getDagInstanceId())
                .addValue("nodeInstanceId", assignment.getNodeInstanceId())
                .addValue("attemptNo", assignment.getAttemptNo())
                .addValue("requestChecksum", assignment.getRequestChecksum())
                .addValue("attemptLeaseVersion", assignment.getAttemptLeaseVersion())
                .addValue("transportGeneration", claim.getTransportGeneration());
            List<Boolean> releases = jdbc.query("""
                    SELECT attempt.capacity_released_at IS NULL AS release_required
                    FROM sr_dag_node_attempt attempt
                    WHERE attempt.id = :attemptId
                      AND attempt.dag_instance_id = :dagInstanceId
                      AND attempt.node_instance_id = :nodeInstanceId
                      AND attempt.attempt_no = :attemptNo
                      AND attempt.status = 10
                      AND attempt.request_id = :requestId
                      AND attempt.request_checksum = :requestChecksum
                      AND attempt.dispatch_generation = :dispatchGeneration
                      AND attempt.dispatch_token = :dispatchToken
                      AND attempt.attempt_lease_version = :attemptLeaseVersion
                      AND attempt.worker_id = :workerId
                      AND attempt.worker_epoch = :workerEpoch
                      AND attempt.transport_generation = :transportGeneration
                      AND attempt.next_dispatch_at IS NOT NULL
                    FOR UPDATE
                    """, rejectionFence,
                    (resultSet, rowNumber) -> resultSet.getBoolean("release_required"));
            if (releases.isEmpty()) {
                return false;
            }
            int failed = jdbc.update("""
                    UPDATE sr_dag_node_attempt
                    SET status = 50,
                        error_code = :errorCode,
                        error_message = :errorMessage,
                        capacity_released_at = COALESCE(capacity_released_at, :now),
                        next_dispatch_at = NULL,
                        finished_at = :now,
                        updated_at = :now
                    WHERE id = :attemptId
                      AND dag_instance_id = :dagInstanceId
                      AND node_instance_id = :nodeInstanceId
                      AND attempt_no = :attemptNo
                      AND status = 10
                      AND request_id = :requestId
                      AND request_checksum = :requestChecksum
                      AND dispatch_generation = :dispatchGeneration
                      AND dispatch_token = :dispatchToken
                      AND attempt_lease_version = :attemptLeaseVersion
                      AND worker_id = :workerId
                      AND worker_epoch = :workerEpoch
                      AND transport_generation = :transportGeneration
                    """, rejectionFence
                    .addValue("errorCode", "DISPATCH_" + ackStatus.name())
                    .addValue("errorMessage", message)
                    .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE));
            jdbc.update("""
                    UPDATE sr_dag_node_instance node
                    SET status = 10,
                        schedule_fail_count = schedule_fail_count + 1,
                        last_schedule_error_code = :errorCode,
                        last_schedule_error_message = :errorMessage,
                        dispatch_owner = NULL,
                        dispatch_lease_expire_time = NULL,
                        next_schedule_time = :nextScheduleTime,
                        updated_at = :now
                    WHERE node.id = :nodeInstanceId
                      AND node.dag_instance_id = :dagInstanceId
                      AND node.status = 20
                      AND node.current_attempt_id = :attemptId
                      AND node.current_attempt_no = :attemptNo
                      AND node.dispatch_generation = :dispatchGeneration
                      AND node.dispatch_token = :dispatchToken
                      AND EXISTS (
                          SELECT 1 FROM sr_dag_instance dag
                          WHERE dag.id = node.dag_instance_id AND dag.status = 10)
                    """, sendFence(assignment)
                .addValue("dagInstanceId", assignment.getDagInstanceId())
                .addValue("nodeInstanceId", assignment.getNodeInstanceId())
                .addValue("attemptNo", assignment.getAttemptNo())
                    .addValue("errorCode", "DISPATCH_" + ackStatus.name())
                    .addValue("errorMessage", message)
                    .addValue("nextScheduleTime", timestamp(nextScheduleTime), Types.TIMESTAMP_WITH_TIMEZONE)
                    .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE));
            int released = releases.get(0) ? jdbc.update("""
                    UPDATE sr_worker
                    SET reserved_capacity = GREATEST(0, reserved_capacity - 1),
                        updated_at = clock_timestamp()
                    WHERE id = :workerId AND worker_epoch = :workerEpoch
                    """, new MapSqlParameterSource()
                    .addValue("workerId", UUID.fromString(assignment.getWorkerId()))
                    .addValue("workerEpoch", UUID.fromString(assignment.getWorkerEpoch()))) : 1;
            if (failed != 1 || released != 1) {
                status.setRollbackOnly();
                return false;
            }
            return true;
        }));
    }

    @Override
    public boolean markUncertain(SendClaim claim, String error, Instant nextTransportAt) {
        Assignment assignment = claim.getAssignment();
        return Boolean.TRUE.equals(transactions.execute(status -> jdbc.update("""
                UPDATE sr_dag_node_attempt
                SET last_dispatch_error = :error,
                    next_dispatch_at = :nextTransportAt,
                    updated_at = clock_timestamp()
                WHERE id = :attemptId
                  AND status = 10
                  AND request_id = :requestId
                  AND dispatch_generation = :dispatchGeneration
                  AND dispatch_token = :dispatchToken
                  AND worker_id = :workerId
                  AND worker_epoch = :workerEpoch
                  AND transport_generation = :transportGeneration
                  AND next_dispatch_at IS NOT NULL
                """, sendFence(assignment)
                .addValue("transportGeneration", claim.getTransportGeneration())
                .addValue("error", error)
                .addValue("nextTransportAt", timestamp(nextTransportAt), Types.TIMESTAMP_WITH_TIMEZONE)) == 1));
    }

    @Override
    public List<Assignment> findDueUncertain(Instant now, int batchSize) {
        return jdbc.query("""
                SELECT attempt.*, node.node_id, application.name AS application_name
                FROM sr_dag_node_attempt attempt
                JOIN sr_dag_node_instance node ON node.id = attempt.node_instance_id
                JOIN sr_dag_instance dag ON dag.id = attempt.dag_instance_id
                JOIN sr_worker worker ON worker.id::text = attempt.worker_id
                    AND worker.worker_epoch::text = attempt.worker_epoch
                JOIN sr_application application ON application.id = worker.application_id
                WHERE attempt.status = 10
                  AND attempt.next_dispatch_at IS NOT NULL
                  AND attempt.next_dispatch_at <= :now
                  AND node.status = 20
                  AND node.current_attempt_id = attempt.id
                  AND node.current_attempt_no = attempt.attempt_no
                  AND node.dispatch_generation = attempt.dispatch_generation
                  AND node.dispatch_token = attempt.dispatch_token
                  AND dag.status = 10
                ORDER BY attempt.next_dispatch_at, attempt.id
                LIMIT :batchSize
                """, new MapSqlParameterSource()
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("batchSize", batchSize),
                (resultSet, rowNumber) -> assignment(
                        resultSet, resultSet.getString("application_name"), null));
    }

    private Assignment assignment(
            ResultSet resultSet, String application, Duration fallbackLease) throws SQLException {
        Instant dispatchedAt = instant(resultSet, "dispatched_at");
        Instant leaseExpiresAt = instant(resultSet, "attempt_lease_expire_time");
        Duration lease = dispatchedAt != null && leaseExpiresAt != null
                ? Duration.between(dispatchedAt, leaseExpiresAt) : fallbackLease;
        if (lease == null || lease.isZero() || lease.isNegative()) {
            lease = Duration.ofSeconds(30);
        }
        return new Assignment(
                resultSet.getLong("dag_instance_id"), resultSet.getLong("node_instance_id"),
                resultSet.getString("node_id"), resultSet.getLong("id"),
                resultSet.getInt("attempt_no"), resultSet.getString("request_id"),
                resultSet.getString("request_checksum"), resultSet.getLong("dispatch_generation"),
                resultSet.getString("dispatch_token"), resultSet.getLong("attempt_lease_version"),
                resultSet.getString("worker_id"), resultSet.getString("worker_epoch"),
                resultSet.getString("worker_address"), application,
                resultSet.getString("algorithm_code"), resultSet.getString("request_json"),
                lease, dispatchedAt);
    }

    private MapSqlParameterSource sendFence(Assignment assignment) {
        return new MapSqlParameterSource()
                .addValue("attemptId", assignment.getAttemptId())
                .addValue("requestId", assignment.getRequestId())
                .addValue("dispatchGeneration", assignment.getDispatchGeneration())
                .addValue("dispatchToken", assignment.getDispatchToken())
                .addValue("workerId", assignment.getWorkerId())
                .addValue("workerEpoch", assignment.getWorkerEpoch());
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static OffsetDateTime timestamp(Instant value) {
        return value == null ? null : value.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet resultSet, String column) throws SQLException {
        OffsetDateTime value = resultSet.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    @Data
    @AllArgsConstructor
    private static class NodeFence {
        private final Long currentAttemptId;
        private final Long dispatchGeneration;
        private final String dispatchToken;
    }

    @Data
    @AllArgsConstructor
    private static class Worker {
        private final UUID workerId;
        private final UUID workerEpoch;
        private final String address;
    }
}
