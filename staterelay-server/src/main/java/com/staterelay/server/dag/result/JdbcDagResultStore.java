package com.staterelay.server.dag.result;

import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

/** 完整围栏 DAG 结果边界的 PostgreSQL 实现。 */
@Repository
public final class JdbcDagResultStore implements DagResultStore {

    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public JdbcDagResultStore(
            NamedParameterJdbcTemplate jdbc, TransactionTemplate transactions) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    @Override
    public Outcome accept(TerminalUpdate update) {
        Objects.requireNonNull(update, "update");
        WorkerExecutionContext context = requireContext(update.getContext());
        Outcome outcome = Objects.requireNonNull(transactions.execute(status -> {
            List<String> outcomes = jdbc.query("""
                    WITH eligible AS MATERIALIZED (
                        SELECT attempt.id AS attempt_id, node.id AS node_id,
                               dag.id AS dag_id, worker.id AS worker_id,
                               (:failureDelta = 1
                                   AND node.retry_count < COALESCE(algorithm.max_retry, 0)) AS retryable,
                               COALESCE(algorithm.retry_interval_seconds, 10) AS retry_interval_seconds
                        FROM sr_dag_node_attempt attempt
                        JOIN sr_dag_node_instance node ON node.id = attempt.node_instance_id
                        JOIN sr_dag_instance dag ON dag.id = node.dag_instance_id
                        LEFT JOIN sr_algorithm_definition algorithm
                               ON algorithm.algorithm_code = attempt.algorithm_code
                        JOIN sr_worker worker ON worker.id::text = attempt.worker_id
                                             AND worker.worker_epoch::text = attempt.worker_epoch
                        WHERE attempt.id = :attemptId
                          AND attempt.dag_instance_id = :dagInstanceId
                          AND attempt.node_instance_id = :nodeInstanceId
                          AND attempt.attempt_no = :attemptNo
                          AND attempt.request_id = :requestId
                          AND attempt.request_checksum = :requestChecksum
                          AND attempt.dispatch_generation = :dispatchGeneration
                          AND attempt.dispatch_token = :dispatchToken
                          AND attempt.attempt_lease_version = :attemptLeaseVersion
                          AND attempt.worker_id = :workerId
                          AND attempt.worker_epoch = :workerEpoch
                          AND attempt.status IN (10, 20, 30)
                          AND attempt.capacity_released_at IS NULL
                          AND node.node_id = :nodeCode
                          AND node.status = 40
                          AND node.current_attempt_id = attempt.id
                          AND node.current_attempt_no = attempt.attempt_no
                          AND node.dispatch_generation = attempt.dispatch_generation
                          AND node.dispatch_token = attempt.dispatch_token
                          AND dag.status = 10
                        FOR UPDATE OF attempt, node, dag, worker
                    ), updated_attempt AS (
                        UPDATE sr_dag_node_attempt attempt
                        SET status = :attemptStatus,
                            result_json = CAST(:resultJson AS jsonb), result_ref = :resultRef,
                            error_code = :errorCode, error_message = :errorMessage,
                            capacity_released_at = :now, finished_at = :now, updated_at = :now
                        FROM eligible
                        WHERE attempt.id = eligible.attempt_id
                        RETURNING attempt.id
                    ), updated_node AS (
                        UPDATE sr_dag_node_instance node
                        SET status = CASE WHEN eligible.retryable THEN 10 ELSE :nodeStatus END,
                            retry_count = node.retry_count
                                + CASE WHEN eligible.retryable THEN 1 ELSE 0 END,
                            result_json = CASE WHEN eligible.retryable THEN NULL
                                ELSE CAST(:resultJson AS jsonb) END,
                            result_ref = CASE WHEN eligible.retryable THEN NULL ELSE :resultRef END,
                            error_code = CASE WHEN eligible.retryable THEN NULL ELSE :errorCode END,
                            error_message = CASE WHEN eligible.retryable THEN NULL ELSE :errorMessage END,
                            dispatch_owner = CASE WHEN eligible.retryable THEN NULL
                                ELSE node.dispatch_owner END,
                            dispatch_lease_expire_time = CASE WHEN eligible.retryable THEN NULL
                                ELSE node.dispatch_lease_expire_time END,
                            next_schedule_time = CASE WHEN eligible.retryable
                                THEN :now + make_interval(secs => eligible.retry_interval_seconds)
                                ELSE node.next_schedule_time END,
                            schedule_fail_count = CASE WHEN eligible.retryable
                                THEN node.schedule_fail_count ELSE 0 END,
                            finished_at = CASE WHEN eligible.retryable THEN NULL ELSE :now END,
                            updated_at = :now
                        FROM eligible, updated_attempt
                        WHERE node.id = eligible.node_id
                        RETURNING node.dag_instance_id, eligible.retryable AS retryable
                    ), updated_dag AS (
                        UPDATE sr_dag_instance dag
                        SET finished_node_count = dag.finished_node_count + 1,
                            success_node_count = dag.success_node_count + :successDelta,
                            failed_node_count = dag.failed_node_count + :failureDelta,
                            last_progress_time = :now, updated_at = :now
                        FROM updated_node
                        WHERE dag.id = updated_node.dag_instance_id
                          AND dag.status = 10
                          AND NOT updated_node.retryable
                        RETURNING dag.id
                    ), released_worker AS (
                        UPDATE sr_worker worker
                        SET reserved_capacity = GREATEST(0, worker.reserved_capacity - 1),
                            updated_at = :now
                        FROM eligible, updated_node
                        WHERE worker.id = eligible.worker_id
                        RETURNING worker.id
                    ), duplicate AS (
                        SELECT 1
                        FROM sr_dag_node_attempt attempt
                        JOIN sr_dag_node_instance node ON node.id = attempt.node_instance_id
                        JOIN sr_dag_instance dag ON dag.id = node.dag_instance_id
                        WHERE attempt.id = :attemptId
                          AND attempt.dag_instance_id = :dagInstanceId
                          AND attempt.node_instance_id = :nodeInstanceId
                          AND attempt.attempt_no = :attemptNo
                          AND attempt.request_id = :requestId
                          AND attempt.request_checksum = :requestChecksum
                          AND attempt.dispatch_generation = :dispatchGeneration
                          AND attempt.dispatch_token = :dispatchToken
                          AND attempt.attempt_lease_version = :attemptLeaseVersion
                          AND attempt.worker_id = :workerId
                          AND attempt.worker_epoch = :workerEpoch
                          AND attempt.status IN (40, 50, 60)
                          AND node.node_id = :nodeCode
                          AND node.current_attempt_id = attempt.id
                          AND node.current_attempt_no = attempt.attempt_no
                          AND node.dispatch_generation = attempt.dispatch_generation
                          AND node.dispatch_token = attempt.dispatch_token
                          AND node.status IN (10, :nodeStatus)
                    )
                    SELECT CASE
                        WHEN EXISTS (SELECT 1 FROM released_worker) THEN 'ACCEPTED'
                        WHEN EXISTS (SELECT 1 FROM duplicate) THEN 'DUPLICATE'
                        ELSE 'REJECTED'
                    END AS outcome
                    """, parameters(update),
                    (resultSet, rowNumber) -> resultSet.getString("outcome"));
            Outcome result = Outcome.valueOf(outcomes.get(0));
            if (result == Outcome.REJECTED) {
                audit("TERMINAL", false, "FENCE_MISMATCH",
                        update.getReportJson(), update.getNow());
            }
            return result;
        }));
        return outcome;
    }

    @Override
    public boolean progress(WorkerExecutionContext context, Instant now, String reportJson) {
        requireContext(context);
        int updated = jdbc.update("""
                UPDATE sr_dag_instance dag
                SET last_progress_time = :now, updated_at = :now
                WHERE dag.id = :dagInstanceId AND dag.status = 10
                  AND EXISTS (
                      SELECT 1 FROM sr_dag_node_attempt attempt
                      JOIN sr_dag_node_instance node ON node.id = attempt.node_instance_id
                      WHERE attempt.id = :attemptId
                        AND attempt.dag_instance_id = :dagInstanceId
                        AND attempt.node_instance_id = :nodeInstanceId
                        AND attempt.attempt_no = :attemptNo
                        AND attempt.request_id = :requestId
                        AND attempt.request_checksum = :requestChecksum
                        AND attempt.dispatch_generation = :dispatchGeneration
                        AND attempt.dispatch_token = :dispatchToken
                        AND attempt.attempt_lease_version = :attemptLeaseVersion
                        AND attempt.worker_id = :workerId
                        AND attempt.worker_epoch = :workerEpoch
                        AND attempt.status IN (10, 20, 30)
                        AND node.node_id = :nodeCode
                        AND node.current_attempt_id = attempt.id
                        AND node.current_attempt_no = attempt.attempt_no
                        AND node.dispatch_generation = attempt.dispatch_generation
                        AND node.dispatch_token = attempt.dispatch_token
                        AND node.status = 40)
                """, parameters(context, now));
        if (updated == 0) {
            audit("PROGRESS", false, "FENCE_MISMATCH", reportJson, now);
        }
        return updated == 1;
    }

    private void audit(String type, boolean accepted, String reason, String report, Instant now) {
        jdbc.update("""
                INSERT INTO sr_dag_execution_report_audit(
                    report_type, accepted, reason, report_json, created_at)
                VALUES (:reportType, :accepted, :reason, CAST(:reportJson AS jsonb), :now)
                """, new MapSqlParameterSource()
                .addValue("reportType", type)
                .addValue("accepted", accepted)
                .addValue("reason", reason)
                .addValue("reportJson", report == null ? "{}" : report)
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE));
    }

    private MapSqlParameterSource parameters(TerminalUpdate update) {
        return parameters(update.getContext(), update.getNow())
                .addValue("attemptStatus", update.getAttemptStatus())
                .addValue("nodeStatus", update.getNodeStatus())
                .addValue("resultJson", update.getResultJson(), Types.VARCHAR)
                .addValue("resultRef", update.getResultRef())
                .addValue("errorCode", update.getErrorCode())
                .addValue("errorMessage", update.getErrorMessage())
                .addValue("successDelta", update.isIncrementSuccess() ? 1 : 0)
                .addValue("failureDelta", update.isIncrementFailure() ? 1 : 0);
    }

    private MapSqlParameterSource parameters(WorkerExecutionContext context, Instant now) {
        return new MapSqlParameterSource()
                .addValue("dagInstanceId", context.getDagInstanceId())
                .addValue("nodeInstanceId", context.getNodeInstanceId())
                .addValue("nodeCode", context.getNodeCode())
                .addValue("attemptId", Long.valueOf(context.getAttemptId()))
                .addValue("attemptNo", context.getAttemptNo())
                .addValue("requestId", context.getRequestId())
                .addValue("requestChecksum", context.getRequestChecksum())
                .addValue("dispatchGeneration", context.getDispatchGeneration())
                .addValue("dispatchToken", context.getDispatchToken())
                .addValue("attemptLeaseVersion", context.getAttemptLeaseVersion())
                .addValue("workerId", context.getWorkerId())
                .addValue("workerEpoch", context.getWorkerEpoch())
                .addValue("now", timestamp(now), Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private static WorkerExecutionContext requireContext(WorkerExecutionContext context) {
        Objects.requireNonNull(context, "executionContext");
        Objects.requireNonNull(context.getDagInstanceId(), "dagInstanceId");
        Objects.requireNonNull(context.getNodeInstanceId(), "nodeInstanceId");
        Objects.requireNonNull(context.getNodeCode(), "nodeCode");
        Objects.requireNonNull(context.getAttemptId(), "attemptId");
        Objects.requireNonNull(context.getAttemptNo(), "attemptNo");
        Objects.requireNonNull(context.getRequestId(), "requestId");
        Objects.requireNonNull(context.getRequestChecksum(), "requestChecksum");
        Objects.requireNonNull(context.getDispatchGeneration(), "dispatchGeneration");
        Objects.requireNonNull(context.getDispatchToken(), "dispatchToken");
        Objects.requireNonNull(context.getAttemptLeaseVersion(), "attemptLeaseVersion");
        Objects.requireNonNull(context.getWorkerId(), "workerId");
        Objects.requireNonNull(context.getWorkerEpoch(), "workerEpoch");
        return context;
    }

    private static OffsetDateTime timestamp(Instant value) {
        return value.atOffset(ZoneOffset.UTC);
    }
}
