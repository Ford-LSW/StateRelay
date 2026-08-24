package com.staterelay.contract.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;

import java.time.Duration;
import java.time.Instant;

/**
 * Scheduler-to-Worker command that starts one concrete task attempt.
 *
 * <p>{@code dispatchId} supports idempotent retransmission, while
 * {@code leaseVersion} and {@code targetWorkerEpoch} fence the command to the
 * current execution owner. Workers must reject commands for another ID or epoch.</p>
 *
 * <p><b>DAG 场景扩展（文档 §12）：</b>
 * {@code executionContext} 携带调度和围栏信息（dagInstanceId / nodeCode / attemptNo /
 * requestId / requestChecksum / attemptLeaseVersion 等），供 {@code AlgorithmExecutor}
 * 通过 {@code AlgorithmExecutionContext} 访问。非 DAG 场景可为 null。</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExecuteTaskCommand(
        String taskInstanceId,
        String attemptId,
        int attemptNumber,
        String dispatchId,
        long leaseVersion,
        String application,
        String targetWorkerId,
        String targetWorkerEpoch,
        String handlerName,
        JsonNode parameter,
        String idempotencyKey,
        Duration leaseDuration,
        Instant dispatchedAt,
        WorkerExecutionContext executionContext) {

    /**
     * 向后兼容的构造器（无 executionContext，非 DAG 场景）。
     */
    public ExecuteTaskCommand(
            String taskInstanceId,
            String attemptId,
            int attemptNumber,
            String dispatchId,
            long leaseVersion,
            String application,
            String targetWorkerId,
            String targetWorkerEpoch,
            String handlerName,
            JsonNode parameter,
            String idempotencyKey,
            Duration leaseDuration,
            Instant dispatchedAt) {
        this(taskInstanceId, attemptId, attemptNumber, dispatchId, leaseVersion, application,
                targetWorkerId, targetWorkerEpoch, handlerName, parameter, idempotencyKey,
                leaseDuration, dispatchedAt, null);
    }
}
