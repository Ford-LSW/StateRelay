package com.staterelay.contract.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** Worker 对单次任务 Attempt 上报的围栏终态结果。 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskResultReport {

    private String taskInstanceId;
    private String attemptId;
    private long leaseVersion;
    private String workerId;
    private String workerEpoch;
    private TerminalStatus terminalStatus;
    private JsonNode result;
    private String errorCode;
    private String errorMessage;
    private Instant startedAt;
    private Instant finishedAt;
    private WorkerExecutionContext executionContext;

    public TaskResultReport(
            String taskInstanceId,
            String attemptId,
            long leaseVersion,
            String workerId,
            String workerEpoch,
            TerminalStatus terminalStatus,
            JsonNode result,
            String errorCode,
            String errorMessage,
            Instant startedAt,
            Instant finishedAt,
            WorkerExecutionContext executionContext) {
        this.taskInstanceId = taskInstanceId;
        this.attemptId = attemptId;
        this.leaseVersion = leaseVersion;
        this.workerId = workerId;
        this.workerEpoch = workerEpoch;
        this.terminalStatus = terminalStatus;
        this.result = result;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.executionContext = executionContext;
    }

    /** 兼容普通任务旧协议的构造器。 */
    public TaskResultReport(
            String taskInstanceId,
            String attemptId,
            long leaseVersion,
            String workerId,
            String workerEpoch,
            TerminalStatus terminalStatus,
            JsonNode result,
            String errorCode,
            String errorMessage,
            Instant startedAt,
            Instant finishedAt) {
        this(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch,
                terminalStatus, result, errorCode, errorMessage, startedAt, finishedAt, null);
    }

    /** 任务执行上报的终态。 */
    public enum TerminalStatus {
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
