package com.staterelay.contract.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** Worker 对单次围栏任务 Attempt 上报的非终态进度。 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskProgressReport {

    private String taskInstanceId;
    private String attemptId;
    private long leaseVersion;
    private String workerId;
    private String workerEpoch;
    private int percent;
    private String message;
    private Instant reportedAt;
    private WorkerExecutionContext executionContext;

    public TaskProgressReport(
            String taskInstanceId,
            String attemptId,
            long leaseVersion,
            String workerId,
            String workerEpoch,
            int percent,
            String message,
            Instant reportedAt,
            WorkerExecutionContext executionContext) {
        this.taskInstanceId = taskInstanceId;
        this.attemptId = attemptId;
        this.leaseVersion = leaseVersion;
        this.workerId = workerId;
        this.workerEpoch = workerEpoch;
        this.percent = percent;
        this.message = message;
        this.reportedAt = reportedAt;
        this.executionContext = executionContext;
    }

    /** 兼容普通任务旧协议的构造器。 */
    public TaskProgressReport(
            String taskInstanceId,
            String attemptId,
            long leaseVersion,
            String workerId,
            String workerEpoch,
            int percent,
            String message,
            Instant reportedAt) {
        this(taskInstanceId, attemptId, leaseVersion, workerId, workerEpoch,
                percent, message, reportedAt, null);
    }
}
