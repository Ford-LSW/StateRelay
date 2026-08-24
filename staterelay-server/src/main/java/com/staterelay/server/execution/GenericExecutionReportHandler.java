package com.staterelay.server.execution;

import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.server.domain.TaskAttemptStatus;
import com.staterelay.server.persistence.TaskAttemptRepository;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** 兼容非 DAG 普通任务上报协议的持久化适配器。 */
@Component("genericExecutionReportHandler")
public final class GenericExecutionReportHandler implements ExecutionReportHandler {

    private final TaskAttemptRepository repository;

    public GenericExecutionReportHandler(TaskAttemptRepository repository) {
        this.repository = repository;
    }

    @Override
    public ReportReceipt progress(TaskProgressReport report) {
        try {
            if (report.getPercent() < 0 || report.getPercent() > 100) {
                return ReportReceipt.rejected("progress percent must be between 0 and 100");
            }
            boolean accepted = repository.casReportProgress(
                    UUID.fromString(report.getTaskInstanceId()), UUID.fromString(report.getAttemptId()),
                    report.getLeaseVersion(), UUID.fromString(report.getWorkerId()),
                    UUID.fromString(report.getWorkerEpoch()), report.getPercent(), report.getMessage());
            return accepted ? ReportReceipt.accepted()
                    : ReportReceipt.rejected("generic task progress fence mismatch");
        } catch (IllegalArgumentException exception) {
            return ReportReceipt.rejected(exception.getMessage());
        }
    }

    @Override
    public ReportReceipt result(TaskResultReport report) {
        try {
            boolean accepted = repository.casCompleteAttempt(
                    UUID.fromString(report.getTaskInstanceId()), UUID.fromString(report.getAttemptId()),
                    report.getLeaseVersion(), UUID.fromString(report.getWorkerId()),
                    UUID.fromString(report.getWorkerEpoch()), status(report.getTerminalStatus()),
                    report.getResult());
            return accepted ? ReportReceipt.accepted() : ReportReceipt.duplicate();
        } catch (IllegalArgumentException exception) {
            return ReportReceipt.rejected(exception.getMessage());
        }
    }

    private static TaskAttemptStatus status(TaskResultReport.TerminalStatus status) {
        return switch (status) {
            case SUCCEEDED -> TaskAttemptStatus.SUCCESS;
            case FAILED -> TaskAttemptStatus.FAILED;
            case CANCELLED -> TaskAttemptStatus.CANCELLED;
        };
    }
}
