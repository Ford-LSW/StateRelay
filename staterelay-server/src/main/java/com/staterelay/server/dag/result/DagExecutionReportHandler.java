package com.staterelay.server.dag.result;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.NodeAttemptEntity;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.NodeAttemptRepository;
import com.staterelay.server.dag.service.AlgorithmContractAdapter;
import com.staterelay.server.execution.ExecutionReportHandler;
import com.staterelay.server.execution.ReportReceipt;
import org.springframework.stereotype.Component;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** 通过完整围栏的结果存储校验并持久化 DAG 上报。 */
@Component("dagExecutionReportHandler")
public final class DagExecutionReportHandler implements ExecutionReportHandler {

    private final DagResultStore store;
    private final NodeAttemptRepository attempts;
    private final AlgorithmDefinitionRepository algorithms;
    private final AlgorithmContractAdapter contractAdapter;
    private final AlgorithmOutputsValidator outputsValidator;
    private final DagArtifactResultBoundary artifacts;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public DagExecutionReportHandler(
            DagResultStore store,
            NodeAttemptRepository attempts,
            AlgorithmDefinitionRepository algorithms,
            AlgorithmContractAdapter contractAdapter,
            AlgorithmOutputsValidator outputsValidator,
            DagArtifactResultBoundary artifacts,
            ObjectMapper objectMapper) {
        this(store, attempts, algorithms, contractAdapter, outputsValidator, artifacts,
                objectMapper, Clock.systemUTC());
    }

    DagExecutionReportHandler(
            DagResultStore store,
            NodeAttemptRepository attempts,
            AlgorithmDefinitionRepository algorithms,
            AlgorithmContractAdapter contractAdapter,
            AlgorithmOutputsValidator outputsValidator,
            DagArtifactResultBoundary artifacts,
            ObjectMapper objectMapper,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.algorithms = Objects.requireNonNull(algorithms, "algorithms");
        this.contractAdapter = Objects.requireNonNull(contractAdapter, "contractAdapter");
        this.outputsValidator = Objects.requireNonNull(outputsValidator, "outputsValidator");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ReportReceipt progress(TaskProgressReport report) {
        try {
            return store.progress(
                    report.getExecutionContext(), clock.instant(), json(report))
                    ? ReportReceipt.accepted()
                    : ReportReceipt.rejected("DAG progress fence mismatch");
        } catch (RuntimeException exception) {
            return ReportReceipt.rejected(message(exception));
        }
    }

    @Override
    public ReportReceipt result(TaskResultReport report) {
        Instant now = clock.instant();
        try {
            Terminal terminal = terminal(report);
            DagResultStore.Outcome outcome = store.accept(new DagResultStore.TerminalUpdate(
                    report.getExecutionContext(), terminal.getAttemptStatus(), terminal.getNodeStatus(),
                    terminal.getResultJson(), terminal.getResultRef(), terminal.getErrorCode(),
                    terminal.getErrorMessage(), terminal.isSuccess(), terminal.isFailure(), now,
                    json(report)));
            artifacts.promote(report.getExecutionContext(), terminal.getStatus(),
                    terminal.getOutputs(), now);
            if (outcome == DagResultStore.Outcome.ACCEPTED
                    || outcome == DagResultStore.Outcome.DUPLICATE) {
                return outcome == DagResultStore.Outcome.ACCEPTED
                        ? ReportReceipt.accepted()
                        : ReportReceipt.duplicate();
            }
            return ReportReceipt.rejected("DAG terminal fence mismatch");
        } catch (RuntimeException exception) {
            return ReportReceipt.rejected(message(exception));
        }
    }

    private Terminal terminal(TaskResultReport report) {
        return switch (report.getTerminalStatus()) {
            case SUCCEEDED -> success(report);
            case FAILED -> new Terminal(
                    50, 60, null, null, report.getErrorCode(), report.getErrorMessage(),
                    false, true, TaskResultReport.TerminalStatus.FAILED, null);
            case CANCELLED -> new Terminal(
                    60, 70, null, null, report.getErrorCode(), report.getErrorMessage(),
                    false, false, TaskResultReport.TerminalStatus.CANCELLED, null);
        };
    }

    private Terminal success(TaskResultReport report) {
        Long attemptId = Long.valueOf(report.getExecutionContext().getAttemptId());
        NodeAttemptEntity attempt = attempts.findById(attemptId)
                .orElseThrow(() -> new IllegalArgumentException("unknown DAG attempt"));
        AlgorithmDefinitionEntity definition = algorithms
                .findByAlgorithmCodeAndStatus(
                        attempt.getAlgorithmCode(), AlgorithmDefinitionStatus.ENABLED)
                .orElseThrow(() -> new IllegalArgumentException(
                        "enabled algorithm output contract not found"));
        AlgorithmContract contract = contractAdapter.toContract(definition);
        AlgorithmOutputsValidator.Validation validation =
                outputsValidator.validate(contract, report.getResult());
        if (!validation.isValid()) {
            return new Terminal(
                    50, 60, null, null, "INVALID_ALGORITHM_OUTPUT", validation.getError(),
                    false, true, TaskResultReport.TerminalStatus.FAILED, null);
        }
        return new Terminal(
                40, 50, validation.getResultJson(), validation.getResultRef(), null, null,
                true, false, TaskResultReport.TerminalStatus.SUCCEEDED,
                report.getResult().path("outputs"));
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("report cannot be serialized", exception);
        }
    }

    private static String message(RuntimeException exception) {
        return exception.getMessage() == null ? exception.getClass().getSimpleName()
                : exception.getMessage();
    }

    @Data
    @AllArgsConstructor
    private static class Terminal {
        private int attemptStatus;
        private int nodeStatus;
        private String resultJson;
        private String resultRef;
        private String errorCode;
        private String errorMessage;
        private boolean success;
        private boolean failure;
        private TaskResultReport.TerminalStatus status;
        private JsonNode outputs;
    }
}
