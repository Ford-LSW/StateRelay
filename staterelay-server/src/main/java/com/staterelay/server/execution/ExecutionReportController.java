package com.staterelay.server.execution;

import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

/** 普通任务与 DAG 节点共用的 Worker 上报入口。 */
@RestController
@RequestMapping("/internal/v1/executions")
public final class ExecutionReportController {

    private final ExecutionReportHandler generic;
    private final ExecutionReportHandler dag;

    public ExecutionReportController(
            @Qualifier("genericExecutionReportHandler") ExecutionReportHandler generic,
            @Qualifier("dagExecutionReportHandler") ExecutionReportHandler dag) {
        this.generic = Objects.requireNonNull(generic, "generic");
        this.dag = Objects.requireNonNull(dag, "dag");
    }

    @PostMapping(value = "/progress", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ReportReceipt progress(@RequestBody TaskProgressReport report) {
        return route(report.getExecutionContext() != null).progress(report);
    }

    @PostMapping(value = "/results", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ReportReceipt result(@RequestBody TaskResultReport report) {
        return route(report.getExecutionContext() != null).result(report);
    }

    private ExecutionReportHandler route(boolean dagContextPresent) {
        return dagContextPresent ? dag : generic;
    }
}
