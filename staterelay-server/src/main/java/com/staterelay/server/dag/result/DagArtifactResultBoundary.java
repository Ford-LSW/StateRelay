package com.staterelay.server.dag.result;

import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.protocol.TaskResultReport;

import java.time.Instant;

/**
 * Task 4 的制品晋升扩展点，仅处理已由结果存储确认的成功输出。
 * 同一权威结果可能因上次晋升中断而以重复上报再次调用，实现必须保持幂等。
 * 文件系统与对象存储安全由 Task 4 实现。
 */
public interface DagArtifactResultBoundary {

    void promote(
            WorkerExecutionContext context,
            TaskResultReport.TerminalStatus status,
            JsonNode outputs,
            Instant confirmedAt);
}
