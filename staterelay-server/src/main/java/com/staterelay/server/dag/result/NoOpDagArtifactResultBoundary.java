package com.staterelay.server.dag.result;

import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.protocol.TaskResultReport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.time.Instant;

/** 未启用 Artifact 持久化模块时使用的兼容边界。 */
@Component
@ConditionalOnMissingBean(DagArtifactResultBoundary.class)
public final class NoOpDagArtifactResultBoundary implements DagArtifactResultBoundary {
    @Override
    public void promote(
            WorkerExecutionContext context,
            TaskResultReport.TerminalStatus status,
            JsonNode outputs,
            Instant confirmedAt) {
        // 兼容精简部署；完整服务由 SpatialArtifactResultBoundary 接管。
    }
}
