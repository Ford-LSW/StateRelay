package com.staterelay.server.dag.result;

import com.fasterxml.jackson.databind.JsonNode;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.server.dag.mapper.DagArtifactMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 将结果接收结论与 Artifact 状态机闭环。
 * 重复上报会再次执行相同的条件更新，因此可恢复且保持幂等。
 */
@Component
public class SpatialArtifactResultBoundary implements DagArtifactResultBoundary {

    private final DagArtifactMapper artifactMapper;

    public SpatialArtifactResultBoundary(DagArtifactMapper artifactMapper) {
        this.artifactMapper = Objects.requireNonNull(artifactMapper, "artifactMapper");
    }

    /**
     * 成功且完整围栏仍有效时，仅晋升输出实际引用的成果；其余成果全部孤儿化。
     */
    @Override
    @Transactional
    public void promote(WorkerExecutionContext context,
                        TaskResultReport.TerminalStatus status,
                        JsonNode outputs,
                        Instant confirmedAt) {
        Objects.requireNonNull(context, "executionContext");
        Objects.requireNonNull(context.getAttemptId(), "attemptId");
        Instant now = confirmedAt == null ? Instant.now() : confirmedAt;
        List<Long> referenced = new ArrayList<>(collectArtifactIds(outputs));
        boolean currentSuccess = status == TaskResultReport.TerminalStatus.SUCCEEDED
                && !referenced.isEmpty()
                && artifactMapper.matchesAcceptedSuccessFence(context);
        List<Long> available = List.of();
        if (currentSuccess) {
            artifactMapper.promoteReferencedArtifacts(context, referenced, now);
            available = referenced;
        }
        artifactMapper.orphanUnreferencedArtifacts(context, available, now);
    }

    /** 递归提取输出对象中的 Artifact 引用，不把普通数字结果误认为 Artifact。 */
    private Set<Long> collectArtifactIds(JsonNode node) {
        Set<Long> ids = new LinkedHashSet<>();
        collectArtifactIds(node, ids);
        return ids;
    }

    /** 递归遍历对象和数组，收集名为 artifactId 的正整数值。 */
    private void collectArtifactIds(JsonNode node, Set<Long> ids) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            JsonNode artifactId = node.get("artifactId");
            if (artifactId != null && artifactId.canConvertToLong()
                    && artifactId.longValue() > 0) {
                ids.add(artifactId.longValue());
            }
            node.fields().forEachRemaining(entry -> collectArtifactIds(entry.getValue(), ids));
            return;
        }
        if (node.isArray()) {
            node.forEach(child -> collectArtifactIds(child, ids));
        }
    }
}
