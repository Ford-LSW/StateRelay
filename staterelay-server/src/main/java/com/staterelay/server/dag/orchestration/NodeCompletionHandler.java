package com.staterelay.server.dag.orchestration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.ArtifactRef;
import com.staterelay.contract.dag.NodeResult;
import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.DagNodeExecutionMapper;
import com.staterelay.server.dag.mapper.dto.NodeCompletionSnapshot;
import com.staterelay.server.dag.repository.DagNodeExecutionJpaRepository;
import com.staterelay.server.dag.service.DagArtifactService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * 节点完成处理器。
 *
 * <p>由 TaskInstance 完成事件触发（现有项目的 TaskInstanceService 调用），负责：
 * <ol>
 *   <li>节点 WAITING_RESULT → DONE, status → SUCCESS/FAILED</li>
 *   <li>持久化节点输出 Artifact</li>
 *   <li>唤醒 DAG 实例（WAITING_NODES → READY）</li>
 * </ol>
 */
@Component
public class NodeCompletionHandler {

    private static final Logger log = LoggerFactory.getLogger(NodeCompletionHandler.class);

    private final DagNodeExecutionJpaRepository nodeExecutionRepository;
    private final DagNodeExecutionMapper nodeExecutionMapper;
    private final DagInstanceMapper instanceMapper;
    private final DagArtifactService artifactService;
    private final ObjectMapper objectMapper;

    public NodeCompletionHandler(DagNodeExecutionJpaRepository nodeExecutionRepository,
                                  DagNodeExecutionMapper nodeExecutionMapper,
                                  DagInstanceMapper instanceMapper,
                                  DagArtifactService artifactService,
                                  ObjectMapper objectMapper) {
        this.nodeExecutionRepository = nodeExecutionRepository;
        this.nodeExecutionMapper = nodeExecutionMapper;
        this.instanceMapper = instanceMapper;
        this.artifactService = artifactService;
        this.objectMapper = objectMapper;
    }

    /**
     * 处理节点完成。
     *
     * @param nodeExecutionId DAG 节点执行实例 ID
     * @param result 节点执行结果
     */
    @Transactional
    public void handleCompletion(Long nodeExecutionId, NodeResult result) {
        Instant now = Instant.now();
        switch (result.getStatus()) {
            case SUCCESS -> handleSuccess(nodeExecutionId, result, now);
            case FAILED -> handleFailed(nodeExecutionId, result, now);
            case SKIPPED -> handleSkipped(nodeExecutionId, result, now);
        }
    }

    private void handleSuccess(Long nodeExecutionId, NodeResult result, Instant now) {
        NodeCompletionSnapshot snapshot = nodeExecutionMapper.markSuccess(nodeExecutionId, now);
        if (snapshot == null) {
            log.debug("Node {} already finalized or not in WAITING_RESULT", nodeExecutionId);
            return;
        }
        artifactService.saveArtifacts(snapshot.getDagInstanceId(), snapshot.getNodeId(), result.getOutputs());
        wakeInstance(snapshot.getDagInstanceId());
    }

    private void handleFailed(Long nodeExecutionId, NodeResult result, Instant now) {
        int updated = nodeExecutionMapper.markFailed(
            nodeExecutionId, null, result.getErrorMessage(), now);
        if (updated == 0) {
            log.debug("Node {} already finalized or not in WAITING_RESULT", nodeExecutionId);
            return;
        }
        DagNodeExecutionEntity node = nodeExecutionRepository.findById(nodeExecutionId)
            .orElseThrow(() -> new IllegalStateException("Node not found: " + nodeExecutionId));
        wakeInstance(node.getDagInstanceId());
    }

    private void handleSkipped(Long nodeExecutionId, NodeResult result, Instant now) {
        // 跳过节点视为 SUCCESS 但不写 Artifact，由上游节点已经提供了所需数据
        NodeCompletionSnapshot snapshot = nodeExecutionMapper.markSuccess(nodeExecutionId, now);
        if (snapshot == null) {
            return;
        }
        wakeInstance(snapshot.getDagInstanceId());
    }

    /**
     * 唤醒 DAG 实例（WAITING_NODES → READY）。
     *
     * <p>幂等：返回 0 表示实例已不在 WAITING_NODES（如已被其他事件唤醒或已终态）。
     */
    private void wakeInstance(Long dagInstanceId) {
        int woken = instanceMapper.tryReenqueue(dagInstanceId, Instant.now());
        if (woken > 0) {
            log.debug("DAG instance {} woken up after node completion", dagInstanceId);
        }
    }
}
