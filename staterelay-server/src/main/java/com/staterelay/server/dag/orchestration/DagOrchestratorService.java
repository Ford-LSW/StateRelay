package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import com.staterelay.server.dag.service.DagDefinitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * DAG Engine 核心 Service（对齐文档 §19.2 轮询模型）。
 *
 * <p>每轮推进职责：
 * <ol>
 *   <li>处理取消中的实例（NodeInstance 链式 CANCELLED → DagInstance CANCELLED）</li>
 *   <li>同步 Attempt 终态到 NodeInstance（§19.1 事务）</li>
 *   <li>推进后继节点 WAITING → READY（含根节点首轮推进）</li>
 *   <li>链式跳过：FAILED 节点的后继 → SKIPPED（§20.2）</li>
 *   <li>终态化 DagInstance（§22）</li>
 * </ol>
 *
 * <p>所有推进均使用 CAS，幂等：CAS 失败说明已被其他 Engine 实例处理，跳过即可。
 */
@Service
public class DagOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(DagOrchestratorService.class);

    private final DagInstanceRepository instanceRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final DagInstanceMapper instanceMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final DagDefinitionService definitionService;
    private final BindingResolver bindingResolver;

    public DagOrchestratorService(DagInstanceRepository instanceRepository,
                                   NodeInstanceJpaRepository nodeInstanceRepository,
                                   DagInstanceMapper instanceMapper,
                                   NodeInstanceMapper nodeInstanceMapper,
                                   DagDefinitionService definitionService,
                                   BindingResolver bindingResolver) {
        this.instanceRepository = instanceRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.instanceMapper = instanceMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.definitionService = definitionService;
        this.bindingResolver = bindingResolver;
    }

    /**
     * 推进一个 RUNNING 的 DAG 实例（对齐文档 §19.2 轮询模型）。
     *
     * <p>scanner 仅扫 RUNNING 实例；CANCELLED 实例的节点取消在 cancel API 同事务内完成。
     */
    @Transactional
    public void advanceInstance(Long dagInstanceId) {
        DagInstanceEntity instance = instanceRepository.findById(dagInstanceId)
            .orElseThrow(() -> new IllegalStateException("DAG instance not found: " + dagInstanceId));

        if (instance.getStatus() != DagInstanceStatus.RUNNING) {
            return;  // scanner 仅领取 RUNNING，理论上不会进入其他分支
        }
        advanceRunning(instance);
    }

    private void advanceRunning(DagInstanceEntity instance) {
        Instant now = Instant.now();

        // NodeAttempt 终态 → NodeInstance 推进由 NodeAttemptSyncService 独立 @Scheduled 完成（§19.1 + §19.2 轮询）

        // 1. 推进后继节点 WAITING → READY（含根节点首轮推进，§5.1 + §20）
        advanceReadyableNodes(instance, now);

        // 2. 链式跳过 FAILED 节点的后继（§20.2）
        skipDescendantsOfFailed(instance, now);

        // 3. 终态化 DagInstance（§22）
        tryFinalizeInstance(instance, now);
    }

    /**
     * 推进所有满足条件的 WAITING 节点到 READY（根节点 + 前驱全部 SUCCESS 的后继节点）。
     */
    private void advanceReadyableNodes(DagInstanceEntity instance, Instant now) {
        List<Long> readyable = nodeInstanceMapper.findReadyableNodes(instance.getId());
        if (readyable.isEmpty()) {
            return;
        }
        BindingResolver.DagDefinitionVersionLite versionLite = loadVersionLite(instance);
        if (versionLite == null) {
            log.warn("DAG definition version not found for instance {}", instance.getId());
            return;
        }
        for (Long nodeInstanceId : readyable) {
            NodeInstanceEntity node = nodeInstanceRepository.findById(nodeInstanceId).orElse(null);
            if (node == null || node.getStatus() != NodeInstanceStatus.WAITING) {
                continue;  // CAS 失败 / 已被其他实例推进
            }
            DagDefinition.DagNode dagNode = findDagNode(versionLite, node.getNodeId());
            if (dagNode == null) {
                log.warn("Node {} not found in DAG definition snapshot for instance {}",
                    node.getNodeId(), instance.getId());
                continue;
            }
            String bindings = bindingResolver.resolveBindings(instance, versionLite, dagNode);
            int updated = nodeInstanceMapper.markReady(nodeInstanceId, bindings, now);
            if (updated > 0) {
                log.debug("Node {} of DAG instance {} advanced WAITING → READY",
                    node.getNodeId(), instance.getId());
            }
        }
    }

    /**
     * 链式跳过 FAILED 节点的后继（§20.2）。
     *
     * <p>规则：若某 WAITING 节点的所有前驱均为 FAILED/SKIPPED（无可成功路径），则 CAS → SKIPPED。
     * 通过 {@link NodeInstanceMapper#findReadyableNodes} 反向查询"无法推进且存在前驱的 WAITING 节点"，
     * 简化实现：扫描 WAITING 节点中前驱全部为终态失败（FAILED/SKIPPED）的节点。
     *
     * <p>根节点（无前驱）天然不会被跳过，因为 {@code hasAnyPred=false} 直接返回 false。
     */
    private void skipDescendantsOfFailed(DagInstanceEntity instance, Instant now) {
        List<NodeInstanceEntity> waitingNodes = nodeInstanceRepository
            .findByDagInstanceIdAndStatusIn(instance.getId(), List.of(NodeInstanceStatus.WAITING));
        if (waitingNodes.isEmpty()) {
            return;
        }
        BindingResolver.DagDefinitionVersionLite versionLite = loadVersionLite(instance);
        if (versionLite == null || versionLite.getSnapshot().getEdges() == null) {
            return;
        }
        List<NodeInstanceEntity> all = nodeInstanceRepository.findByDagInstanceId(instance.getId());
        for (NodeInstanceEntity node : waitingNodes) {
            if (allPredecessorsFailed(versionLite, all, node.getNodeId())) {
                int updated = nodeInstanceMapper.markSkipped(node.getId(), now);
                if (updated > 0) {
                    log.debug("Node {} of DAG instance {} skipped (predecessors all failed)",
                        node.getNodeId(), instance.getId());
                }
            }
        }
    }

    /**
     * 判断指定节点的所有前驱是否均为 FAILED/SKIPPED。
     * 根节点（无前驱）返回 false（不跳过）。
     */
    private boolean allPredecessorsFailed(BindingResolver.DagDefinitionVersionLite versionLite,
                                          List<NodeInstanceEntity> allNodes,
                                          String nodeId) {
        boolean hasAnyPred = false;
        for (DagDefinition.DagEdge edge : versionLite.getSnapshot().getEdges()) {
            if (!nodeId.equals(edge.getTo())) {
                continue;
            }
            hasAnyPred = true;
            for (NodeInstanceEntity pred : allNodes) {
                if (pred.getNodeId().equals(edge.getFrom())) {
                    if (pred.getStatus() != NodeInstanceStatus.FAILED
                            && pred.getStatus() != NodeInstanceStatus.SKIPPED) {
                        // 前驱非失败，不能跳过
                        return false;
                    }
                }
            }
        }
        return hasAnyPred;
    }

    /**
     * 终态化 DagInstance（§22）。
     */
    private void tryFinalizeInstance(DagInstanceEntity instance, Instant now) {
        NodeInstanceMapper.NodeStatusCount count = nodeInstanceMapper.countByStatus(instance.getId());
        if (count == null || count.getActiveCount() == null || count.getActiveCount() > 0) {
            return;  // 仍有非终态节点
        }
        DagInstanceStatus finalStatus = count.getFailedCount() > 0
            ? DagInstanceStatus.FAILED : DagInstanceStatus.SUCCESS;
        int updated = instanceMapper.finalizeInstance(
            instance.getId(), finalStatus,
            count.getSuccessCount() == null ? 0 : count.getSuccessCount(),
            count.getFailedCount() == null ? 0 : count.getFailedCount(),
            count.getSkippedCount() == null ? 0 : count.getSkippedCount(),
            null, null, now);
        if (updated > 0) {
            log.info("DAG instance {} finalized as {}", instance.getId(), finalStatus);
        }
    }

    private BindingResolver.DagDefinitionVersionLite loadVersionLite(DagInstanceEntity instance) {
        return definitionService.findVersionEntity(instance.getDagDefinitionVersionId())
            .map(v -> new BindingResolver.DagDefinitionVersionLite(v.getId(), definitionService.loadSnapshot(v)))
            .orElse(null);
    }

    private DagDefinition.DagNode findDagNode(
        BindingResolver.DagDefinitionVersionLite versionLite, String nodeId) {
        return versionLite.getSnapshot().getNodes().stream()
            .filter(n -> nodeId.equals(n.getId()))
            .findFirst()
            .orElse(null);
    }
}
