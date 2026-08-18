package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.contract.dag.enums.DagNodeExecutionStatus;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import com.staterelay.server.dag.mapper.DagEdgeMapper;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.DagNodeExecutionMapper;
import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.DagNodeExecutionJpaRepository;
import com.staterelay.server.dag.service.DagDefinitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * DAG Orchestrator 核心 Service，负责一个 DAG 实例在本轮的推进逻辑。
 *
 * <p>推进流程：
 * <ol>
 *   <li>加载实例和节点</li>
 *   <li>对每个未完成节点：判断前置是否满足，满足则 WAITING→READY（冻结 binding）→ READY→QUEUED（关联 task）</li>
 *   <li>所有节点终态化后，终态化 DAG 实例</li>
 * </ol>
 *
 * <p>实际任务分发由 {@code DispatchScanner}（现有项目）负责，本类只负责 DAG 层的节点推进和唤醒。
 */
@Service
public class DagOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(DagOrchestratorService.class);

    private final DagInstanceRepository instanceRepository;
    private final DagNodeExecutionJpaRepository nodeExecutionRepository;
    private final DagInstanceMapper instanceMapper;
    private final DagNodeExecutionMapper nodeExecutionMapper;
    private final DagEdgeMapper edgeMapper;
    private final DagDefinitionService definitionService;
    private final BindingResolver bindingResolver;

    public DagOrchestratorService(DagInstanceRepository instanceRepository,
                                   DagNodeExecutionJpaRepository nodeExecutionRepository,
                                   DagInstanceMapper instanceMapper,
                                   DagNodeExecutionMapper nodeExecutionMapper,
                                   DagEdgeMapper edgeMapper,
                                   DagDefinitionService definitionService,
                                   BindingResolver bindingResolver) {
        this.instanceRepository = instanceRepository;
        this.nodeExecutionRepository = nodeExecutionRepository;
        this.instanceMapper = instanceMapper;
        this.nodeExecutionMapper = nodeExecutionMapper;
        this.edgeMapper = edgeMapper;
        this.definitionService = definitionService;
        this.bindingResolver = bindingResolver;
    }

    /**
     * CAS: READY → QUEUED (version+1)，抢占推进权。
     */
    @Transactional
    public boolean tryEnqueueForAdvance(DagInstanceLease lease, Instant now, Instant deadline) {
        int updated = instanceMapper.tryEnqueueForAdvance(
            lease.getId(), lease.getWorkerId(), lease.getLeaseVersion(), now, deadline);
        return updated > 0;
    }

    public Long getCurrentOrchestrationVersion(Long dagInstanceId) {
        return instanceMapper.getCurrentOrchestrationVersion(dagInstanceId);
    }

    /**
     * CAS: QUEUED → EXECUTING（Runnable 启动时校验版本）。
     */
    @Transactional
    public boolean tryEnterExecuting(DagInstanceLease lease, Long expectedVersion, Instant now) {
        int updated = instanceMapper.tryEnterExecuting(
            lease.getId(), expectedVersion, lease.getWorkerId(), lease.getLeaseVersion(), now);
        return updated > 0;
    }

    /**
     * CAS: EXECUTING → WAITING_NODES (version+1)，本轮推进结束。
     */
    @Transactional
    public int tryEnterWaitingNodes(Long dagInstanceId) {
        return instanceMapper.tryEnterWaitingNodes(dagInstanceId, Instant.now());
    }

    /**
     * 推进一个 DAG 实例的节点。
     *
     * <p>幂等：节点 CAS 失败说明已被其他 Orchestrator 处理，跳过即可。
     */
    @Transactional
    public void advanceInstance(Long dagInstanceId) {
        DagInstanceEntity instance = instanceRepository.findById(dagInstanceId)
            .orElseThrow(() -> new IllegalStateException("DAG instance not found: " + dagInstanceId));
        if ("CANCELLING".equals(instance.getStatus())) {
            handleCancelling(instance);
            return;
        }
        if (!"RUNNING".equals(instance.getStatus())) {
            return;
        }

        // 加载定义版本快照（用于查询边和节点定义）
        BindingResolver.DagDefinitionVersionLite versionLite = loadVersionLite(instance);
        if (versionLite == null) {
            log.warn("DAG definition version not found for instance {}", dagInstanceId);
            return;
        }

        List<DagNodeExecutionEntity> nodes = nodeExecutionRepository.findByDagInstanceId(dagInstanceId);
        Instant now = Instant.now();
        for (DagNodeExecutionEntity node : nodes) {
            tryAdvanceNode(instance, versionLite, node, now);
        }
        tryFinalizeInstance(instance);
    }

    private void handleCancelling(DagInstanceEntity instance) {
        Instant now = Instant.now();
        nodeExecutionMapper.cancelUnstartedNodes(instance.getId(), now);
        nodeExecutionMapper.cancelRunningNodes(instance.getId(), now);
        instanceMapper.finalizeInstance(instance.getId(), DagInstanceStatus.CANCELLED, 0, 0, 0, null, null, now);
    }

    private void tryAdvanceNode(DagInstanceEntity instance,
                                BindingResolver.DagDefinitionVersionLite versionLite,
                                DagNodeExecutionEntity node,
                                Instant now) {
        if (node.getStatus() != DagNodeExecutionStatus.PENDING
                && node.getStatus() != DagNodeExecutionStatus.WAITING) {
            return;
        }
        if (node.getStatus() == DagNodeExecutionStatus.PENDING) {
            if (nodeExecutionMapper.markWaiting(node.getId(), now) == 0) {
                return;
            }
        }
        // 检查前置
        int unsatisfied = edgeMapper.countUnsatisfiedPredecessors(
            instance.getId(), versionLite.getVersionId(), node.getNodeId());
        if (unsatisfied > 0) {
            return;
        }
        // WAITING → READY，冻结 binding
        var dagNode = findDagNode(versionLite, node.getNodeId());
        if (dagNode == null) {
            log.warn("Node {} not found in DAG definition snapshot for instance {}",
                node.getNodeId(), instance.getId());
            return;
        }
        String bindings = bindingResolver.resolveBindings(instance, versionLite, dagNode);
        if (nodeExecutionMapper.markReady(node.getId(), bindings, now) == 0) {
            return;
        }
        // 实际 task_instance_id 由 DispatchScanner 在领取节点后回写（markQueued）
        log.debug("Node {} of DAG instance {} advanced to READY", node.getNodeId(), instance.getId());
    }

    private void tryFinalizeInstance(DagInstanceEntity instance) {
        List<DagNodeExecutionEntity> nodes = nodeExecutionRepository.findByDagInstanceId(instance.getId());
        int success = 0, failed = 0, skipped = 0;
        for (DagNodeExecutionEntity node : nodes) {
            switch (node.getStatus()) {
                case SUCCESS -> success++;
                case FAILED -> failed++;
                case SKIPPED, CANCELLED -> skipped++;
                default -> { /* still in progress */ return; }
            }
        }
        DagInstanceStatus finalStatus = failed > 0 ? DagInstanceStatus.FAILED : DagInstanceStatus.SUCCESS;
        instanceMapper.finalizeInstance(instance.getId(), finalStatus, success, failed, skipped, null, null, Instant.now());
    }

    private BindingResolver.DagDefinitionVersionLite loadVersionLite(DagInstanceEntity instance) {
        return definitionService.findVersionEntity(instance.getDagDefinitionVersionId())
            .map(v -> new BindingResolver.DagDefinitionVersionLite(v.getId(), definitionService.loadSnapshot(v)))
            .orElse(null);
    }

    private com.staterelay.contract.dag.DagDefinition.DagNode findDagNode(
        BindingResolver.DagDefinitionVersionLite versionLite, String nodeId) {
        return versionLite.getSnapshot().getNodes().stream()
            .filter(n -> nodeId.equals(n.getId()))
            .findFirst()
            .orElse(null);
    }
}
