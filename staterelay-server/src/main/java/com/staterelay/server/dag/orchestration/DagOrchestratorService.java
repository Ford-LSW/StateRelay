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
 * DAG Engine 核心 Service（对齐文档 §19.2 轮询模型 / §25.1 / §31.1 / §33.6 / §35.1 / §37.2）。
 *
 * <p>scanner 每轮领取活跃 DagInstance（RUNNING / CANCELLING / FAILING），按状态分流推进：
 * <ul>
 *   <li>RUNNING：推进 READY 节点；检测 FAILED/TIMEOUT 触发 FAILING 启动事务；判断全终态推进 SUCCESS</li>
 *   <li>CANCELLING：判断 RUNNING 节点是否全部终态（由 Scheduler 调 Worker cancel 完成）；满足后 CAS CANCELLING → CANCELLED</li>
 *   <li>FAILING：判断清理完成；CAS FAILING → FAILED</li>
 * </ul>
 *
 * <p>终态判断顺序（§35.1）：CANCELLING > FAILED > SUCCESS
 * <ol>
 *   <li>若存在 CANCELLED 节点 → 走 CANCELLING/CANCELLED 收敛路径</li>
 *   <li>否则若存在 FAILED/TIMEOUT 节点 → 走 FAILING/FAILED 收敛路径</li>
 *   <li>否则若全节点 SUCCESS → SUCCESS</li>
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
     * 推进一个活跃的 DAG 实例（对齐文档 §19.2 轮询模型 / §35.1 状态分流）。
     *
     * <p>scanner 领取 RUNNING / CANCELLING / FAILING 三种状态，统一进入此方法。
     *
     * <p>NodeAttempt 终态 → NodeInstance 推进由 NodeAttemptSyncService 独立 @Scheduled 完成（§19.1）。
     */
    @Transactional
    public void advanceInstance(Long dagInstanceId) {
        DagInstanceEntity instance = instanceRepository.findById(dagInstanceId)
            .orElseThrow(() -> new IllegalStateException("DAG instance not found: " + dagInstanceId));

        DagInstanceStatus status = instance.getStatus();
        if (status == null || !status.isActive()) {
            return;  // 已终态，跳过
        }
        switch (status) {
            case RUNNING -> advanceRunning(instance);
            case CANCELLING -> advanceCancelling(instance);
            case FAILING -> advanceFailing(instance);
            default -> { /* 不会到这里 */ }
        }
    }

    /**
     * RUNNING 推进（对齐文档 §35.1 判断顺序）：
     * <ol>
     *   <li>推进后继节点 WAITING → READY（含根节点首轮推进，§5.1 + §20）</li>
     *   <li>若存在 FAILED/TIMEOUT 节点 → 立即启动 FAILING 事务（§31.1 / §39.1 T7），
     *       不等待其余 RUNNING 节点收敛；剩余节点由 Scheduler 协作式取消推进</li>
     *   <li>若全节点 SUCCESS → CAS RUNNING → SUCCESS</li>
     *   <li>否则仍有未终态节点且无失败 → 维持 RUNNING，等下一轮</li>
     * </ol>
     *
     * <p>关键修正（对齐 §35.1）：FAILING 触发条件是"任意节点 FAILED/TIMEOUT"，
     * <b>不是</b>"全部节点终态后存在 FAILED"。后者会导致：
     * A FAILED + B WAITING（前驱失败）→ 永远 activeCount &gt; 0 → 永远不进入 FAILING。
     */
    private void advanceRunning(DagInstanceEntity instance) {
        Instant now = Instant.now();

        // 1. 推进后继节点 WAITING → READY（含根节点首轮推进，§5.1 + §20）
        // 注意：findReadyableNodes 只返回前驱全部 SUCCESS 的节点，
        // 因此前驱失败的 WAITING 节点不会被推进（替代了原 skipDescendantsOfFailed 的职责，§20.2）
        advanceReadyableNodes(instance, now);

        // 2. 终态化判断（§35.1 判断顺序：CANCELLING > FAILED > SUCCESS）
        NodeInstanceMapper.NodeStatusCount count = nodeInstanceMapper.countByStatus(instance.getId());
        if (count == null) {
            return;
        }
        int failedCount = (count.getFailedCount() == null ? 0 : count.getFailedCount())
                + (count.getTimeoutCount() == null ? 0 : count.getTimeoutCount());

        // §35.1 步骤 3：存在 FAILED/TIMEOUT 节点 → 立即启动 FAILING（不等剩余节点收敛）
        if (failedCount > 0) {
            int started = instanceMapper.startFailing(instance.getId(), now);
            if (started == 0) {
                // 已被其他流程推进（用户取消 → CANCELLING），不再处理
                return;
            }
            // FAILING 启动后，由 advanceFailing 完成终态化（CAS FAILING → FAILED）
            DagInstanceEntity refreshed = instanceRepository.findById(instance.getId()).orElse(null);
            if (refreshed != null) {
                advanceFailing(refreshed);
            }
            return;
        }

        // §35.1 步骤 4：全节点 SUCCESS → RUNNING → SUCCESS
        // 否则：仍有未终态节点且无失败 → 维持 RUNNING，等下一轮
        if (count.getActiveCount() != null && count.getActiveCount() > 0) {
            return;
        }
        finalizeAs(instance, DagInstanceStatus.RUNNING, DagInstanceStatus.SUCCESS, count, now);
    }

    /**
     * CANCELLING 推进：
     * <p>等待 Scheduler 把所有 RUNNING 节点 cancel 到 CANCELLED（§33.6 步骤 3）。
     * 一旦所有节点都进入终态，CAS CANCELLING → CANCELLED。
     */
    private void advanceCancelling(DagInstanceEntity instance) {
        Instant now = Instant.now();
        NodeInstanceMapper.NodeStatusCount count = nodeInstanceMapper.countByStatus(instance.getId());
        if (count == null) {
            return;
        }
        if (count.getActiveCount() != null && count.getActiveCount() > 0) {
            return;  // 仍有 RUNNING 节点未收敛
        }
        // 全部节点终态，CAS CANCELLING → CANCELLED
        finalizeAs(instance, DagInstanceStatus.CANCELLING, DagInstanceStatus.CANCELLED, count, now);
    }

    /**
     * FAILING 推进：
     * <p>等待清理完成（无未终态节点）。一旦全终态，CAS FAILING → FAILED。
     *
     * <p>注意：进入 FAILING 前必然存在 FAILED/TIMEOUT 节点，所以这里直接走 FAILED 终态化。
     */
    private void advanceFailing(DagInstanceEntity instance) {
        Instant now = Instant.now();
        NodeInstanceMapper.NodeStatusCount count = nodeInstanceMapper.countByStatus(instance.getId());
        if (count == null) {
            return;
        }
        if (count.getActiveCount() != null && count.getActiveCount() > 0) {
            return;  // 仍有未终态节点，等下一轮
        }
        // CAS FAILING → FAILED
        finalizeAs(instance, DagInstanceStatus.FAILING, DagInstanceStatus.FAILED, count, now);
    }

    /**
     * 终态化 DagInstance：CAS expectedFrom → finalStatus，写计数。
     */
    private void finalizeAs(DagInstanceEntity instance,
                            DagInstanceStatus expectedFrom,
                            DagInstanceStatus finalStatus,
                            NodeInstanceMapper.NodeStatusCount count,
                            Instant now) {
        String errorCode = null;
        String errorMessage = null;
        if (finalStatus == DagInstanceStatus.FAILED) {
            errorCode = "NODE_FAILED";
            errorMessage = "DAG failed: " + (count.getFailedCount() == null ? 0 : count.getFailedCount())
                    + " failed, " + (count.getTimeoutCount() == null ? 0 : count.getTimeoutCount()) + " timeout";
        }
        int updated = instanceMapper.finalizeInstance(
            instance.getId(),
            expectedFrom,
            finalStatus,
            count.getSuccessCount() == null ? 0 : count.getSuccessCount(),
            (count.getFailedCount() == null ? 0 : count.getFailedCount())
                + (count.getTimeoutCount() == null ? 0 : count.getTimeoutCount()),
            count.getSkippedCount() == null ? 0 : count.getSkippedCount(),
            errorCode,
            errorMessage,
            now);
        if (updated > 0) {
            log.info("DAG instance {} finalized as {} (from {})", instance.getId(), finalStatus, expectedFrom);
        }
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
                log.debug("Node {} of DAG instance {} advanced WAITING → READY", node.getNodeId(), instance.getId());
            }
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
