package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.dispatch.DagDispatchCoordinator;
import com.staterelay.server.dag.dispatch.DagDispatchGateway;
import com.staterelay.server.dag.dispatch.DagDispatchStore;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.mapper.dto.NodeInstanceLease;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import com.staterelay.server.dag.service.AlgorithmContractAdapter;
import com.staterelay.server.dag.service.DagDefinitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * DAG 节点调度 Service（对齐文档 §26 / §27）。
 *
 * <p>由 {@link DagNodeDispatchScanner} 调用，完成从 READY 节点到 Worker 调度的完整链路：
 * <ol>
 *   <li>查询 NodeInstance + DagInstance + AlgorithmDefinition</li>
 *   <li>调用 {@link ParameterResolver#resolve} 解析参数（失败则 DISPATCHING → FAILED，§17）</li>
 *   <li>在统一的 {@code sr_worker} 注册表中选择 Worker 并原子预留容量</li>
 *   <li>无 Worker：
 *      <ul>
 *          <li>schedule_fail_count 未达上限：CAS DISPATCHING → READY，不创建 Attempt</li>
 *          <li>schedule_fail_count 已达上限：CAS DISPATCHING → FAILED，+1 finished_node_count</li>
 *      </ul>
 *   </li>
 *   <li>有 Worker：同一事务创建 NodeAttempt、写入 current-attempt fence，再在提交后发送 HTTP</li>
 * </ol>
 *
 * <p>HTTP 明确拒绝时释放容量并回退节点；ACK 丢失或网络异常进入 UNCERTAIN，
 * 后续重试复用同一个 requestId 与 attemptId。
 *
 * <p>事务管理：使用 {@link TransactionTemplate} 显式开启事务，避免 Spring AOP 自调用代理失效问题。
 */
@Service
public class DagNodeDispatchService {

    private static final Logger log = LoggerFactory.getLogger(DagNodeDispatchService.class);

    private final NodeInstanceMapper nodeInstanceMapper;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final DagInstanceMapper dagInstanceMapper;
    private final DagInstanceRepository dagInstanceRepository;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final ParameterResolver parameterResolver;
    private final AlgorithmContractAdapter algorithmContractAdapter;
    private final DagDefinitionService dagDefinitionService;
    private final DagDispatchGateway dispatchGateway;
    private final TransactionTemplate transactions;
    private final int maxScheduleFailCount;
    private final int noWorkerRetryIntervalSeconds;
    private final int workerLeaseSeconds;

    public DagNodeDispatchService(NodeInstanceMapper nodeInstanceMapper,
                                   NodeInstanceJpaRepository nodeInstanceRepository,
                                   DagInstanceMapper dagInstanceMapper,
                                   DagInstanceRepository dagInstanceRepository,
                                   AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                   ParameterResolver parameterResolver,
                                   AlgorithmContractAdapter algorithmContractAdapter,
                                   DagDefinitionService dagDefinitionService,
                                   DagDispatchGateway dispatchGateway,
                                   TransactionTemplate transactions,
                                   @Value("${staterelay.dag.scheduler.max-schedule-fail-count:10}") int maxScheduleFailCount,
                                   @Value("${staterelay.dag.scheduler.no-worker-retry-interval-seconds:5}") int noWorkerRetryIntervalSeconds,
                                   @Value("${staterelay.dag.scheduler.worker-lease-seconds:30}") int workerLeaseSeconds) {
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.dagInstanceMapper = dagInstanceMapper;
        this.dagInstanceRepository = dagInstanceRepository;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.parameterResolver = parameterResolver;
        this.algorithmContractAdapter = algorithmContractAdapter;
        this.dagDefinitionService = dagDefinitionService;
        this.dispatchGateway = dispatchGateway;
        this.transactions = transactions;
        this.maxScheduleFailCount = maxScheduleFailCount;
        this.noWorkerRetryIntervalSeconds = noWorkerRetryIntervalSeconds;
        this.workerLeaseSeconds = workerLeaseSeconds;
    }

    /**
     * 调度一个 READY 节点（§27 完整流程）。
     *
     * <p>抢占由 {@link DagNodeDispatchScanner} 通过 {@code scanReadyNodes} 完成，
     * 本方法假定 CAS READY → DISPATCHING 已经成功（由 SQL FOR UPDATE SKIP LOCKED 一并完成）。
     *
     * <p>这里只处理调度决策与状态推进；任何异常都被吞掉并由 Scanner 兜底回退 DISPATCHING。
     */
    public void dispatch(NodeInstanceLease lease, Instant now) {
        NodeInstanceEntity node = nodeInstanceRepository.findById(lease.getId()).orElse(null);
        if (node == null) {
            return;
        }
        if (node.getStatus() != NodeInstanceStatus.DISPATCHING) {
            log.debug("NodeInstance {} not in DISPATCHING (status={}), skip",
                lease.getId(), node.getStatus());
            return;
        }

        DagInstanceEntity instance = dagInstanceRepository.findById(lease.getDagInstanceId()).orElse(null);
        if (instance == null) {
            log.warn("DagInstance {} not found for NodeInstance {}", lease.getDagInstanceId(), lease.getId());
            return;
        }

        // 1. 查 AlgorithmDefinition
        String algorithmCode = lease.getHandlerName();
        AlgorithmDefinitionEntity algo = algorithmDefinitionRepository
            .findByAlgorithmCodeAndStatus(algorithmCode, AlgorithmDefinitionStatus.ENABLED)
            .orElse(null);
        if (algo == null) {
            transactions.executeWithoutResult(status ->
                handleAlgorithmNotFound(node, algorithmCode, now));
            return;
        }

        // 2. ParameterResolver 解析（§16 / §17）
        String requestJson;
        boolean algorithmProtocol;
        try {
            algorithmProtocol = isAlgorithmNode(instance, node);
            requestJson = algorithmProtocol
                ? parameterResolver.resolve(instance, node, algorithmContractAdapter.toContract(algo))
                : parameterResolver.resolve(instance, node);
        } catch (InputResolveException ex) {
            transactions.executeWithoutResult(status ->
                handleParameterResolveFailure(node, ex, now));
            return;
        } catch (IllegalArgumentException ex) {
            InputResolveException wrapped = new InputResolveException(
                "Invalid algorithm contract for " + algorithmCode, ex);
            transactions.executeWithoutResult(status ->
                handleParameterResolveFailure(node, wrapped, now));
            return;
        }

        // 3. 在 sr_worker 上原子预留容量、创建 Attempt、更新 current-attempt fence；
        //    reservation 事务提交后才发送真实 HTTP。
        Integer timeoutSeconds = algo.getTimeoutSeconds() == null ? 300 : algo.getTimeoutSeconds();
        DagDispatchCoordinator.Outcome outcome = dispatchGateway.dispatch(
            new DagDispatchStore.ReservationRequest(
                node.getDagInstanceId(), node.getId(), node.getNodeId(), algorithmCode,
                algorithmProtocol, algo.getContractVersion(), algo.getContractChecksum(),
                algo.getExecutorGroupCode(), requestJson,
                computeRequestChecksum(algo.getAlgorithmCode(), requestJson),
                Duration.ofSeconds(workerLeaseSeconds), Duration.ofSeconds(timeoutSeconds)),
            now);
        if (outcome == DagDispatchCoordinator.Outcome.NO_WORKER) {
            transactions.executeWithoutResult(status ->
                handleNoWorker(node, algo, now));
        }
    }

    private boolean isAlgorithmNode(DagInstanceEntity instance, NodeInstanceEntity node) {
        return dagDefinitionService.findVersionEntity(instance.getDagDefinitionVersionId())
            .map(dagDefinitionService::loadSnapshot)
            .flatMap(snapshot -> snapshot.getNodes().stream()
                .filter(candidate -> node.getNodeId().equals(candidate.getId()))
                .findFirst())
            .map(dagNode -> dagNode.getAlgorithmCode() != null && !dagNode.getAlgorithmCode().isBlank())
            .orElseThrow(() -> new InputResolveException(
                "DAG node definition not found for runtime protocol: " + node.getNodeId()));
    }

    /**
     * AlgorithmDefinition 不存在或未 ENABLED：视为配置错误，DISPATCHING → FAILED（§17）。
     */
    private void handleAlgorithmNotFound(NodeInstanceEntity node, String algorithmCode, Instant now) {
        int updated = nodeInstanceMapper.markFailedFromDispatching(
            node.getId(),
            "ALGORITHM_DEFINITION_NOT_FOUND",
            "AlgorithmDefinition not found or not ENABLED: " + algorithmCode,
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            log.error("NodeInstance {} FAILED (algorithm not found: {})", node.getId(), algorithmCode);
        }
    }

    /**
     * 参数解析失败：DISPATCHING → FAILED，不重试（§17）。
     */
    private void handleParameterResolveFailure(NodeInstanceEntity node, InputResolveException ex, Instant now) {
        int updated = nodeInstanceMapper.markFailedFromDispatching(
            node.getId(),
            "PARAMETER_RESOLVE_FAILED",
            ex.getMessage(),
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            log.error("NodeInstance {} FAILED (parameter resolve failed): {}",
                node.getId(), ex.getMessage());
        }
    }

    /**
     * 无可用 Worker（§9.1 / §27 step 5）：
     * <ul>
     *   <li>schedule_fail_count + 1 未达上限：CAS DISPATCHING → READY，退避后重试</li>
     *   <li>已达上限：CAS DISPATCHING → FAILED，+1 finished_node_count</li>
     * </ul>
     */
    private void handleNoWorker(NodeInstanceEntity node, AlgorithmDefinitionEntity algo, Instant now) {
        int newFailCount = (node.getScheduleFailCount() == null ? 0 : node.getScheduleFailCount()) + 1;

        if (newFailCount < maxScheduleFailCount) {
            Instant nextScheduleTime = now.plus(Duration.ofSeconds(noWorkerRetryIntervalSeconds));
            int updated = nodeInstanceMapper.revertToReadyForNoWorker(
                node.getId(),
                nextScheduleTime,
                "NO_AVAILABLE_WORKER",
                "No available worker for executorGroup=" + algo.getExecutorGroupCode(),
                now);
            if (updated > 0) {
                log.info("NodeInstance {} reverted to READY (schedule_fail_count={}/{})",
                    node.getId(), newFailCount, maxScheduleFailCount);
            }
            return;
        }

        // 达到上限：DISPATCHING → FAILED（T6）
        int updated = nodeInstanceMapper.markFailedForNoWorker(
            node.getId(),
            newFailCount,
            "NO_AVAILABLE_WORKER",
            "No available worker after " + newFailCount + " schedule failures",
            now);
        if (updated > 0) {
            dagInstanceMapper.incrementFinishedCount(node.getDagInstanceId(), 1, now);
            log.error("NodeInstance {} FAILED (no available worker, schedule_fail_count={})",
                node.getId(), newFailCount);
        }
    }

    /**
     * 计算业务参数摘要（§44 rebindFence 围栏校验字段）。
     *
     * <p>checksum = SHA-256(algorithmCode + ":" + requestJson)
     * <p>相同 requestId + 不同 checksum 必须 fail-closed 拒绝（防止业务参数变化后旧 requestId 误覆盖）。
     */
    private static String computeRequestChecksum(String algorithmCode, String requestJson) {
        String input = (algorithmCode == null ? "" : algorithmCode)
                + ":"
                + (requestJson == null ? "" : requestJson);
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException ex) {
            // SHA-256 是 JDK 标准算法，理论不会缺失；若发生则用 fallback
            return Integer.toHexString(input.hashCode());
        }
    }
}
