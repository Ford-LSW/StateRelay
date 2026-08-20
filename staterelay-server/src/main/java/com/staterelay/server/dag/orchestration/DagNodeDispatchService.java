package com.staterelay.server.dag.orchestration;

import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.contract.dag.enums.ExecutorRegistrationStatus;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.ExecutorRegistrationEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeAttemptMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.mapper.dto.NodeInstanceLease;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.ExecutorRegistrationRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * DAG 节点调度 Service（对齐文档 §26 / §27）。
 *
 * <p>由 {@link DagNodeDispatchScanner} 调用，完成从 READY 节点到 Worker 调度的完整链路：
 * <ol>
 *   <li>查询 NodeInstance + DagInstance + AlgorithmDefinition</li>
 *   <li>调用 {@link ParameterResolver#resolve} 解析参数（失败则 DISPATCHING → FAILED，§17）</li>
 *   <li>查 {@link ExecutorRegistrationEntity} 选 Worker</li>
 *   <li>无 Worker：
 *      <ul>
 *          <li>schedule_fail_count 未达上限：CAS DISPATCHING → READY，不创建 Attempt</li>
 *          <li>schedule_fail_count 已达上限：CAS DISPATCHING → FAILED，+1 finished_node_count</li>
 *      </ul>
 *   </li>
 *   <li>有 Worker：插入 NodeAttempt（同步模式直接 RUNNING），CAS NodeInstance DISPATCHING → RUNNING</li>
 * </ol>
 *
 * <p>第一版不实现 Worker HTTP 调用：仅持久化 NodeAttempt，由后续 Worker 状态回报触发推进。
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
    private final NodeAttemptMapper nodeAttemptMapper;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final ExecutorRegistrationRepository executorRegistrationRepository;
    private final ParameterResolver parameterResolver;
    private final TransactionTemplate transactions;
    private final int maxScheduleFailCount;
    private final int noWorkerRetryIntervalSeconds;
    private final int workerLeaseSeconds;

    public DagNodeDispatchService(NodeInstanceMapper nodeInstanceMapper,
                                   NodeInstanceJpaRepository nodeInstanceRepository,
                                   DagInstanceMapper dagInstanceMapper,
                                   DagInstanceRepository dagInstanceRepository,
                                   NodeAttemptMapper nodeAttemptMapper,
                                   AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                   ExecutorRegistrationRepository executorRegistrationRepository,
                                   ParameterResolver parameterResolver,
                                   TransactionTemplate transactions,
                                   @Value("${staterelay.dag.scheduler.max-schedule-fail-count:10}") int maxScheduleFailCount,
                                   @Value("${staterelay.dag.scheduler.no-worker-retry-interval-seconds:5}") int noWorkerRetryIntervalSeconds,
                                   @Value("${staterelay.dag.scheduler.worker-lease-seconds:30}") int workerLeaseSeconds) {
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.dagInstanceMapper = dagInstanceMapper;
        this.dagInstanceRepository = dagInstanceRepository;
        this.nodeAttemptMapper = nodeAttemptMapper;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.executorRegistrationRepository = executorRegistrationRepository;
        this.parameterResolver = parameterResolver;
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
        try {
            requestJson = parameterResolver.resolve(instance, node);
        } catch (InputResolveException ex) {
            transactions.executeWithoutResult(status ->
                handleParameterResolveFailure(node, ex, now));
            return;
        }

        // 3. 查 ExecutorRegistration 找 Worker
        ExecutorRegistrationEntity worker = selectWorker(algo.getExecutorGroupCode(), now);
        if (worker == null) {
            transactions.executeWithoutResult(status ->
                handleNoWorker(node, algo, now));
            return;
        }

        // 4. 有 Worker：创建 NodeAttempt + CAS NodeInstance DISPATCHING → RUNNING（同步模式，T5）
        final String finalRequestJson = requestJson;
        transactions.executeWithoutResult(status ->
            executeWithWorker(node, algo, worker, finalRequestJson, now));
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
     * 有 Worker：创建 NodeAttempt + CAS DISPATCHING → RUNNING（同步模式，T5/T8）。
     *
     * <p>对齐文档 §27 / §43：
     * <ul>
     *   <li>固定 requestId（后续重发 / rebindFence 复用）</li>
     *   <li>attempt_lease_version = 1（首次创建）</li>
     *   <li>attempt_lease_expire_time = now + workerLeaseSeconds（heartbeat 可续约）</li>
     *   <li>execution_deadline_at = now + timeout_seconds（不可续约硬截止，T8 创建时一次性固化）</li>
     *   <li>worker_epoch 取自 ExecutorRegistration（旧 epoch 一律不能覆盖）</li>
     * </ul>
     *
     * <p>第一版 NodeAttempt 在同步模式下直接进入 RUNNING，跳过 DISPATCHING/ACCEPTED 中间状态。
     * HTTP 调用由后续 Worker 状态回报模块处理；容量拒绝回报走 T6C 事务（§27.2）。
     */
    private void executeWithWorker(NodeInstanceEntity node, AlgorithmDefinitionEntity algo,
                                   ExecutorRegistrationEntity worker, String requestJson, Instant now) {
        Integer currentAttemptNo = (node.getCurrentAttemptNo() == null ? 0 : node.getCurrentAttemptNo()) + 1;
        String requestId = UUID.randomUUID().toString();

        // T8：固定 requestId + lease 围栏 + 不可续约硬截止（§27 / §43）
        Long attemptLeaseVersion = 1L;
        Instant attemptLeaseExpireTime = now.plus(Duration.ofSeconds(workerLeaseSeconds));
        Integer timeoutSeconds = algo.getTimeoutSeconds() != null ? algo.getTimeoutSeconds() : 300;
        Instant executionDeadlineAt = now.plus(Duration.ofSeconds(timeoutSeconds));

        NodeAttemptMapper.NodeAttemptInsert attemptInsert = new NodeAttemptMapper.NodeAttemptInsert(
            node.getDagInstanceId(),
            node.getId(),
            currentAttemptNo,
            requestId,
            algo.getAlgorithmCode(),
            worker.getWorkerId(),
            worker.getAddress(),
            worker.getWorkerEpoch(),
            requestJson,
            attemptLeaseVersion,
            attemptLeaseExpireTime,
            executionDeadlineAt);
        nodeAttemptMapper.insert(attemptInsert, now);

        // CAS NodeInstance DISPATCHING → RUNNING（同步模式跳过 DISPATCHED，§11）
        int updated = nodeInstanceMapper.markRunning(node.getId(), now);
        if (updated == 0) {
            log.warn("NodeInstance {} CAS DISPATCHING → RUNNING failed (state changed by other thread)",
                node.getId());
            return;
        }
        log.info("NodeInstance {} dispatched to worker {} (attemptNo={}, algorithm={}, leaseVersion={}, deadline={})",
            node.getId(), worker.getWorkerId(), currentAttemptNo, algo.getAlgorithmCode(),
            attemptLeaseVersion, executionDeadlineAt);
    }

    /**
     * 第一版 Worker 选择策略：从 ONLINE + lease 未过期的 Worker 中选第一个。
     *
     * <p>后续可扩展为 PowerOfTwoChoices / 加权选择 / capabilities 过滤。
     * capabilities 校验第一版暂不实现（依赖 AlgorithmDefinition 与 Worker capabilities 的协议）。
     */
    private ExecutorRegistrationEntity selectWorker(String executorGroupCode, Instant now) {
        List<ExecutorRegistrationEntity> workers = executorRegistrationRepository
            .findByExecutorGroupCodeAndStatus(executorGroupCode, ExecutorRegistrationStatus.ONLINE);
        return workers.stream()
            .filter(w -> w.getLeaseExpireTime() != null && w.getLeaseExpireTime().isAfter(now))
            .filter(w -> w.getMaxConcurrency() != null && w.getMaxConcurrency() > 0)
            .findFirst()
            .orElse(null);
    }
}
