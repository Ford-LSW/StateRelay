package com.staterelay.contract.dag.algorithm;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * Worker 执行上下文（对齐文档 §12）。
 *
 * <p>Scheduler 派发给 Worker 的请求分成两部分：
 * <pre>
 *   executionContext —— 调度和围栏信息（本对象）
 *   params           —— ParameterResolver 解析后的算法业务参数
 * </pre>
 *
 * <p>Worker Starter 依据 {@code algorithmCode} 从 HandlerRegistry 查找对应
 * {@code AlgorithmExecutor}，把 {@code params} 反序列化成算法参数对象，
 * 同时把本 {@code executionContext} 注入 {@code AlgorithmExecutionContext} 供
 * 算法实现访问调度侧信息（如 dagInstanceId、attemptId、工作目录等）。
 *
 * <p>JSON 示例（文档 §12）：
 * <pre>{@code
 * {
 *   "dagInstanceId": 50001,
 *   "nodeInstanceId": 60002,
 *   "nodeCode": "B",
 *   "attemptId": 70002,
 *   "attemptNo": 1,
 *   "requestId": "abc-002",
 *   "requestChecksum": "sha256:xxx",
 *   "dispatchGeneration": 3,
 *   "dispatchToken": "token-xxx",
 *   "attemptLeaseVersion": 1,
 *   "workerId": "gis-worker-01",
 *   "workerEpoch": "epoch-001"
 * }
 * }</pre>
 *
 * <p><b>围栏字段（文档 §17 完整围栏）：</b>
 * <ul>
 *   <li>{@code requestId} + {@code attemptId} —— 唯一标识本次派发</li>
 *   <li>{@code nodeInstanceId} + {@code current_attempt_id} —— NodeInstance 当前权威</li>
 *   <li>{@code dispatchGeneration} + {@code dispatchToken} —— 调度代次令牌</li>
 *   <li>{@code attemptLeaseVersion} —— 租约版本（rebindFence 协议）</li>
 *   <li>{@code workerId} + {@code workerEpoch} —— Worker 身份与代次</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkerExecutionContext {

    /** 所属 DAG 实例 ID */
    private Long dagInstanceId;

    /** 所属节点实例 ID */
    private Long nodeInstanceId;

    /** 节点编码 */
    private String nodeCode;

    /** 本次 Attempt ID */
    private String attemptId;

    /** Attempt 序号（从 1 开始） */
    private Integer attemptNo;

    /**
     * 请求 ID（dispatch 去重和围栏主键）。
     * 同一 Attempt 内重发复用同一 requestId。
     */
    private String requestId;

    /**
     * 请求 checksum，用于 rebindFence 协议校验业务参数一致性
     * （{@code algorithmCode + requestJson} 的 SHA-256，文档 §19 rebindFence）。
     */
    private String requestChecksum;

    /** 调度代次（每次重新派发自增，文档 §17） */
    private Long dispatchGeneration;

    /** 调度令牌，用于本次派发的额外凭证 */
    private String dispatchToken;

    /** 租约版本（rebindFence 协议中可被恢复者升级，文档 §19） */
    private Long attemptLeaseVersion;

    /** Worker ID */
    private String workerId;

    /** Worker 代次（每次 Worker 重启自增，防止旧 Worker 越权） */
    private String workerEpoch;

    public WorkerExecutionContext(Long dagInstanceId, Long nodeInstanceId, String nodeCode,
                                   String attemptId, Integer attemptNo, String requestId,
                                   Long attemptLeaseVersion, String workerId, String workerEpoch) {
        this.dagInstanceId = Objects.requireNonNull(dagInstanceId, "dagInstanceId");
        this.nodeInstanceId = Objects.requireNonNull(nodeInstanceId, "nodeInstanceId");
        this.nodeCode = Objects.requireNonNull(nodeCode, "nodeCode");
        this.attemptId = Objects.requireNonNull(attemptId, "attemptId");
        this.attemptNo = Objects.requireNonNull(attemptNo, "attemptNo");
        this.requestId = Objects.requireNonNull(requestId, "requestId");
        this.attemptLeaseVersion = Objects.requireNonNull(attemptLeaseVersion, "attemptLeaseVersion");
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.workerEpoch = Objects.requireNonNull(workerEpoch, "workerEpoch");
    }
}
