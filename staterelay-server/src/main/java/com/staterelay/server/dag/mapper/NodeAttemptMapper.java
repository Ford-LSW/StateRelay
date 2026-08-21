package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 节点执行尝试 MyBatis Mapper。
 *
 * <p>对应 XML：{@code resources/mapper/NodeAttemptMapper.xml}
 *
 * <p>状态机对齐文档：
 * <ul>
 *   <li>CREATED(0) → DISPATCHING(10) → ACCEPTED(20) → RUNNING(30) → SUCCESS(40)/FAILED(50)/TIMEOUT(70)/UNKNOWN(80)</li>
 * </ul>
 */
@Mapper
public interface NodeAttemptMapper {

    /**
     * 插入新的 NodeAttempt 记录。
     */
    int insert(@Param("attempt") NodeAttemptInsert attempt, @Param("now") Instant now);

    /**
     * CAS: CREATED(0) → DISPATCHING(10)。
     */
    int markDispatching(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(10) → ACCEPTED(20)（Worker 确认接收）。
     */
    int markAccepted(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: ACCEPTED(20) → RUNNING(30)（Worker 开始执行）。
     */
    int markRunning(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → SUCCESS(40)。
     */
    int markSuccess(@Param("attemptId") Long attemptId,
                    @Param("resultJson") String resultJson,
                    @Param("resultRef") String resultRef,
                    @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(10) / RUNNING(30) / UNKNOWN(80) → SUCCESS(40)，
     * 带 lease_version 围栏校验（对齐文档 §19.1 / §47.1）。
     *
     * <p>由 {@link com.staterelay.server.dag.orchestration.NodeAttemptLeaseRecoverScanner}
     * 在 lease 接管恢复收到 Worker SUCCESS 响应时调用。
     *
     * <p>围栏条件：{@code attempt_lease_version = #{leaseVersion}}（防旧版本反向覆盖）。
     */
    int markSuccessFromRecovery(@Param("attemptId") Long attemptId,
                                @Param("leaseVersion") Long leaseVersion,
                                @Param("resultJson") String resultJson,
                                @Param("resultRef") String resultRef,
                                @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → FAILED(50)。
     */
    int markFailed(@Param("attemptId") Long attemptId,
                   @Param("errorCode") String errorCode,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(10) / RUNNING(30) / UNKNOWN(80) → FAILED(50)，
     * 带 lease_version 围栏校验（对齐文档 §19.1 / §47.1）。
     *
     * <p>由 {@link com.staterelay.server.dag.orchestration.NodeAttemptLeaseRecoverScanner}
     * 在 lease 接管恢复收到 Worker FAILED 响应时调用。
     */
    int markFailedFromRecovery(@Param("attemptId") Long attemptId,
                               @Param("leaseVersion") Long leaseVersion,
                               @Param("errorCode") String errorCode,
                               @Param("errorMessage") String errorMessage,
                               @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → TIMEOUT(70)（Scheduler 超时扫描）。
     */
    int markTimeout(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: → UNKNOWN(80)（网络超时无法确认结果）。
     */
    int markUnknown(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * 扫描 RUNNING(30) 且超时的 Attempt（Scheduler 超时检测，§29.4）。
     *
     * <p>对齐文档 §29.4：判定条件改为 {@code NOW() >= execution_deadline_at}（基于不可续约硬截止），
     * 不再依赖 {@code started_at + timeout_seconds}。
     * 扫描起点状态包含 DISPATCHING(10) / RUNNING(30) / UNKNOWN(80)，
     * 三者只要硬截止已到期都需要收敛为 TIMEOUT。
     */
    List<Long> scanTimeoutAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 扫描 Attempt lease 到期但硬截止未到期的 Attempt（§29.5 / §43 / §19.1）。
     *
     * <p>对齐文档 §43：lease 到期只触发 UNKNOWN / 接管恢复，不直接等于 TIMEOUT。
     * 只有硬截止未到期时，才进入 UNKNOWN 接管恢复流程（{@link NodeAttemptRecoverService}）。
     *
     * <p>扫描起点状态：DISPATCHING(10) / RUNNING(30) / UNKNOWN(80)；
     * 条件：{@code attempt_lease_expire_time < NOW() AND execution_deadline_at > NOW()}。
     */
    List<Long> scanLeaseExpiredAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 扫描终态但未处理的 Attempt（DAG Engine 推进 NodeInstance）。
     */
    List<Long> scanTerminalAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 扫描 UNKNOWN(80) 且达到收敛窗口阈值的 Attempt（§19.1 / §29.5）。
     *
     * <p>对齐文档 §19.1：UNKNOWN 状态在 2×timeout_seconds 后未收到任何反馈，
     * 则视为最终 TIMEOUT，避免永久卡住。
     *
     * <p>调用方根据 AlgorithmDefinition.timeout_seconds 计算截止时间：
     * {@code now - finished_at > 2 * timeout_seconds}
     */
    List<Long> scanUnknownAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * CAS: UNKNOWN(80) → TIMEOUT(70)（UNKNOWN 收敛，§19.1）。
     *
     * <p>调用方在确认达到收敛窗口后调用，使 NodeAttemptSyncService 能将其作为 TIMEOUT 处理。
     */
    int markUnknownAsTimeout(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * T8A：Worker heartbeat 续约 Attempt lease（对齐文档 §23 / §27 / §43）。
     *
     * <p>必须完整围栏校验：{@code attempt_id + attempt_lease_version + worker_id + worker_epoch} 全部匹配。
     * 仅续约 {@code attempt_lease_expire_time}；显式不修改 {@code execution_deadline_at}。
     *
     * <p>同步模式：若 Attempt 仍处于 DISPATCHING(10) 或 UNKNOWN(80)，顺带推进到 RUNNING(30)
     * （heartbeat 证明 Worker 已开始执行）；首次 RUNNING 时写 {@code started_at} 供审计。
     *
     * <p>调用方应在同事务内调 {@link NodeInstanceMapper#resetScheduleFailCount}，
     * 把 {@code schedule_fail_count} 重置为 0（§9.1 重置时机之一：heartbeat 证明 Handler 已运行）。
     *
     * @return 受影响行数；0 表示围栏不匹配或状态不合法
     */
    int renewLease(@Param("attemptId") Long attemptId,
                   @Param("leaseVersion") Long leaseVersion,
                   @Param("workerId") String workerId,
                   @Param("workerEpoch") String workerEpoch,
                   @Param("newLeaseExpireTime") Instant newLeaseExpireTime,
                   @Param("now") Instant now);

    /**
     * CAS 升级 Attempt lease_version（§19.1 / §44 / §47.1 rebindFence 协议）。
     *
     * <p>Scheduler 在 lease 接管恢复时调用：
     * <ol>
     *   <li>本方法 DB CAS 升级 {@code attempt_lease_version} N→N+1，重绑 {@code worker_epoch}</li>
     *   <li>调 Worker rebindFence 同步升级 Worker Store 内部 lease_version</li>
     *   <li>使用新 lease_version 重发原 requestId 或查询状态</li>
     * </ol>
     *
     * <p>升级前置条件（fail-closed）：
     * <ul>
     *   <li>{@code newLeaseVersion > currentLeaseVersion}（严格单调递增，防旧 Scheduler 反向覆盖）</li>
     *   <li>{@code attemptId} 匹配</li>
     *   <li>状态 ∈ {DISPATCHING(10), RUNNING(30), UNKNOWN(80)}（终态不允许升级以避免重启已完成 Attempt）</li>
     * </ul>
     *
     * @return 受影响行数；0 表示前置条件不满足
     */
    int incrementLeaseVersion(@Param("attemptId") Long attemptId,
                              @Param("currentLeaseVersion") Long currentLeaseVersion,
                              @Param("newLeaseVersion") Long newLeaseVersion,
                              @Param("newWorkerEpoch") String newWorkerEpoch,
                              @Param("newLeaseExpireTime") Instant newLeaseExpireTime,
                              @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(10) → UNKNOWN(80)（lease 到期，§29.5）。
     *
     * <p>用于 lease 到期但硬截止未到期时把 DISPATCHING 推进为 UNKNOWN，进入 §19.1 接管恢复。
     */
    int markUnknownFromLeaseExpired(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * 用于插入的参数。
     */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class NodeAttemptInsert {
        private Long dagInstanceId;
        private Long nodeInstanceId;
        private Integer attemptNo;
        private String requestId;
        private String requestChecksum;
        private String algorithmCode;
        private String workerId;
        private String workerAddress;
        private String workerEpoch;
        private String requestJson;
        private Long attemptLeaseVersion;
        private Instant attemptLeaseExpireTime;
        private Instant executionDeadlineAt;
    }
}
