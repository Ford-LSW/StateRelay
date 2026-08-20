package com.staterelay.starter.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Worker 端 requestId 去重存储 SPI（对齐文档 §44）。
 *
 * <p>requestId 去重是平台协议；去重记录存在哪里，是 Worker 用户可选择的策略。
 * Worker Starter 在 Handler 之前统一执行去重，业务 Handler 不需要实现"查询当前任务状态"等平台逻辑。
 *
 * <p>支持策略：
 * <ul>
 *   <li>{@link DedupCapability#PROCESS_LOCAL PROCESS_LOCAL}：仅保证当前 Java 进程存活期间去重；
 *       Pod 重启后记录丢失。MEMORY 实现属于此类</li>
 *   <li>{@link DedupCapability#DURABLE DURABLE}：跨进程保留记录（JDBC / Redis / 本地文件 / PVC 等）；
 *       重启后仍能识别原 requestId 和读取已保存终态</li>
 * </ul>
 *
 * <p>实现必须满足协议规则（对齐文档 §44）：
 * <ul>
 *   <li>首次收到 requestId：原子写入 RUNNING 后才能进入 Handler</li>
 *   <li>重复收到 RUNNING：返回正在执行，不重复进入 Handler</li>
 *   <li>重复收到 SUCCESS / FAILED：返回已保存的终态结果</li>
 *   <li>相同 requestId 但 requestChecksum 不同：必须 fail-closed，拒绝执行并告警</li>
 *   <li>retention 必须覆盖 Scheduler 的最大超时、UNKNOWN 等待、重发和人工处理窗口</li>
 *   <li>cleanup 只能自动清理已经确认的终态记录；过期但仍为 RUNNING 的记录不能直接删除后重新执行</li>
 *   <li>MEMORY 不能宣称具备跨进程恢复能力</li>
 * </ul>
 *
 * <p><b>rebindFence 协议（对齐文档 §19.1 / §44 / §47.1）：</b>
 * Attempt lease 接管恢复时，Scheduler 先在 DB CAS 升级 attempt_lease_version，
 * 再调用 {@link #rebindFence} 同步升级 Worker Store 内部 lease_version，然后重发原 requestId。
 *
 * <p>升级规则（fail-closed）：
 * <ol>
 *   <li>{@code newLeaseVersion > storedLeaseVersion}（严格单调递增，防旧 Scheduler 反向覆盖）</li>
 *   <li>requestId / requestChecksum / attemptId / workerId 完整围栏匹配</li>
 *   <li>升级范围适用于 RUNNING / SUCCESS / FAILED 三种状态
 *       （否则升级前 Worker 已 markSuccess 但 Scheduler 未回写的终态结果永远回写不进去）</li>
 *   <li>升级前在途的旧版本响应一律按围栏不匹配拒绝（§47）</li>
 *   <li>升级后 heartbeat / RUNNING / SUCCESS / FAILED 全部使用新 lease_version</li>
 * </ol>
 */
public interface RequestIdStore {

    /**
     * 声明本 Store 的去重能力。
     */
    DedupCapability capability();

    /**
     * 首次接收 requestId（对齐文档 §44 协议规则第 1 条）。
     *
     * <p>原子写入 RUNNING 后才能进入 Handler。重复 requestId 返回 false。
     *
     * @param requestId     任务唯一标识
     * @param requestChecksum 业务参数摘要（algorithmCode + params + 与业务执行相关的固定字段）
     * @param fence         完整围栏（attemptId / workerId / workerEpoch / leaseVersion）
     * @param now           接收时间
     * @return true 表示首次接收成功；false 表示已存在（调用方应改走 {@link #find} 查询状态）
     */
    boolean tryStart(String requestId, String requestChecksum, RequestFence fence, Instant now);

    /**
     * 查询 requestId 当前状态（对齐文档 §44 协议规则第 2/3 条）。
     *
     * <p>重复收到相同 requestId 时调用，返回已保存的 RUNNING / SUCCESS / FAILED 记录。
     */
    Optional<RequestIdRecord> find(String requestId);

    /**
     * 标记 SUCCESS 终态（对齐文档 §44）。
     */
    void markSuccess(String requestId, String resultJson, String resultRef, Instant now);

    /**
     * 标记 FAILED 终态（对齐文档 §44）。
     */
    void markFailed(String requestId, String errorCode, String errorMessage, Instant now);

    /**
     * 升级 Worker Store 内部 lease_version（§19.1 / §44 / §47.1 rebindFence 协议）。
     *
     * <p>fail-closed 条件全部通过才允许升级：
     * <ul>
     *   <li>{@code newFence.leaseVersion() > stored.leaseVersion()}（严格单调递增）</li>
     *   <li>requestId / requestChecksum / attemptId / workerId 完整围栏匹配</li>
     *   <li>记录状态 ∈ {RUNNING, SUCCESS, FAILED}</li>
     * </ul>
     *
     * <p>升级后：
     * <ul>
     *   <li>内部记录的 leaseVersion / workerEpoch 同步更新为 newFence 中的值</li>
     *   <li>后续 heartbeat / find / markSuccess / markFailed 全部使用新 lease_version</li>
     *   <li>升级前在途的旧版本响应一律按围栏不匹配拒绝</li>
     * </ul>
     *
     * @return true 表示升级成功；false 表示前置条件不满足（版本回退、checksum 不一致、围栏不全、状态不合法等）
     */
    boolean rebindFence(String requestId, RequestFence newFence, Instant now);

    /**
     * 清理已确认终态记录（对齐文档 §44）。
     *
     * <p>只能清理已经确认的终态记录（Scheduler 已落库）；
     * 过期但仍为 RUNNING 的记录不能直接删除后重新执行。
     *
     * @param retention 保留时长
     * @param now      当前时间
     * @return 已清理的记录数
     */
    int cleanup(Duration retention, Instant now);

    /**
     * 围栏信息（用于 tryStart / rebindFence 校验）。
     *
     * @param attemptId    NodeAttempt ID
     * @param workerId     Worker ID
     * @param workerEpoch  Worker 重启周期
     * @param leaseVersion Attempt lease 围栏版本号
     */
    final class RequestFence {
        private final Long attemptId;
        private final String workerId;
        private final String workerEpoch;
        private final Long leaseVersion;

        public RequestFence(Long attemptId, String workerId, String workerEpoch, Long leaseVersion) {
            this.attemptId = attemptId;
            this.workerId = workerId;
            this.workerEpoch = workerEpoch;
            this.leaseVersion = leaseVersion;
        }

        public Long attemptId() {
            return attemptId;
        }

        public String workerId() {
            return workerId;
        }

        public String workerEpoch() {
            return workerEpoch;
        }

        public Long leaseVersion() {
            return leaseVersion;
        }

        @Override
        public String toString() {
            return "RequestFence{attemptId=" + attemptId + ", workerId=" + workerId
                    + ", workerEpoch=" + workerEpoch + ", leaseVersion=" + leaseVersion + "}";
        }
    }

    /**
     * Store 内部记录。
     */
    final class RequestIdRecord {
        private final String requestId;
        private final String requestChecksum;
        private final Long attemptId;
        private final String workerId;
        private final String workerEpoch;
        private final Long leaseVersion;
        private final RequestState state;
        private final Instant createdAt;
        private final Instant updatedAt;
        private final String resultJson;
        private final String resultRef;
        private final String errorCode;
        private final String errorMessage;

        public RequestIdRecord(String requestId, String requestChecksum, Long attemptId,
                               String workerId, String workerEpoch, Long leaseVersion,
                               RequestState state, Instant createdAt, Instant updatedAt,
                               String resultJson, String resultRef,
                               String errorCode, String errorMessage) {
            this.requestId = requestId;
            this.requestChecksum = requestChecksum;
            this.attemptId = attemptId;
            this.workerId = workerId;
            this.workerEpoch = workerEpoch;
            this.leaseVersion = leaseVersion;
            this.state = state;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
            this.resultJson = resultJson;
            this.resultRef = resultRef;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        public String requestId() {
            return requestId;
        }

        public String requestChecksum() {
            return requestChecksum;
        }

        public Long attemptId() {
            return attemptId;
        }

        public String workerId() {
            return workerId;
        }

        public String workerEpoch() {
            return workerEpoch;
        }

        public Long leaseVersion() {
            return leaseVersion;
        }

        public RequestState state() {
            return state;
        }

        public Instant createdAt() {
            return createdAt;
        }

        public Instant updatedAt() {
            return updatedAt;
        }

        public String resultJson() {
            return resultJson;
        }

        public String resultRef() {
            return resultRef;
        }

        public String errorCode() {
            return errorCode;
        }

        public String errorMessage() {
            return errorMessage;
        }
    }

    /**
     * 内部记录状态。
     */
    enum RequestState {
        RUNNING,
        SUCCESS,
        FAILED
    }

    /**
     * 去重能力（对齐文档 §44）。
     */
    enum DedupCapability {
        /**
         * 仅保证当前 Java 进程存活期间去重；Pod 重启后记录丢失。
         */
        PROCESS_LOCAL,
        /**
         * 跨进程保留记录；重启后仍能识别原 requestId 和读取已保存终态。
         */
        DURABLE
    }
}
