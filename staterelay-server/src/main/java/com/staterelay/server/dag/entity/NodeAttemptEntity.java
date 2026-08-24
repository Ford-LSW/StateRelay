package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import com.staterelay.server.dag.converter.NodeAttemptStatusConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.Instant;

/**
 * DAG 节点执行尝试（{@code sr_dag_node_attempt}）。
 *
 * <p>每次真实执行产生一条记录。NodeInstance 表示逻辑节点最终状态，
 * NodeAttempt 表示这个节点每一次具体执行。
 *
 * <p>状态机对齐文档：
 * <pre>
 * CREATED → DISPATCHING → ACCEPTED → RUNNING → SUCCESS/FAILED/CANCELLED/TIMEOUT/UNKNOWN
 * </pre>
 *
 * <p><b>lease 与硬截止时间分离（对齐文档 §43）：</b>
 * <ul>
 *   <li>{@link #attemptLeaseVersion}：lease 围栏版本号，单调递增；Scheduler CAS 升级后
 *       通过 rebindFence 同步给 Worker Store；对齐 §19.1 / §44 / §47.1</li>
 *   <li>{@link #attemptLeaseExpireTime}：Worker heartbeat 可续约的 lease 到期时间；
 *       到期只触发 UNKNOWN / 接管恢复，不直接等于 TIMEOUT</li>
 *   <li>{@link #executionDeadlineAt}：T8 创建 Attempt 时由数据库 NOW() + timeout_seconds
 *       一次性固化的不可续约硬截止时间；heartbeat 不能延长；到期无论 lease 是否仍有效，
 *       都进入 TIMEOUT 处理并 best-effort cancel；对齐 §29.4 / §29.5</li>
 *   <li>{@link #workerEpoch}：Worker 重启周期；旧 epoch 的 heartbeat / 取消响应 / 执行结果
 *       一律不能覆盖新 epoch 状态；对齐 §23</li>
 * </ul>
 */
@Data
@Entity
@Table(name = "sr_dag_node_attempt")
public class NodeAttemptEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_instance_id", nullable = false)
    private Long dagInstanceId;

    @Column(name = "node_instance_id", nullable = false)
    private Long nodeInstanceId;

    @Column(name = "attempt_no", nullable = false)
    private Integer attemptNo;

    @Column(name = "request_id", nullable = false, length = 128)
    private String requestId;

    /**
     * 业务参数摘要（对齐文档 §44 / §47.1 rebindFence 围栏校验）。
     *
     * <p>algorithmCode + requestJson 的 SHA-256 摘要。
     * <p>lease 接管恢复时，Server 调用 Worker rebindFence 必须携带本字段；
     * Worker Store 校验"相同 requestId + 相同 checksum"才允许升级 lease_version，
     * 防止业务参数变化后旧 requestId 误覆盖新结果。
     */
    @Column(name = "request_checksum", nullable = false, length = 64)
    private String requestChecksum = "";

    @Column(name = "algorithm_code", nullable = false, length = 128)
    private String algorithmCode;

    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "worker_address", length = 512)
    private String workerAddress;

    /**
     * Worker 重启周期标识（对齐文档 §23）。
     *
     * <p>旧 epoch 的 heartbeat / 取消响应 / 执行结果一律不能覆盖新 epoch 状态。
     * 跨 epoch 恢复时，Scheduler 必须先 CAS 重绑 worker_epoch + 升级 attempt_lease_version，
     * 再通过 rebindFence 同步给 Worker Store（§19.1）。
     */
    @Column(name = "worker_epoch", length = 64)
    private String workerEpoch;

    @Column(name = "dispatch_generation")
    private Long dispatchGeneration;

    @Column(name = "dispatch_token", length = 128)
    private String dispatchToken;

    @Column(name = "capacity_released_at")
    private Instant capacityReleasedAt;

    @Column(name = "transport_generation", nullable = false)
    private Long transportGeneration = 0L;

    @Column(name = "transport_attempts", nullable = false)
    private Integer transportAttempts = 0;

    @Column(name = "next_dispatch_at")
    private Instant nextDispatchAt;

    @Column(name = "last_dispatch_error", columnDefinition = "TEXT")
    private String lastDispatchError;

    /**
     * Attempt lease 围栏版本号（对齐文档 §19.1 / §44 / §47.1）。
     *
     * <p>单调递增：仅当 {@code newLeaseVersion > storedLeaseVersion} 时允许 CAS 升级，
     * 防止旧 Scheduler 在网络分区恢复后反向覆盖已升级的版本。
     *
     * <p>升级流程：
     * <ol>
     *   <li>Scheduler DB CAS：{@code attempt_lease_version} N→N+1</li>
     *   <li>HTTP rebindFence 请求 Worker：校验 requestId / requestChecksum / attemptId /
     *       workerId / workerEpoch 完整围栏 + 单调递增</li>
     *   <li>Worker Store 原子升级内部 lease_version</li>
     *   <li>后续 heartbeat / RUNNING / SUCCESS / FAILED 全部使用新 lease_version</li>
     *   <li>升级前在途的旧版本响应一律按围栏不匹配拒绝（§47）</li>
     * </ol>
     */
    @Column(name = "attempt_lease_version", nullable = false)
    private Long attemptLeaseVersion = 0L;

    /**
     * Attempt lease 到期时间（对齐文档 §43）。
     *
     * <p>Worker heartbeat 可续约；续约时必须 {@code worker_id + worker_epoch +
     * attempt_lease_version + current_attempt_id} 完整围栏匹配（§23）。
     *
     * <p>到期只触发 UNKNOWN / 接管恢复，不直接等于 TIMEOUT。
     */
    @Column(name = "attempt_lease_expire_time")
    private Instant attemptLeaseExpireTime;

    /**
     * 执行硬截止时间（不可续约，对齐文档 §43 / §29.4 / §29.5）。
     *
     * <p>T8 创建 Attempt 时由数据库 {@code NOW() + timeout_seconds} 一次性固化。
     * <p>heartbeat 不能延长；到期无论 lease 是否仍有效，都进入 TIMEOUT 处理并发起 best-effort cancel。
     * <p>§29.4 超时判定：{@code DB_NOW >= execution_deadline_at}；扫描起点状态包含
     * DISPATCHING / RUNNING / UNKNOWN。
     */
    @Column(name = "execution_deadline_at")
    private Instant executionDeadlineAt;

    @Column(name = "status", nullable = false)
    @Convert(converter = NodeAttemptStatusConverter.class)
    private NodeAttemptStatus status = NodeAttemptStatus.CREATED;

    @Column(name = "request_json", columnDefinition = "jsonb")
    private String requestJson;

    @Column(name = "result_json", columnDefinition = "jsonb")
    private String resultJson;

    @Column(name = "result_ref", length = 1000)
    private String resultRef;

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "version", nullable = false)
    private Long version = 0L;

    @Column(name = "dispatched_at")
    private Instant dispatchedAt;

    @Column(name = "accepted_at")
    private Instant acceptedAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
