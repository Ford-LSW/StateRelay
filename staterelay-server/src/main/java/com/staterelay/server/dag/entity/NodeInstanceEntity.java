package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagNodeFanoutStrategy;
import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.converter.DagNodeFanoutStrategyConverter;
import com.staterelay.server.dag.converter.NodeInstanceStatusConverter;
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
 * DAG 节点实例（{@code sr_dag_node_instance}）。
 *
 * <p>状态机对齐文档：
 * <pre>
 * WAITING → READY → DISPATCHING → DISPATCHED → RUNNING → SUCCESS/FAILED/CANCELLED
 * WAITING → SKIPPED（前驱失败链式跳过）
 * RUNNING → TIMEOUT → READY（重试）或 FAILED（超限）
 * </pre>
 *
 * <p>创建时统一初始化为 {@link NodeInstanceStatus#WAITING}，
 * 根节点由 DAG Engine 首轮 CAS 推进到 READY。
 *
 * <p>简单查询走 JPA；状态推进、CAS 走 MyBatis Mapper。
 */
@Data
@Entity
@Table(name = "sr_dag_node_instance")
public class NodeInstanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_instance_id", nullable = false)
    private Long dagInstanceId;

    @Column(name = "node_id", nullable = false, length = 128)
    private String nodeId;

    @Column(name = "node_name_snapshot", length = 256)
    private String nodeNameSnapshot;

    @Column(name = "handler_name_snapshot", nullable = false, length = 128)
    private String handlerNameSnapshot;

    @Column(name = "status", nullable = false)
    @Convert(converter = NodeInstanceStatusConverter.class)
    private NodeInstanceStatus status = NodeInstanceStatus.WAITING;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "current_attempt_no", nullable = false)
    private Integer currentAttemptNo = 0;

    /**
     * Scheduler 已领取 Node 但因没有可用 Worker 而调度失败的次数（§9.1）。
     * 与 {@link #retryCount} 含义严格分开：调度失败不创建 NodeAttempt，不增加 retry_count。
     */
    @Column(name = "schedule_fail_count", nullable = false)
    private Integer scheduleFailCount = 0;

    @Column(name = "last_schedule_error_code", length = 64)
    private String lastScheduleErrorCode;

    @Column(name = "last_schedule_error_message", length = 512)
    private String lastScheduleErrorMessage;

    /**
     * 节点取消原因（USER_CANCELLED / DAG_FAILED / ...），仅在 CANCELLED 状态下有值。
     */
    @Column(name = "cancel_reason", length = 64)
    private String cancelReason;

    @Column(name = "next_schedule_time")
    private Instant nextScheduleTime;

    @Column(name = "dispatch_owner", length = 160)
    private String dispatchOwner;

    @Column(name = "dispatch_lease_expire_time")
    private Instant dispatchLeaseExpireTime;

    @Column(name = "version", nullable = false)
    private Long version = 0L;

    @Column(name = "fanout_strategy", nullable = false)
    @Convert(converter = DagNodeFanoutStrategyConverter.class)
    private DagNodeFanoutStrategy fanoutStrategy = DagNodeFanoutStrategy.SINGLE;

    @Column(name = "input_bindings_snapshot", nullable = false, columnDefinition = "jsonb")
    private String inputBindingsSnapshot = "{}";

    @Column(name = "result_json", columnDefinition = "jsonb")
    private String resultJson;

    @Column(name = "result_ref", length = 1000)
    private String resultRef;

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "waiting_reason", length = 64)
    private String waitingReason;

    @Column(name = "ready_at")
    private Instant readyAt;

    @Column(name = "dispatched_at")
    private Instant dispatchedAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
