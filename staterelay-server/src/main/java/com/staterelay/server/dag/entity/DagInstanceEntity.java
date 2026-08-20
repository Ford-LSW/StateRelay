package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.server.dag.converter.DagInstanceStatusConverter;
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
 * DAG 实例（{@code sr_dag_instance}）。
 *
 * <p>状态机对齐文档 §33.1 / §37.2：
 * <pre>
 * INIT → RUNNING → SUCCESS
 * RUNNING → CANCELLING → CANCELLED
 * RUNNING → FAILING → FAILED
 * </pre>
 * 去掉了 orchestration_state 子状态机，由 status 唯一驱动。
 *
 * <p>简单查询走 JPA；租约抢占、CAS 推进走 MyBatis Mapper。
 */
@Data
@Entity
@Table(name = "sr_dag_instance")
public class DagInstanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_definition_version_id", nullable = false)
    private Long dagDefinitionVersionId;

    @Column(name = "dag_code_snapshot", nullable = false, length = 128)
    private String dagCodeSnapshot;

    @Column(name = "dag_version_snapshot", nullable = false)
    private Integer dagVersionSnapshot;

    @Column(name = "definition_hash_snapshot", nullable = false, length = 64)
    private String definitionHashSnapshot;

    @Column(name = "business_id", length = 128)
    private String businessId;

    @Column(name = "idempotency_key", nullable = false, length = 128)
    private String idempotencyKey;

    @Column(name = "input_json", nullable = false, columnDefinition = "jsonb")
    private String inputJson;

    @Column(name = "status", nullable = false)
    @Convert(converter = DagInstanceStatusConverter.class)
    private DagInstanceStatus status = DagInstanceStatus.INIT;

    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "lease_version", nullable = false)
    private Long leaseVersion = 0L;

    @Column(name = "heartbeat_time")
    private Instant heartbeatTime;

    @Column(name = "lease_expire_time")
    private Instant leaseExpireTime;

    @Column(name = "last_progress_time")
    private Instant lastProgressTime;

    @Column(name = "total_node_count", nullable = false)
    private Integer totalNodeCount = 0;

    /**
     * 已完成节点数（任何终态都 +1）。
     * <p>对齐文档 §39：NodeInstance 第一次进入终态时同事务 +1。
     * 重试 / 无 Worker 未达上限的调度失败不触发 +1。
     */
    @Column(name = "finished_node_count", nullable = false)
    private Integer finishedNodeCount = 0;

    @Column(name = "success_node_count", nullable = false)
    private Integer successNodeCount = 0;

    @Column(name = "failed_node_count", nullable = false)
    private Integer failedNodeCount = 0;

    @Column(name = "skipped_node_count", nullable = false)
    private Integer skippedNodeCount = 0;

    /**
     * 取消原因（USER_CANCELLED），仅在 CANCELLING / CANCELLED 状态下有值。
     */
    @Column(name = "cancel_reason", length = 64)
    private String cancelReason;

    @Column(name = "next_schedule_time", nullable = false)
    private Instant nextScheduleTime;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
