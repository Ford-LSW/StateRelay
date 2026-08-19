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

    @Column(name = "algorithm_code", nullable = false, length = 128)
    private String algorithmCode;

    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "worker_address", length = 512)
    private String workerAddress;

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
