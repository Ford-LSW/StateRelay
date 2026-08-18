package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagNodeExecutionOrchestrationState;
import com.staterelay.contract.dag.enums.DagNodeExecutionStatus;
import com.staterelay.contract.dag.enums.DagNodeFanoutStrategy;
import com.staterelay.server.dag.converter.DagNodeExecutionOrchestrationStateConverter;
import com.staterelay.server.dag.converter.DagNodeExecutionStatusConverter;
import com.staterelay.server.dag.converter.DagNodeFanoutStrategyConverter;
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
 * DAG 节点执行实例（{@code sr_dag_node_execution}）。
 *
 * <p>简单查询走 JPA；状态推进、CAS、binding 快照更新走 MyBatis Mapper。
 */
@Data
@Entity
@Table(name = "sr_dag_node_execution")
public class DagNodeExecutionEntity {

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
    @Convert(converter = DagNodeExecutionStatusConverter.class)
    private DagNodeExecutionStatus status = DagNodeExecutionStatus.PENDING;

    @Column(name = "orchestration_state", nullable = false)
    @Convert(converter = DagNodeExecutionOrchestrationStateConverter.class)
    private DagNodeExecutionOrchestrationState orchestrationState = DagNodeExecutionOrchestrationState.PENDING;

    @Column(name = "orchestration_version", nullable = false)
    private Long orchestrationVersion = 0L;

    @Column(name = "orchestration_deadline")
    private Instant orchestrationDeadline;

    @Column(name = "orchestration_updated_at")
    private Instant orchestrationUpdatedAt;

    @Column(name = "task_instance_id")
    private Long taskInstanceId;

    @Column(name = "fanout_strategy", nullable = false)
    @Convert(converter = DagNodeFanoutStrategyConverter.class)
    private DagNodeFanoutStrategy fanoutStrategy = DagNodeFanoutStrategy.SINGLE;

    @Column(name = "input_bindings_snapshot", nullable = false, columnDefinition = "jsonb")
    private String inputBindingsSnapshot = "{}";

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "waiting_reason", length = 64)
    private String waitingReason;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}

