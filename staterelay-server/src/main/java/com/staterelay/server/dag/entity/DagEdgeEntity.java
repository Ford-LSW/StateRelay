package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagEdgeType;
import com.staterelay.server.dag.converter.DagEdgeTypeConverter;
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
 * DAG 边定义（{@code sr_dag_edge}）。
 */
@Data
@Entity
@Table(name = "sr_dag_edge")
public class DagEdgeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_definition_version_id", nullable = false)
    private Long dagDefinitionVersionId;

    @Column(name = "from_node_id", nullable = false, length = 128)
    private String fromNodeId;

    @Column(name = "to_node_id", nullable = false, length = 128)
    private String toNodeId;

    @Column(name = "edge_type", nullable = false)
    @Convert(converter = DagEdgeTypeConverter.class)
    private DagEdgeType edgeType;

    @Column(name = "condition_expr", length = 500)
    private String conditionExpr;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}

