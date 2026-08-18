package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagArtifactType;
import com.staterelay.server.dag.converter.DagArtifactTypeConverter;
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
 * DAG Artifact 引用（{@code sr_dag_artifact}）。
 *
 * <p>由 JPA Repository 处理简单 CRUD；查询按节点查所有产物也走 JPA。
 */
@Data
@Entity
@Table(name = "sr_dag_artifact")
public class DagArtifactEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_instance_id", nullable = false)
    private Long dagInstanceId;

    @Column(name = "node_id", nullable = false, length = 128)
    private String nodeId;

    @Column(name = "output_name", nullable = false, length = 128)
    private String outputName;

    @Column(name = "artifact_type", nullable = false)
    @Convert(converter = DagArtifactTypeConverter.class)
    private DagArtifactType artifactType;

    @Column(name = "storage_type", length = 32)
    private String storageType;

    @Column(name = "uri", columnDefinition = "TEXT")
    private String uri;

    @Column(name = "value_json", columnDefinition = "jsonb")
    private String valueJson;

    @Column(name = "metadata", columnDefinition = "jsonb")
    private String metadata;

    @Column(name = "parent_artifact_ids", columnDefinition = "BIGINT[]")
    private Long[] parentArtifactIds;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}

