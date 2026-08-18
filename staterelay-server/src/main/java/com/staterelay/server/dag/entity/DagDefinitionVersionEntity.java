package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import com.staterelay.server.dag.converter.DagDefinitionVersionStatusConverter;
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
 * DAG 定义版本（{@code sr_dag_definition_version}），发布后不可变。
 */
@Data
@Entity
@Table(name = "sr_dag_definition_version")
public class DagDefinitionVersionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dag_definition_id", nullable = false)
    private Long dagDefinitionId;

    @Column(name = "version_no", nullable = false)
    private Integer versionNo;

    @Column(name = "status", nullable = false)
    @Convert(converter = DagDefinitionVersionStatusConverter.class)
    private DagDefinitionVersionStatus status = DagDefinitionVersionStatus.DRAFT;

    @Column(name = "definition_snapshot", nullable = false, columnDefinition = "jsonb")
    private String definitionSnapshot;

    @Column(name = "definition_hash", nullable = false, length = 64)
    private String definitionHash;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}

