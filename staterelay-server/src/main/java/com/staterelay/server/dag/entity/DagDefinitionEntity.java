package com.staterelay.server.dag.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.Instant;

/**
 * DAG 定义元信息（{@code sr_dag_definition}）。
 *
 * <p>简单 CRUD 由 JPA Repository 处理；版本流转涉及事务和 CAS，由 MyBatis Mapper 处理。
 */
@Data
@Entity
@Table(name = "sr_dag_definition")
public class DagDefinitionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "app_id", nullable = false)
    private Long appId;

    @Column(name = "dag_code", nullable = false, length = 128)
    private String dagCode;

    @Column(name = "dag_name", nullable = false, length = 256)
    private String dagName;

    @Column(name = "description", length = 1000)
    private String description;

    /**
     * 完整DAG定义
     */
    @Column(name = "def_json", columnDefinition = "jsonb")
    private String definitionJson;

    @Column(name = "enabled", nullable = false)
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
