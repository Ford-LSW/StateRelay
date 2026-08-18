package com.staterelay.server.dag.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.Instant;

/**
 * Handler 定义元信息（{@code sr_handler_definition}），描述"怎么执行"。
 *
 * <p>发布期静态校验 {@code dag_node.handler} 引用是否存在；
 * 阶段二可扩展为 DispatchScanner 按 handler 能力过滤 Worker 的依据。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sr_handler_definition")
public class HandlerDefinitionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "app_id", nullable = false)
    private Long appId;

    @Column(name = "handler_name", nullable = false, length = 128)
    private String handlerName;

    @Column(name = "handler_version", nullable = false)
    private Integer handlerVersion = 1;

    @Column(name = "implementation_class", nullable = false, length = 512)
    private String implementationClass;

    @Column(name = "description", length = 1000)
    private String description;

    @Column(name = "parameter_schema", columnDefinition = "jsonb")
    private String parameterSchema;

    @Column(name = "result_schema", columnDefinition = "jsonb")
    private String resultSchema;

    @Column(name = "runtime_requirements", columnDefinition = "jsonb")
    private String runtimeRequirements;

    @Column(name = "timeout_seconds")
    private Integer timeoutSeconds;

    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
