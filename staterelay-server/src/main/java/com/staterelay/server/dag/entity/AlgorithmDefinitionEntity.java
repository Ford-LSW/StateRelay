package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.server.dag.converter.AlgorithmDefinitionStatusConverter;
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
 * 算法定义（{@code sr_algorithm_definition}）。
 *
 * <p>表示系统具备的一种可调度能力。Scheduler 根据 {@code algorithmCode} 查到
 * {@code executorGroupCode}，再从 ExecutorRegistration 中选择存活 Worker。
 */
@Data
@Entity
@Table(name = "sr_algorithm_definition")
public class AlgorithmDefinitionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "algorithm_code", nullable = false, length = 128)
    private String algorithmCode;

    @Column(name = "algorithm_name", nullable = false, length = 256)
    private String algorithmName;

    @Column(name = "executor_group_code", nullable = false, length = 128)
    private String executorGroupCode;

    @Column(name = "invoke_mode", nullable = false, length = 32)
    private String invokeMode = "SYNC";

    @Column(name = "timeout_seconds", nullable = false)
    private Integer timeoutSeconds = 300;

    @Column(name = "max_retry", nullable = false)
    private Integer maxRetry = 0;

    @Column(name = "retry_interval_seconds", nullable = false)
    private Integer retryIntervalSeconds = 10;

    @Column(name = "input_schema_json", columnDefinition = "jsonb")
    private String inputSchemaJson;

    @Column(name = "output_schema_json", columnDefinition = "jsonb")
    private String outputSchemaJson;

    @Column(name = "status", nullable = false)
    @Convert(converter = AlgorithmDefinitionStatusConverter.class)
    private AlgorithmDefinitionStatus status = AlgorithmDefinitionStatus.DISABLED;

    @Column(name = "description", length = 1000)
    private String description;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
