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
 * {@code executorGroupCode}，再从统一的 {@code sr_worker} 注册表中选择存活 Worker。
 *
 * <p><b>不可修改约束（对齐文档 §10）：</b>
 * 一旦 DagInstance 开始执行，本表行记录（包括 timeout_seconds / max_retry / retry_interval_seconds /
 * executor_group_code / input_schema_json / output_schema_json）在本次执行期间<b>禁止修改</b>。
 *
 * <p>第一版通过操作纪律约束（运维/发布流程）：
 * <ul>
 *   <li>修改 AlgorithmDefinition 必须先停掉所有依赖此 algorithmCode 的 DagInstance</li>
 *   <li>正式版本变更通过新建 AlgorithmDefinition 行 + 切换 algorithmCode 别名实现，避免原地修改</li>
 *   <li>如果运维违反约束，NodeAttemptSyncService / DagNodeDispatchService 读取的字段值可能前后不一致，
 *       导致 retry 计数错乱或超时窗口偏差，但不会破坏数据完整性</li>
 * </ul>
 *
 * <p>第二版可引入 {@code version} 字段 + DagInstance 启动时 snapshot 冗余字段，从机制层面消除风险。
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

    /**
     * 契约版本号（§3.2）。Worker 心跳上报 contractVersion 供校验，
     * 不一致时该 Worker 不得接收该算法的新任务 + 记录 ALGORITHM_CONTRACT_MISMATCH 告警。
     */
    @Column(name = "contract_version", nullable = false)
    private String contractVersion = "1.0";

    /**
     * 契约 checksum（§3.2，contractVersion + inputs + outputs 序列化哈希）。
     * 可空，空时校验降级为版本号匹配；非空时与 Worker 上报严格匹配。
     */
    @Column(name = "contract_checksum", length = 128)
    private String contractChecksum;

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
