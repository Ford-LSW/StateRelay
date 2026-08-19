package com.staterelay.server.dag.entity;

import com.staterelay.contract.dag.enums.ExecutorRegistrationStatus;
import com.staterelay.server.dag.converter.ExecutorRegistrationStatusConverter;
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
 * Worker 注册信息（{@code sr_executor_registration}）。
 *
 * <p>表示当前某个 executorGroup 下有哪些 Worker 存活。
 * Scheduler 选择 Worker 时需要满足：
 * <ul>
 *   <li>executor_group_code 匹配</li>
 *   <li>status = ONLINE</li>
 *   <li>lease_expire_time > NOW()</li>
 *   <li>capabilities_json 包含 algorithmCode</li>
 * </ul>
 */
@Data
@Entity
@Table(name = "sr_executor_registration")
public class ExecutorRegistrationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "executor_group_code", nullable = false, length = 128)
    private String executorGroupCode;

    @Column(name = "worker_id", nullable = false, length = 160)
    private String workerId;

    @Column(name = "address", nullable = false, length = 512)
    private String address;

    @Column(name = "capabilities_json", nullable = false, columnDefinition = "jsonb")
    private String capabilitiesJson = "[]";

    @Column(name = "status", nullable = false)
    @Convert(converter = ExecutorRegistrationStatusConverter.class)
    private ExecutorRegistrationStatus status = ExecutorRegistrationStatus.OFFLINE;

    @Column(name = "max_concurrency", nullable = false)
    private Integer maxConcurrency = 1;

    @Column(name = "running_task_count", nullable = false)
    private Integer runningTaskCount = 0;

    @Column(name = "weight", nullable = false)
    private Integer weight = 1;

    @Column(name = "registered_at", nullable = false)
    private Instant registeredAt;

    @Column(name = "last_heartbeat_time")
    private Instant lastHeartbeatTime;

    @Column(name = "lease_expire_time")
    private Instant leaseExpireTime;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
