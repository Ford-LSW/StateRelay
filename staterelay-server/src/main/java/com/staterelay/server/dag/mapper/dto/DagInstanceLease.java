package com.staterelay.server.dag.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DagOrchestratorScanner 领取的 DAG 实例（行级 FOR UPDATE SKIP LOCKED）。
 *
 * <p>对齐文档：去掉了 orchestration_state / orchestration_version，
 * 由 status 唯一驱动；scanner 只需 id 和租约信息。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DagInstanceLease {
    private Long id;
    private String workerId;
    private Long leaseVersion;
    private Instant leaseExpireTime;
}
