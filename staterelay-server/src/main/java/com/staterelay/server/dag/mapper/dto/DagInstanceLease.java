package com.staterelay.server.dag.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DagOrchestratorScanner 领取的 DAG 实例（行级 FOR UPDATE SKIP LOCKED）。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DagInstanceLease {
    private Long id;
    private String workerId;
    private Long leaseVersion;
    private Instant leaseExpireTime;
    private Long orchestrationVersion;
}
