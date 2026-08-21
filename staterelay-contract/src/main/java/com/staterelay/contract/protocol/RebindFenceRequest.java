package com.staterelay.contract.protocol;

/**
 * Server 端 lease 接管恢复时，请求 Worker Store 升级内部 lease_version 的命令
 * （对齐文档 §19.1 / §44.2 rebindFence 协议）。
 *
 * <p>Worker 端 fail-closed 校验条件（任一不满足则返回 success=false）：
 * <ul>
 *   <li>requestId / requestChecksum / attemptId / workerId 完整围栏匹配</li>
 *   <li>{@code newLeaseVersion > storedLeaseVersion}（严格单调递增，
 *       防旧 Scheduler 在网络分区恢复后反向覆盖已升级的版本）</li>
 *   <li>记录状态 ∈ {RUNNING, SUCCESS, FAILED}（终态也要允许升级，
 *       §47 边界场景：Worker 已 markSuccess 但 Scheduler 未回写时 lease 接管）</li>
 * </ul>
 *
 * <p>校验通过后，Worker Store 原子升级内部 lease_version + worker_epoch；
 * 升级前在途的旧版本响应一律按 §47 围栏不匹配拒绝。
 */
public record RebindFenceRequest(
        String requestId,
        String requestChecksum,
        Long attemptId,
        String workerId,
        String workerEpoch,
        Long newLeaseVersion) {
}
