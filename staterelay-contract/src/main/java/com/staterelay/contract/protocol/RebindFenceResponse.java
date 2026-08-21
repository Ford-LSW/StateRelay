package com.staterelay.contract.protocol;

/**
 * Worker 端 rebindFence 响应（对齐文档 §19.1 / §44.2）。
 *
 * <p>{@code success=true} 表示 Worker Store 已升级到 newLeaseVersion；
 * {@code success=false} 表示前置条件不满足（版本回退、checksum 不一致、
 * 围栏不全、状态不合法、记录不存在等），reason 字段说明失败原因。
 *
 * <p>成功时 currentState / currentLeaseVersion 反映升级后的状态；
 * 失败时反映当前 Worker Store 实际状态（便于 Server 端诊断）。
 */
public record RebindFenceResponse(
        boolean success,
        String currentState,
        Long currentLeaseVersion,
        String reason) {

    public static RebindFenceResponse failed(String reason) {
        return new RebindFenceResponse(false, null, null, reason);
    }

    public static RebindFenceResponse notFound(String requestId) {
        return new RebindFenceResponse(false, null, null,
                "requestId not found in Worker Store: " + requestId);
    }
}
