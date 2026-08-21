package com.staterelay.contract.protocol;

/**
 * Worker 端 requestId 状态查询响应（对齐文档 §19.1 / §44）。
 *
 * <p>用于 lease 接管恢复时"使用原 requestId 重发"语义：
 * Server 调用此接口查询 Worker 端记录的当前状态，根据状态决定下一步处理：
 * <ul>
 *   <li>RUNNING：Worker 仍在执行，等待 heartbeat 或硬截止</li>
 *   <li>SUCCESS：Worker 已完成，按 §47 围栏回写 NodeAttempt</li>
 *   <li>FAILED：Worker 已失败，按 §47 围栏回写 NodeAttempt</li>
 *   <li>present=false（PROCESS_LOCAL + Pod 重启）：Worker 端记录丢失，
 *       等硬截止到期收敛为 TIMEOUT（§29.4）</li>
 * </ul>
 */
public record RequestStatusResponse(
        boolean present,
        String state,            // RUNNING / SUCCESS / FAILED；present=false 时为 null
        Long leaseVersion,
        String requestChecksum,
        Long attemptId,
        String workerId,
        String workerEpoch,
        String resultJson,
        String resultRef,
        String errorCode,
        String errorMessage) {

    public static RequestStatusResponse notFound() {
        return new RequestStatusResponse(false, null, null, null, null, null, null,
                null, null, null, null);
    }
}
