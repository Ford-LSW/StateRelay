package com.staterelay.server.dag.worker;

import com.staterelay.contract.protocol.RebindFenceRequest;
import com.staterelay.contract.protocol.RebindFenceResponse;
import com.staterelay.contract.protocol.RequestStatusResponse;
import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;

/**
 * Server 端调用 Worker 围栏协议 HTTP 客户端（对齐文档 §19.1 / §44.2 / §47.1）。
 *
 * <p>由 {@link com.staterelay.server.dag.orchestration.NodeAttemptLeaseRecoverScanner} 调用，
 * 完成 lease 接管恢复协议的跨进程闭环：
 * <ol>
 *   <li>{@link #findRequestStatus}：查询 Worker 端 requestId 当前状态
 *       （§19.1 "使用原 requestId 重发"语义的实现）</li>
 *   <li>{@link #rebindFence}：升级 Worker Store 内部 lease_version
 *       （§44.2 rebindFence 协议）</li>
 * </ol>
 *
 * <p>异常一律 fail-closed：网络错误 / 超时 / 5xx 响应统一抛出 {@link WorkerHttpException}，
 * 由 Scanner 决定是否重试或等硬截止收敛。
 */
public interface WorkerHttpClient {

    /** 向选定 Worker 发送一条具体的 DAG 执行命令。 */
    DispatchAck execute(String workerAddress, ExecuteTaskCommand command);

    /**
     * 查询 Worker 端 requestId 当前状态（§19.1 分支 A "重发原 requestId"语义）。
     *
     * @param workerAddress Worker 地址（host:port，来自统一 {@code sr_worker} 注册表）
     * @param requestId     NodeAttempt.requestId
     * @return Worker 端记录的当前状态；present=false 表示 Worker 端无此记录
     * @throws WorkerHttpException 网络/超时/5xx 等失败场景
     */
    RequestStatusResponse findRequestStatus(String workerAddress, String requestId);

    /**
     * 升级 Worker Store 内部 lease_version（§44.2 rebindFence 协议）。
     *
     * @param workerAddress Worker 地址
     * @param request      rebindFence 请求（含完整围栏：requestId / requestChecksum /
     *                     attemptId / workerId / workerEpoch / newLeaseVersion）
     * @return success=true 表示 Worker Store 已升级到 newLeaseVersion；
     *         success=false 表示 fail-closed 条件不满足
     * @throws WorkerHttpException 网络/超时/5xx 等失败场景
     */
    RebindFenceResponse rebindFence(String workerAddress, RebindFenceRequest request);
}
