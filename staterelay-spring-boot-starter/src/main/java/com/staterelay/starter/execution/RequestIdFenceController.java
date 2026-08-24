package com.staterelay.starter.execution;

import com.staterelay.contract.protocol.RebindFenceRequest;
import com.staterelay.contract.protocol.RebindFenceResponse;
import com.staterelay.contract.protocol.RequestStatusResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Worker 端 requestId 围栏协议 HTTP 端点（对齐文档 §19.1 / §44.2 / §47.1）。
 *
 * <p>提供 lease 接管恢复时 Server 调用 Worker Store 的两个端点：
 * <ol>
 *   <li>{@code POST /staterelay/internal/v1/executions/{requestId}/rebind-fence}：
 *       升级 Worker Store 内部 lease_version（rebindFence 协议）</li>
 *   <li>{@code GET /staterelay/internal/v1/executions/{requestId}/status}：
 *       查询 requestId 当前状态（"使用原 requestId 重发"语义的实现）</li>
 * </ol>
 *
 * <p>本 Controller 独立于 {@link ExecutorController}，仅依赖 {@link RequestIdStore} SPI，
 * 不引入 {@link ExecutionCoordinator} 强耦合（避免 Worker 未启用执行模块时端点不可用）。
 *
 * <p>围栏校验全部 fail-closed：requestId 不存在、checksum 不一致、attemptId/workerId 不匹配、
 * 状态不合法等场景返回 success=false 或 notFound 响应，由 Server 端 Scanner 决定后续路径。
 */
@RestController
@RequestMapping("/staterelay/internal/v1/executions/{requestId}")
public final class RequestIdFenceController {

    private static final Logger log = Logger.getLogger(RequestIdFenceController.class.getName());

    private final RequestIdStore requestIdStore;

    public RequestIdFenceController(RequestIdStore requestIdStore) {
        this.requestIdStore = Objects.requireNonNull(requestIdStore, "requestIdStore");
    }

    /**
     * rebindFence 端点（§19.1 / §44.2）。
     *
     * <p>Server 端 lease 接管恢复时调用：先在 DB CAS 升级 attempt_lease_version，
     * 再调用本端点同步升级 Worker Store 内部 lease_version，
     * 然后使用新 lease_version 重发原 requestId。
     *
     * <p>校验规则全部在 {@link RequestIdStore#rebindFence} 内 fail-closed 实现：
     * 单调递增 + 围栏匹配 + 状态合法。
     */
    @PostMapping(value = "/rebind-fence", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public RebindFenceResponse rebindFence(@PathVariable String requestId,
                                           @RequestBody RebindFenceRequest request) {
        if (!Objects.equals(requestId, request.requestId())) {
            return RebindFenceResponse.failed("path requestId does not match body requestId");
        }
        Instant now = Instant.now();
        RequestIdStore.RequestFence fence = new RequestIdStore.RequestFence(
                request.attemptId(),
                request.workerId(),
                request.workerEpoch(),
                request.newLeaseVersion());

        try {
            boolean upgraded = requestIdStore.rebindFence(
                    requestId, request.requestChecksum(), fence, now);
            if (!upgraded) {
                return RebindFenceResponse.failed(
                        "rebindFence rejected: version not monotonic, fence mismatch, or state not in {RUNNING,SUCCESS,FAILED}");
            }
            // 升级成功：回查当前状态用于响应
            return requestIdStore.find(requestId)
                    .map(record -> new RebindFenceResponse(
                            true,
                            record.state().name(),
                            record.leaseVersion(),
                            "rebindFence upgraded to leaseVersion=" + request.newLeaseVersion()))
                    .orElseGet(() -> new RebindFenceResponse(
                            true,
                            null,
                            request.newLeaseVersion(),
                            "rebindFence upgraded but record vanished (race with cleanup)"));
        } catch (IllegalArgumentException ex) {
            // 围栏不匹配（attemptId/workerId 不一致）
            log.warning("rebindFence rejected for requestId=" + requestId + ": " + ex.getMessage());
            return RebindFenceResponse.failed("fence mismatch: " + ex.getMessage());
        }
    }

    /**
     * requestId 状态查询端点（§19.1 "使用原 requestId 重发"语义）。
     *
     * <p>Server 端 lease 接管恢复时，通过本端点查询 Worker 端记录的当前状态：
     * <ul>
     *   <li>RUNNING：Worker 仍在执行，等待 heartbeat 或硬截止</li>
     *   <li>SUCCESS：Worker 已完成，按 §47 围栏回写 NodeAttempt</li>
     *   <li>FAILED：Worker 已失败，按 §47 围栏回写 NodeAttempt</li>
     *   <li>present=false（PROCESS_LOCAL + Pod 重启）：记录丢失，等硬截止收敛为 TIMEOUT</li>
     * </ul>
     */
    @GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatusResponse status(@PathVariable String requestId) {
        return requestIdStore.find(requestId)
                .map(record -> new RequestStatusResponse(
                        true,
                        record.state().name(),
                        record.leaseVersion(),
                        record.requestChecksum(),
                        record.attemptId(),
                        record.workerId(),
                        record.workerEpoch(),
                        record.resultJson(),
                        record.resultRef(),
                        record.errorCode(),
                        record.errorMessage()))
                .orElseGet(RequestStatusResponse::notFound);
    }

    /**
     * 围栏校验异常统一处理：转成 fail-closed 响应，不向 Server 暴露内部堆栈。
     */
    @ExceptionHandler(IllegalStateException.class)
    @ResponseBody
    public RebindFenceResponse handleIllegalState(IllegalStateException ex) {
        log.warning("RequestIdFenceController illegal state: " + ex.getMessage());
        return RebindFenceResponse.failed("illegal state: " + ex.getMessage());
    }
}
