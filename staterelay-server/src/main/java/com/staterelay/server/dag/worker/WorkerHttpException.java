package com.staterelay.server.dag.worker;

/**
 * Worker HTTP 调用异常（对齐文档 §19.1 / §47.1 fail-closed 语义）。
 *
 * <p>所有网络错误 / 超时 / 5xx 响应统一抛出本异常；
 * Scanner 捕获后等下一轮重试或硬截止到期收敛为 TIMEOUT（§29.4）。
 */
public class WorkerHttpException extends RuntimeException {

    public WorkerHttpException(String message) {
        super(message);
    }

    public WorkerHttpException(String message, Throwable cause) {
        super(message, cause);
    }
}
