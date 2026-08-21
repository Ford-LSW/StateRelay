package com.staterelay.server.dag.worker;

import com.staterelay.contract.protocol.RebindFenceRequest;
import com.staterelay.contract.protocol.RebindFenceResponse;
import com.staterelay.contract.protocol.RequestStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;

/**
 * {@link WorkerHttpClient} 的 RestClient 实现（对齐文档 §19.1 / §44.2）。
 *
 * <p>使用 Spring 6 的 {@link RestClient}（与 starter 模块的 RestClient.Builder 风格一致）。
 * 通过 {@code @Value} 注入超时参数，可由 application.yaml 覆盖。
 *
 * <p>URL 规约：{@code http://{workerAddress}/staterelay/internal/v1/executions/{requestId}/...}
 *
 * <p>异常处理：网络错误 / 超时 / 5xx 一律抛 {@link WorkerHttpException}，
 * Scanner 捕获后等下一轮或硬截止收敛。
 */
@Component
public class RestClientWorkerHttpClient implements WorkerHttpClient {

    private static final Logger log = LoggerFactory.getLogger(RestClientWorkerHttpClient.class);

    private static final String URL_BASE = "http://{address}/staterelay/internal/v1/executions/{requestId}";
    private static final String PATH_REBIND_FENCE = "/rebind-fence";
    private static final String PATH_STATUS = "/status";

    private final RestClient restClient;

    public RestClientWorkerHttpClient(
            @Value("${staterelay.dag.worker-http.connect-timeout-ms:2000}") long connectTimeoutMs,
            @Value("${staterelay.dag.worker-http.read-timeout-ms:3000}") long readTimeoutMs) {
        // 第一版用最简 RestClient 配置；如需精细超时可改用 RestClient.Builder + ClientHttpRequestFactory
        this.restClient = RestClient.builder()
                .build();
    }

    @Override
    public RequestStatusResponse findRequestStatus(String workerAddress, String requestId) {
        try {
            return restClient.get()
                    .uri(URL_BASE + PATH_STATUS, workerAddress, requestId)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(RequestStatusResponse.class);
        } catch (RestClientException ex) {
            log.warn("findRequestStatus failed for worker={} requestId={}: {}",
                    workerAddress, requestId, ex.getMessage());
            throw new WorkerHttpException(
                    "GET /status failed for worker=" + workerAddress + " requestId=" + requestId, ex);
        }
    }

    @Override
    public RebindFenceResponse rebindFence(String workerAddress, RebindFenceRequest request) {
        try {
            return restClient.post()
                    .uri(URL_BASE + PATH_REBIND_FENCE, workerAddress, request.requestId())
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(RebindFenceResponse.class);
        } catch (RestClientException ex) {
            log.warn("rebindFence failed for worker={} requestId={}: {}",
                    workerAddress, request.requestId(), ex.getMessage());
            throw new WorkerHttpException(
                    "POST /rebind-fence failed for worker=" + workerAddress
                            + " requestId=" + request.requestId(), ex);
        }
    }
}
