package com.staterelay.server.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

public final class ExecutorHttpClient {

    private static final String EXECUTE_PATH = "/staterelay/internal/v1/executions";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(3);

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public ExecutorHttpClient(ObjectMapper objectMapper) {
        this(objectMapper, HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
    }

    ExecutorHttpClient(ObjectMapper objectMapper, HttpClient httpClient) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    /** Sends one logical dispatch transmission to the exact target Pod instance. */
    public DispatchAck execute(String workerAddress, ExecuteTaskCommand command)
            throws IOException, InterruptedException {
        String baseAddress = workerAddress.endsWith("/")
                ? workerAddress.substring(0, workerAddress.length() - 1) : workerAddress;
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseAddress + EXECUTE_PATH))
                .timeout(RESPONSE_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(command)))
                .build();
        HttpResponse<byte[]> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("executor returned HTTP " + response.statusCode());
        }
        return objectMapper.readValue(response.body(), DispatchAck.class);
    }
}
