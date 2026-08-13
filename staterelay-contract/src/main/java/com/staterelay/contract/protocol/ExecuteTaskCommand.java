package com.staterelay.contract.protocol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;

public record ExecuteTaskCommand(
        String taskInstanceId,
        String attemptId,
        int attemptNumber,
        String dispatchId,
        long leaseVersion,
        String application,
        String targetWorkerId,
        String targetWorkerEpoch,
        String handlerName,
        JsonNode parameter,
        String idempotencyKey,
        Duration leaseDuration,
        Instant dispatchedAt) {
}
