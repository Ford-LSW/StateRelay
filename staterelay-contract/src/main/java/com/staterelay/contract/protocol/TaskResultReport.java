package com.staterelay.contract.protocol;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record TaskResultReport(
        String taskInstanceId,
        String attemptId,
        long leaseVersion,
        String workerId,
        String workerEpoch,
        TerminalStatus terminalStatus,
        JsonNode result,
        String errorCode,
        String errorMessage,
        Instant startedAt,
        Instant finishedAt) {

    public enum TerminalStatus {
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
