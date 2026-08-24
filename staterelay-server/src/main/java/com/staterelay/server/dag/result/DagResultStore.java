package com.staterelay.server.dag.result;

import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** DAG 进度与终态报告的原子持久化边界。 */
public interface DagResultStore {

    Outcome accept(TerminalUpdate update);

    boolean progress(WorkerExecutionContext context, Instant now, String reportJson);

    enum Outcome {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class TerminalUpdate {
        private WorkerExecutionContext context;
        private int attemptStatus;
        private int nodeStatus;
        private String resultJson;
        private String resultRef;
        private String errorCode;
        private String errorMessage;
        private boolean incrementSuccess;
        private boolean incrementFailure;
        private Instant now;
        private String reportJson;
    }
}
