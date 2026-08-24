package com.staterelay.starter.execution;

import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.handler.spi.AlgorithmExecutionContext;
import com.staterelay.contract.handler.spi.ArtifactClient;
import com.staterelay.contract.handler.spi.ArtifactMetadataClient;
import com.staterelay.contract.handler.spi.WorkDirectory;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.starter.registration.WorkerIdentityProvider;

import java.time.Clock;
import java.util.Objects;

/**
 * Worker 侧 {@link AlgorithmExecutionContext} 默认实现（对齐文档 §12、§14、§15）。
 *
 * <p>由 {@link ExecutionCoordinator} 在调用 {@link com.staterelay.contract.handler.spi.AlgorithmExecutor#execute}
 * 时构造，注入以下能力：
 * <ul>
 *   <li>{@link #executionContext()} —— 调度和围栏信息（dagInstanceId / nodeCode / attemptId / leaseVersion 等）</li>
 *   <li>{@link #workDirectory()} —— Attempt 独立工作目录（input / temp / output，§14.1）</li>
 *   <li>{@link #artifactClient()} —— Artifact 下载 / 上传（§15.1）</li>
 *   <li>{@link #artifactMetadataClient()} —— Artifact 元数据查询与 STAGED 登记（§15.1）</li>
 * </ul>
 *
 * <p>同时实现 {@link com.staterelay.contract.handler.TaskContext} 的基础方法
 * （taskInstanceId / attemptId / leaseVersion / idempotencyKey / reportProgress），
 * 复用 {@link ResultReporter} 上报进度。
 */
public final class DefaultAlgorithmExecutionContext implements AlgorithmExecutionContext {

    private final ExecuteTaskCommand command;
    private final WorkerIdentityProvider.WorkerIdentity identity;
    private final ResultReporter reporter;
    private final Clock clock;
    private final WorkerExecutionContext executionContext;
    private final WorkDirectory workDirectory;
    private final ArtifactClient artifactClient;
    private final ArtifactMetadataClient artifactMetadataClient;

    public DefaultAlgorithmExecutionContext(
            ExecuteTaskCommand command,
            WorkerIdentityProvider.WorkerIdentity identity,
            ResultReporter reporter,
            Clock clock,
            WorkDirectory workDirectory,
            ArtifactClient artifactClient,
            ArtifactMetadataClient artifactMetadataClient) {
        this.command = Objects.requireNonNull(command, "command");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.executionContext = command.executionContext();
        this.workDirectory = workDirectory;
        this.artifactClient = artifactClient;
        this.artifactMetadataClient = artifactMetadataClient;
    }

    @Override
    public WorkerExecutionContext executionContext() {
        return executionContext;
    }

    @Override
    public WorkDirectory workDirectory() {
        return workDirectory;
    }

    @Override
    public ArtifactClient artifactClient() {
        return artifactClient;
    }

    @Override
    public ArtifactMetadataClient artifactMetadataClient() {
        return artifactMetadataClient;
    }

    // ===== TaskContext 基础方法 =====

    @Override
    public String taskInstanceId() {
        return command.taskInstanceId();
    }

    @Override
    public String attemptId() {
        return command.attemptId();
    }

    @Override
    public long leaseVersion() {
        return command.leaseVersion();
    }

    @Override
    public String idempotencyKey() {
        return command.idempotencyKey();
    }

    @Override
    public boolean isCancellationRequested() {
        return false;
    }

    @Override
    public void reportProgress(int percent, String message) {
        reporter.reportProgress(new TaskProgressReport(
                command.taskInstanceId(), command.attemptId(), command.leaseVersion(),
                identity.workerId().toString(), identity.workerEpoch().toString(),
                percent, message, clock.instant()));
    }
}
