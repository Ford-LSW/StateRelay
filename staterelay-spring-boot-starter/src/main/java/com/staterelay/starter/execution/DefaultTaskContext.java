package com.staterelay.starter.execution;

import com.staterelay.contract.handler.TaskContext;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.starter.registration.WorkerIdentityProvider;

import java.time.Clock;
import java.util.Objects;

public final class DefaultTaskContext implements TaskContext {

    private final ExecuteTaskCommand command;
    private final WorkerIdentityProvider.WorkerIdentity identity;
    private final ResultReporter reporter;
    private final Clock clock;

    public DefaultTaskContext(
            ExecuteTaskCommand command,
            WorkerIdentityProvider.WorkerIdentity identity,
            ResultReporter reporter,
            Clock clock) {
        this.command = Objects.requireNonNull(command, "command");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

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
