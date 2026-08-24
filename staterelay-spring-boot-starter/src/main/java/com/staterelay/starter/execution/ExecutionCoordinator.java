package com.staterelay.starter.execution;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.handler.TaskContext;
import com.staterelay.contract.handler.TaskHandler;
import com.staterelay.contract.handler.TaskResult;
import com.staterelay.contract.handler.spi.AlgorithmExecutor;
import com.staterelay.contract.handler.spi.WorkDirectory;
import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.contract.protocol.ExecutionKind;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.starter.StateRelayProperties;
import com.staterelay.starter.handler.HandlerRegistry;
import com.staterelay.starter.registration.WorkerIdentityProvider;
import com.staterelay.starter.registration.WorkerRegistrationClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Worker 侧执行协调器（对齐文档 §12、§14、§15）。
 *
 * <p>负责接收 {@link ExecuteTaskCommand}，按 {@code handlerName} 查找
 * {@link TaskHandler} 或 {@link AlgorithmExecutor}，在线程池中执行并回报结果。
 *
 * <p><b>AlgorithmExecutor 支持（文档 §12）：</b>
 * 当 handler 是 {@link AlgorithmExecutor} 且 command 携带 {@link WorkerExecutionContext} 时：
 * <ol>
 *   <li>通过 {@link WorkDirectoryManager} 创建 Attempt 独立工作目录（§14.1）</li>
 *   <li>构造 {@link DefaultAlgorithmExecutionContext}，注入 workDir + artifactClient + metadataClient</li>
 *   <li>执行后清理工作目录（§19.4）</li>
 * </ol>
 * 非 DAG 场景（executionContext 为 null）退化为 {@link DefaultTaskContext}。
 */
public final class ExecutionCoordinator implements AutoCloseable {

    private final WorkerIdentityProvider.WorkerIdentity identity;
    private final HandlerRegistry handlers;
    private final ObjectMapper objectMapper;
    private final LocalDispatchStore store;
    private final ResultReporter reporter;
    private final ActivityListener activityListener;
    private final Clock clock;
    private final ThreadPoolTaskExecutor executor;
    private final DispatchDeduplicator deduplicator;
    private final WorkDirectoryManager workDirectoryManager;
    private final com.staterelay.contract.handler.spi.ArtifactClient artifactClient;
    private final com.staterelay.contract.handler.spi.ArtifactMetadataClient artifactMetadataClient;
    private final RequestIdStore requestIdStore;
    private final ConcurrentHashMap<String, ExecuteTaskCommand> activeDispatches =
            new ConcurrentHashMap<>();

    public ExecutionCoordinator(
            StateRelayProperties properties,
            WorkerIdentityProvider identityProvider,
            HandlerRegistry handlers,
            ObjectMapper objectMapper,
            LocalDispatchStore store,
            ResultReporter reporter,
            ActivityListener activityListener) {
        this(properties, identityProvider, handlers, objectMapper, store, reporter,
                activityListener, Clock.systemUTC(), null, null, null);
    }

    public ExecutionCoordinator(
            StateRelayProperties properties,
            WorkerIdentityProvider identityProvider,
            HandlerRegistry handlers,
            ObjectMapper objectMapper,
            LocalDispatchStore store,
            ResultReporter reporter,
            ActivityListener activityListener,
            Clock clock) {
        this(properties, identityProvider, handlers, objectMapper, store, reporter,
                activityListener, clock, null, null, null);
    }

    public ExecutionCoordinator(
            StateRelayProperties properties,
            WorkerIdentityProvider identityProvider,
            HandlerRegistry handlers,
            ObjectMapper objectMapper,
            LocalDispatchStore store,
            ResultReporter reporter,
            ActivityListener activityListener,
            Clock clock,
            WorkDirectoryManager workDirectoryManager,
            com.staterelay.contract.handler.spi.ArtifactClient artifactClient,
            com.staterelay.contract.handler.spi.ArtifactMetadataClient artifactMetadataClient) {
        this(properties, identityProvider, handlers, objectMapper, store, reporter,
                activityListener, clock, workDirectoryManager, artifactClient,
                artifactMetadataClient, new InMemoryRequestIdStore());
    }

    public ExecutionCoordinator(
            StateRelayProperties properties,
            WorkerIdentityProvider identityProvider,
            HandlerRegistry handlers,
            ObjectMapper objectMapper,
            LocalDispatchStore store,
            ResultReporter reporter,
            ActivityListener activityListener,
            Clock clock,
            WorkDirectoryManager workDirectoryManager,
            com.staterelay.contract.handler.spi.ArtifactClient artifactClient,
            com.staterelay.contract.handler.spi.ArtifactMetadataClient artifactMetadataClient,
            RequestIdStore requestIdStore) {
        Objects.requireNonNull(properties, "properties");
        identity = Objects.requireNonNull(identityProvider, "identityProvider").identity();
        this.handlers = Objects.requireNonNull(handlers, "handlers");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.store = Objects.requireNonNull(store, "store");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.activityListener = Objects.requireNonNull(activityListener, "activityListener");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.workDirectoryManager = workDirectoryManager;
        this.artifactClient = artifactClient;
        this.artifactMetadataClient = artifactMetadataClient;
        this.requestIdStore = Objects.requireNonNull(requestIdStore, "requestIdStore");
        int maxConcurrency = requirePositive(properties.getMaxConcurrency(), "maxConcurrency");
        int queueCapacity = requireNonNegative(properties.getQueueCapacity(), "queueCapacity");
        deduplicator = new DispatchDeduplicator(store, maxConcurrency + queueCapacity);
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(maxConcurrency);
        executor.setMaxPoolSize(maxConcurrency);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("staterelay-executor-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.initialize();
    }

    public DispatchAck execute(ExecuteTaskCommand command) {
        Objects.requireNonNull(command, "command");
        if (!matchesIdentity(command)) {
            return ack(command, DispatchAck.AckStatus.REJECTED_STALE_EPOCH,
                    "command target does not match this Worker epoch");
        }
        HandlerRegistry.HandlerBinding binding = handlers.findBinding(command.handlerName())
                .orElse(null);
        if (binding == null) {
            return ack(command, DispatchAck.AckStatus.REJECTED_HANDLER,
                    "unknown handler: " + command.handlerName());
        }

        boolean algorithmRequest = isAlgorithmRequest(command, binding);
        if (algorithmRequest) {
            try {
                String requestId = requestId(command);
                requestChecksum(command);
                requestFence(command);
                if (!(requestIdStore instanceof RequestIdStore.AtomicLifecycle)) {
                    return ack(command, DispatchAck.AckStatus.REJECTED_HANDLER,
                            "RequestIdStore does not provide atomic lifecycle operations");
                }
                if (requestIdStore.find(requestId).isPresent()) {
                    return duplicateAlgorithm(command);
                }
            } catch (IllegalArgumentException exception) {
                return ack(command, DispatchAck.AckStatus.REJECTED_HANDLER,
                        exceptionMessage(exception));
            }
        }

        DispatchDeduplicator.Admission admission = deduplicator.tryAdmit(
                command, identity, clock.instant());
        if (admission.status() == DispatchDeduplicator.AdmissionStatus.DUPLICATE) {
            return ack(command, DispatchAck.AckStatus.DUPLICATE,
                    "dispatch was already admitted durably");
        }
        if (admission.status() == DispatchDeduplicator.AdmissionStatus.REJECTED_CAPACITY) {
            return ack(command, DispatchAck.AckStatus.REJECTED_CAPACITY,
                    "local executor capacity is full");
        }

        if (algorithmRequest) {
            try {
                if (!requestIdStore.tryStart(
                        requestId(command), requestChecksum(command), requestFence(command),
                        clock.instant())) {
                    deduplicator.rollback(admission, command.dispatchId());
                    return duplicateAlgorithm(command);
                }
            } catch (IllegalArgumentException | IllegalStateException exception) {
                deduplicator.rollback(admission, command.dispatchId());
                return ack(command, DispatchAck.AckStatus.REJECTED_HANDLER,
                        exceptionMessage(exception));
            }
        }

        activeDispatches.put(command.dispatchId(), command);
        try {
            executor.execute(() -> runHandler(command, binding, admission));
        } catch (RuntimeException exception) {
            activeDispatches.remove(command.dispatchId(), command);
            if (algorithmRequest) {
                abortAlgorithmStart(command);
            }
            deduplicator.rollback(admission, command.dispatchId());
            publishActivity();
            return ack(command, DispatchAck.AckStatus.REJECTED_CAPACITY,
                    "local executor rejected the dispatch");
        }
        publishActivity();
        return ack(command, DispatchAck.AckStatus.ACCEPTED,
                "dispatch admitted durably");
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private void runHandler(
            ExecuteTaskCommand command,
            HandlerRegistry.HandlerBinding binding,
            DispatchDeduplicator.Admission admission) {
        Instant startedAt = clock.instant();
        WorkDirectory workDir = null;
        boolean isAlgorithm = binding.isAlgorithmExecutor()
                && command.executionContext() != null
                && workDirectoryManager != null;
        boolean running = false;
        boolean rolledBack = false;
        try {
            // DAG 场景为每个 AlgorithmExecutor Attempt 创建独立工作目录（§14.1）。
            if (isAlgorithm) {
                WorkerExecutionContext ctx = command.executionContext();
                workDir = workDirectoryManager.createWorkDirectory(
                        ctx.getDagInstanceId(), ctx.getNodeCode(), ctx.getAttemptId());
            }
            store.markRunning(command.dispatchId(), startedAt);
            running = true;
            publishActivity();
            TaskResultReport report = executeToReport(command, binding, startedAt, workDir);
            if (isAlgorithmRequest(command, binding)) {
                report = persistAlgorithmTerminal(command, report);
            }
            String checksum = LocalDispatchStore.checksum(objectMapper, report);
            store.markTerminal(command.dispatchId(), report, checksum, report.getFinishedAt());
            reporter.completeAttempt(command.attemptId());
            reporter.reportTerminal(command.dispatchId());
        } catch (Throwable throwable) {
            if (!running) {
                if (isAlgorithmRequest(command, binding)) {
                    abortAlgorithmStart(command);
                }
                deduplicator.rollback(admission, command.dispatchId());
                rolledBack = true;
            } else {
                finishUnexpectedFailure(command, binding, startedAt, throwable);
            }
        } finally {
            // 执行后尽力清理工作目录，清理异常不得阻止释放本地容量（§19.4）。
            if (workDir != null) {
                try {
                    workDirectoryManager.cleanupWorkDirectory(
                            workDir.dagInstanceId(), workDir.nodeCode(), workDir.attemptId());
                } catch (RuntimeException ignored) {
                    // Task 4 负责工作目录残留的后续清理策略。
                }
            }
            activeDispatches.remove(command.dispatchId(), command);
            if (!rolledBack) {
                deduplicator.complete(admission);
            }
            publishActivity();
        }
    }

    private void abortAlgorithmStart(ExecuteTaskCommand command) {
        requestIdStore.abortRunningStart(
                requestId(command), requestChecksum(command), requestFence(command));
    }

    private void finishUnexpectedFailure(
            ExecuteTaskCommand command,
            HandlerRegistry.HandlerBinding binding,
            Instant startedAt,
            Throwable throwable) {
        try {
            TaskResultReport report = failure(
                    command, "EXECUTION_COORDINATOR_FAILURE",
                    exceptionMessage(throwable), startedAt);
            if (isAlgorithmRequest(command, binding)) {
                RequestIdStore.RequestIdRecord record = requestIdStore.find(requestId(command))
                        .orElse(null);
                if (record != null && record.state() == RequestIdStore.RequestState.RUNNING) {
                    requestIdStore.markFailed(
                            requestId(command), report.getErrorCode(), report.getErrorMessage(),
                            report.getFinishedAt());
                }
                RequestIdStore.RequestIdRecord terminal = requestIdStore.find(requestId(command))
                        .orElse(null);
                if (terminal != null && terminal.state() != RequestIdStore.RequestState.RUNNING) {
                    report = reportFromRecord(command, terminal);
                }
            }
            LocalDispatchStore.DispatchRecord local = store.find(command.dispatchId()).orElse(null);
            if (local != null && local.state() == LocalDispatchStore.DispatchState.RUNNING) {
                String checksum = LocalDispatchStore.checksum(objectMapper, report);
                store.markTerminal(command.dispatchId(), report, checksum, report.getFinishedAt());
                reporter.completeAttempt(command.attemptId());
                reporter.reportTerminal(command.dispatchId());
            }
        } catch (RuntimeException ignored) {
            // 原始异常后的收敛失败由本地终态恢复与 Server 超时扫描继续处理。
        }
    }

    private TaskResultReport executeToReport(
            ExecuteTaskCommand command,
            HandlerRegistry.HandlerBinding binding,
            Instant startedAt,
            WorkDirectory workDir) {
        Object parameter;
        try {
            JavaType parameterType = objectMapper.getTypeFactory()
                    .constructType(binding.getParameterType());
            parameter = objectMapper.convertValue(command.parameter(), parameterType);
        } catch (IllegalArgumentException exception) {
            return failure(command, "PARAMETER_CONVERSION", exceptionMessage(exception),
                    startedAt);
        }

        try {
            // 根据 handler 类型选择 context（§12）
            TaskContext context;
            if (binding.isAlgorithmExecutor() && command.executionContext() != null
                    && artifactClient != null && artifactMetadataClient != null) {
                context = new DefaultAlgorithmExecutionContext(
                        command, identity, reporter, clock,
                        workDir, artifactClient, artifactMetadataClient, requestIdStore);
            } else {
                context = new DefaultTaskContext(
                        command, identity, reporter, clock);
            }
            @SuppressWarnings("unchecked")
            TaskHandler<Object, Object> handler =
                    (TaskHandler<Object, Object>) binding.getHandler();
            TaskResult<Object> result = handler.execute(context, parameter);
            if (result == null) {
                return failure(command, "INVALID_HANDLER_RESULT",
                        "TaskHandler returned null", startedAt);
            }
            if (!result.success()) {
                return failure(command, result.errorCode(), result.message(), startedAt);
            }
            JsonNode resultValue = result.value() == null
                    ? null : objectMapper.valueToTree(result.value());
            return new TaskResultReport(
                    command.taskInstanceId(), command.attemptId(), command.leaseVersion(),
                    identity.workerId().toString(), identity.workerEpoch().toString(),
                    TaskResultReport.TerminalStatus.SUCCEEDED, resultValue, null, null,
                    startedAt, clock.instant());
        } catch (Throwable throwable) {
            return failure(command, "HANDLER_EXCEPTION", exceptionMessage(throwable),
                    startedAt);
        }
    }

    private TaskResultReport failure(
            ExecuteTaskCommand command,
            String errorCode,
            String errorMessage,
            Instant startedAt) {
        return new TaskResultReport(
                command.taskInstanceId(), command.attemptId(), command.leaseVersion(),
                identity.workerId().toString(), identity.workerEpoch().toString(),
                TaskResultReport.TerminalStatus.FAILED, null, errorCode, errorMessage,
                startedAt, clock.instant());
    }

    private boolean matchesIdentity(ExecuteTaskCommand command) {
        return identity.workerId().toString().equals(command.targetWorkerId())
                && identity.workerEpoch().toString().equals(command.targetWorkerEpoch());
    }

    private DispatchAck ack(
            ExecuteTaskCommand command, DispatchAck.AckStatus status, String message) {
        return new DispatchAck(
                command.dispatchId(), command.attemptId(), identity.workerId().toString(),
                identity.workerEpoch().toString(), status, message);
    }

    private DispatchAck duplicateAlgorithm(ExecuteTaskCommand command) {
        RequestIdStore.RequestIdRecord record;
        try {
            requestIdStore.tryStart(
                    requestId(command), requestChecksum(command), requestFence(command),
                    clock.instant());
            record = requestIdStore.find(requestId(command)).orElseThrow();
            validateDuplicateFence(record, command);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return ack(command, DispatchAck.AckStatus.REJECTED_HANDLER,
                    exceptionMessage(exception));
        }
        if (record.state() == RequestIdStore.RequestState.SUCCESS
                || record.state() == RequestIdStore.RequestState.FAILED) {
            reporter.reportRecoveredTerminal(reportFromRecord(command, record));
        }
        return ack(command, DispatchAck.AckStatus.DUPLICATE,
                "requestId was already admitted by the AlgorithmExecutor fence");
    }

    private TaskResultReport persistAlgorithmTerminal(
            ExecuteTaskCommand command, TaskResultReport executed) {
        String requestId = requestId(command);
        if (executed.getTerminalStatus() == TaskResultReport.TerminalStatus.SUCCEEDED) {
            requestIdStore.markSuccess(
                    requestId,
                    executed.getResult() == null ? "null" : executed.getResult().toString(),
                    null,
                    executed.getFinishedAt());
        } else {
            requestIdStore.markFailed(
                    requestId, executed.getErrorCode(), executed.getErrorMessage(),
                    executed.getFinishedAt());
        }
        return reportFromRecord(command, requestIdStore.find(requestId).orElseThrow());
    }

    private TaskResultReport reportFromRecord(
            ExecuteTaskCommand command, RequestIdStore.RequestIdRecord record) {
        WorkerExecutionContext context = latestContext(command.executionContext(), record);
        JsonNode result = null;
        if (record.state() == RequestIdStore.RequestState.SUCCESS
                && record.resultJson() != null) {
            try {
                result = objectMapper.readTree(record.resultJson());
            } catch (java.io.IOException exception) {
                throw new IllegalStateException("stored terminal result is invalid JSON", exception);
            }
        }
        TaskResultReport.TerminalStatus status = record.state() == RequestIdStore.RequestState.SUCCESS
                ? TaskResultReport.TerminalStatus.SUCCEEDED
                : TaskResultReport.TerminalStatus.FAILED;
        return new TaskResultReport(
                command.taskInstanceId(), command.attemptId(), record.leaseVersion(),
                record.workerId(), record.workerEpoch(), status, result,
                record.errorCode(), record.errorMessage(), record.createdAt(), record.updatedAt(),
                context);
    }

    private static void validateDuplicateFence(
            RequestIdStore.RequestIdRecord record, ExecuteTaskCommand command) {
        WorkerExecutionContext context = command.executionContext();
        Long attemptId = parseAttemptId(context.getAttemptId());
        if (!Objects.equals(record.attemptId(), attemptId)
                || !Objects.equals(record.workerId(), context.getWorkerId())
                || !Objects.equals(record.workerEpoch(), context.getWorkerEpoch())
                || !Objects.equals(record.leaseVersion(), context.getAttemptLeaseVersion())) {
            throw new IllegalStateException("requestId is bound to another execution fence");
        }
    }

    private static boolean isAlgorithmRequest(
            ExecuteTaskCommand command, HandlerRegistry.HandlerBinding binding) {
        return binding.isAlgorithmExecutor() && command.executionContext() != null;
    }

    private static String requestId(ExecuteTaskCommand command) {
        String value = command.executionContext().getRequestId();
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("AlgorithmExecutor requestId must not be blank");
        }
        return value;
    }

    private static String requestChecksum(ExecuteTaskCommand command) {
        String value = command.executionContext().getRequestChecksum();
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("AlgorithmExecutor requestChecksum must not be blank");
        }
        return value;
    }

    private static RequestIdStore.RequestFence requestFence(ExecuteTaskCommand command) {
        WorkerExecutionContext context = command.executionContext();
        return new RequestIdStore.RequestFence(
                parseAttemptId(context.getAttemptId()), context.getWorkerId(),
                context.getWorkerEpoch(), context.getAttemptLeaseVersion());
    }

    private static Long parseAttemptId(String value) {
        try {
            return Long.valueOf(value);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("AlgorithmExecutor attemptId must be a number", exception);
        }
    }

    private static WorkerExecutionContext latestContext(
            WorkerExecutionContext source, RequestIdStore.RequestIdRecord record) {
        WorkerExecutionContext context = new WorkerExecutionContext();
        context.setDagInstanceId(source.getDagInstanceId());
        context.setNodeInstanceId(source.getNodeInstanceId());
        context.setNodeCode(source.getNodeCode());
        context.setAttemptId(source.getAttemptId());
        context.setAttemptNo(source.getAttemptNo());
        context.setRequestId(source.getRequestId());
        context.setRequestChecksum(record.requestChecksum());
        context.setDispatchGeneration(source.getDispatchGeneration());
        context.setDispatchToken(source.getDispatchToken());
        context.setAttemptLeaseVersion(record.leaseVersion());
        context.setWorkerId(record.workerId());
        context.setWorkerEpoch(record.workerEpoch());
        return context;
    }

    private void publishActivity() {
        try {
            List<WorkerRegistrationClient.ExecutionLease> leases = activeDispatches.values()
                    .stream()
                    .map(command -> {
                        boolean dagExecution = command.executionContext() != null;
                        String attemptId = dagExecution
                                ? command.executionContext().getAttemptId()
                                : command.attemptId();
                        RequestIdStore.RequestIdRecord latest = dagExecution
                                ? requestIdStore.find(command.executionContext().getRequestId())
                                        .orElse(null)
                                : null;
                        long leaseVersion = latest == null
                                ? command.leaseVersion() : latest.leaseVersion();
                        UUID workerEpoch = latest == null
                                ? identity.workerEpoch() : UUID.fromString(latest.workerEpoch());
                        return new WorkerRegistrationClient.ExecutionLease(
                                attemptId, leaseVersion, workerEpoch,
                                dagExecution ? ExecutionKind.DAG_NODE : ExecutionKind.GENERIC_TASK);
                    })
                    .toList();
            activityListener.report(
                    executor.getActiveCount(), executor.getThreadPoolExecutor().getQueue().size(),
                    leases);
        } catch (RuntimeException ignored) {
            // 活动上报不得改变本地准入或执行结果。
        }
    }

    private static String exceptionMessage(Throwable throwable) {
        String message = throwable.getMessage();
        return message == null || message.isBlank()
                ? throwable.getClass().getName() : message;
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    @FunctionalInterface
    public interface ActivityListener {

        void report(
                int activeCount,
                int queueDepth,
                List<WorkerRegistrationClient.ExecutionLease> activeLeases);

        static ActivityListener noop() {
            return (activeCount, queueDepth, activeLeases) -> {
            };
        }
    }
}
