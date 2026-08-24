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

        activeDispatches.put(command.dispatchId(), command);
        try {
            executor.execute(() -> runHandler(command, binding, admission));
        } catch (RuntimeException exception) {
            activeDispatches.remove(command.dispatchId(), command);
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
        // DAG 场景：为 AlgorithmExecutor 创建独立工作目录（§14.1）
        WorkDirectory workDir = null;
        boolean isAlgorithm = binding.isAlgorithmExecutor()
                && command.executionContext() != null
                && workDirectoryManager != null;
        if (isAlgorithm) {
            WorkerExecutionContext ctx = command.executionContext();
            workDir = workDirectoryManager.createWorkDirectory(
                    ctx.getDagInstanceId(), ctx.getNodeCode(), ctx.getAttemptId());
        }
        try {
            store.markRunning(command.dispatchId(), startedAt);
            publishActivity();
            TaskResultReport report = executeToReport(command, binding, startedAt, workDir);
            String checksum = LocalDispatchStore.checksum(objectMapper, report);
            store.markTerminal(command.dispatchId(), report, checksum, report.finishedAt());
            reporter.completeAttempt(command.attemptId());
            reporter.reportTerminal(command.dispatchId());
        } finally {
            // 执行后清理工作目录（§19.4，best-effort）
            if (workDir != null) {
                workDirectoryManager.cleanupWorkDirectory(
                        workDir.dagInstanceId(), workDir.nodeCode(), workDir.attemptId());
            }
            activeDispatches.remove(command.dispatchId(), command);
            deduplicator.complete(admission);
            publishActivity();
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
                        workDir, artifactClient, artifactMetadataClient);
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

    private void publishActivity() {
        try {
            List<WorkerRegistrationClient.ExecutionLease> leases = activeDispatches.values()
                    .stream()
                    .map(command -> {
                        boolean dagExecution = command.executionContext() != null;
                        String attemptId = dagExecution
                                ? command.executionContext().getAttemptId()
                                : command.attemptId();
                        return new WorkerRegistrationClient.ExecutionLease(
                                attemptId, command.leaseVersion(), identity.workerEpoch(),
                                dagExecution ? ExecutionKind.DAG_NODE : ExecutionKind.GENERIC_TASK);
                    })
                    .toList();
            activityListener.report(
                    executor.getActiveCount(), executor.getThreadPoolExecutor().getQueue().size(),
                    leases);
        } catch (RuntimeException ignored) {
            // Activity reporting must not change local admission or execution outcomes.
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
