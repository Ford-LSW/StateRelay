package com.staterelay.starter.execution;

import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

public final class ResultReporter implements AutoCloseable {

    private static final Duration MIN_SCHEDULER_INTERVAL = Duration.ofMillis(100);

    private final LocalDispatchStore store;
    private final Transport transport;
    private final Clock clock;
    private final Duration progressInterval;
    private final Duration retryInitial;
    private final Duration retryMaximum;
    private final Duration terminalRetention;
    private final RequestIdStore requestIdStore;
    private final Map<String, ProgressState> progress = new ConcurrentHashMap<>();
    private final Object terminalDeliveryLock = new Object();
    private volatile TaskScheduler scheduler;
    private volatile ScheduledFuture<?> progressTask;
    private volatile ScheduledFuture<?> terminalTask;

    public ResultReporter(
            LocalDispatchStore store,
            Transport transport,
            Clock clock,
            Duration progressInterval,
            Duration retryInitial,
            Duration retryMaximum,
            Duration terminalRetention) {
        this(store, transport, clock, progressInterval, retryInitial, retryMaximum,
                terminalRetention, null);
    }

    public ResultReporter(
            LocalDispatchStore store,
            Transport transport,
            Clock clock,
            Duration progressInterval,
            Duration retryInitial,
            Duration retryMaximum,
            Duration terminalRetention,
            RequestIdStore requestIdStore) {
        this.store = Objects.requireNonNull(store, "store");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.progressInterval = requirePositive(progressInterval, "progressInterval");
        this.retryInitial = requirePositive(retryInitial, "retryInitial");
        this.retryMaximum = requirePositive(retryMaximum, "retryMaximum");
        if (retryMaximum.compareTo(retryInitial) < 0) {
            throw new IllegalArgumentException("retryMaximum must not be less than retryInitial");
        }
        if (terminalRetention == null || terminalRetention.isNegative()) {
            throw new IllegalArgumentException("terminalRetention must not be negative");
        }
        this.terminalRetention = terminalRetention;
        this.requestIdStore = requestIdStore;
    }

    public void start(TaskScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        if (progressTask != null || terminalTask != null) {
            return;
        }
        this.scheduler = scheduler;
        Duration progressTick = progressInterval.compareTo(MIN_SCHEDULER_INTERVAL) < 0
                ? progressInterval : MIN_SCHEDULER_INTERVAL;
        progressTask = scheduler.scheduleWithFixedDelay(
                () -> runSafely(this::flushProgress), progressTick);
        terminalTask = scheduler.scheduleWithFixedDelay(() -> runSafely(() -> {
            retryPendingTerminals();
            pruneAcknowledgedTerminals();
        }), retryInitial);
        try {
            scheduler.schedule(() -> runSafely(this::retryPendingTerminals), clock.instant());
        } catch (RuntimeException ignored) {
            // 即时恢复无法入队时，周期重试任务仍是最终兜底。
        }
    }

    public void reportProgress(TaskProgressReport report) {
        Objects.requireNonNull(report, "report");
        ProgressState state = progress.computeIfAbsent(report.getAttemptId(), ignored ->
                new ProgressState());
        TaskProgressReport outbound = null;
        Instant now = clock.instant();
        synchronized (state) {
            if (state.lastSentAt == null
                    || !now.isBefore(state.lastSentAt.plus(progressInterval))) {
                state.lastSentAt = now;
                state.pending = null;
                outbound = report;
            } else {
                state.pending = report;
            }
        }
        if (outbound != null) {
            sendProgress(state, outbound);
        }
    }

    public void flushProgress() {
        Instant now = clock.instant();
        for (ProgressState state : progress.values()) {
            TaskProgressReport outbound = null;
            synchronized (state) {
                if (state.pending != null
                        && (state.lastSentAt == null
                        || !now.isBefore(state.lastSentAt.plus(progressInterval)))) {
                    outbound = state.pending;
                    state.pending = null;
                    state.lastSentAt = now;
                }
            }
            if (outbound != null) {
                sendProgress(state, outbound);
            }
        }
    }

    public void completeAttempt(String attemptId) {
        progress.remove(attemptId);
    }

    public void reportTerminal(String dispatchId) {
        TaskScheduler currentScheduler = scheduler;
        if (currentScheduler == null) {
            deliverSerialized(dispatchId);
            return;
        }
        try {
            currentScheduler.schedule(
                    () -> runSafely(() -> deliverSerialized(dispatchId)), clock.instant());
        } catch (RuntimeException ignored) {
            // 持久化到期记录仍可由周期重试任务处理。
        }
    }

    /**
     * 重新上报从 RequestIdStore 恢复的终态记录。
     *
     * <p>RequestIdStore 始终是持久化权威来源；发送失败后可在调度器重发同一 requestId 时重试。
     */
    public void reportRecoveredTerminal(TaskResultReport report) {
        Objects.requireNonNull(report, "report");
        TaskScheduler currentScheduler = scheduler;
        if (currentScheduler == null) {
            sendRecovered(report);
            return;
        }
        try {
            currentScheduler.schedule(() -> runSafely(() -> sendRecovered(report)), clock.instant());
        } catch (RuntimeException ignored) {
            // 下次重发时会再次上报 RequestIdStore 中的持久化记录。
        }
    }

    public void retryPendingTerminals() {
        synchronized (terminalDeliveryLock) {
            Instant now = clock.instant();
            store.records().stream()
                    .filter(record -> record.state() == LocalDispatchStore.DispatchState.TERMINAL)
                    .filter(record -> record.acknowledgedAt() == null)
                    .filter(record -> !record.nextTerminalDeliveryAt().isAfter(now))
                    .forEach(this::deliver);
        }
    }

    public int pruneAcknowledgedTerminals() {
        return store.pruneAcknowledged(terminalRetention, clock.instant());
    }

    @Override
    public void close() {
        ScheduledFuture<?> currentProgressTask = progressTask;
        if (currentProgressTask != null) {
            currentProgressTask.cancel(false);
        }
        ScheduledFuture<?> currentTerminalTask = terminalTask;
        if (currentTerminalTask != null) {
            currentTerminalTask.cancel(false);
        }
        synchronized (terminalDeliveryLock) {
            // 在持久化存储关闭前等待已开始的发送完成。
        }
    }

    private void sendProgress(ProgressState state, TaskProgressReport report) {
        try {
            transport.sendProgress(report);
        } catch (Exception exception) {
            synchronized (state) {
                if (state.pending == null) {
                    state.pending = report;
                }
            }
        }
    }

    private void sendRecovered(TaskResultReport report) {
        try {
            transport.sendTerminal(latestTerminalReport(report));
        } catch (Exception ignored) {
            // 终态仍持久化在 RequestIdStore 中。
        }
    }

    private void deliverSerialized(String dispatchId) {
        synchronized (terminalDeliveryLock) {
            deliver(store.require(dispatchId));
        }
    }

    private void deliver(LocalDispatchStore.DispatchRecord record) {
        if (record.state() != LocalDispatchStore.DispatchState.TERMINAL
                || record.acknowledgedAt() != null) {
            return;
        }
        boolean acknowledged = false;
        try {
            acknowledged = transport.sendTerminal(latestTerminalReport(record.terminalReport()));
        } catch (Exception ignored) {
            // 完整报告保持持久化，后续继续重试。
        }
        Instant now = clock.instant();
        if (acknowledged) {
            store.markTerminalAcknowledged(record.dispatchId(), now);
        } else {
            store.recordTerminalDeliveryFailure(
                    record.dispatchId(), now.plus(retryDelay(record.terminalDeliveryAttempts())));
        }
    }

    private TaskResultReport latestTerminalReport(TaskResultReport report) {
        WorkerExecutionContext source = report.getExecutionContext();
        if (requestIdStore == null || source == null) {
            return report;
        }
        RequestIdStore.RequestIdRecord record = requestIdStore.find(source.getRequestId())
                .orElseThrow(() -> new IllegalStateException(
                        "requestId terminal fence is unavailable: " + source.getRequestId()));
        if (!Objects.equals(record.attemptId(), Long.valueOf(source.getAttemptId()))) {
            throw new IllegalStateException("requestId terminal attempt fence mismatch");
        }
        WorkerExecutionContext latest = copyContext(source);
        latest.setRequestChecksum(record.requestChecksum());
        latest.setAttemptLeaseVersion(record.leaseVersion());
        latest.setWorkerId(record.workerId());
        latest.setWorkerEpoch(record.workerEpoch());
        return new TaskResultReport(
                report.getTaskInstanceId(), report.getAttemptId(), record.leaseVersion(),
                record.workerId(), record.workerEpoch(), report.getTerminalStatus(),
                report.getResult(), report.getErrorCode(), report.getErrorMessage(),
                report.getStartedAt(), report.getFinishedAt(), latest);
    }

    private static WorkerExecutionContext copyContext(WorkerExecutionContext source) {
        WorkerExecutionContext copy = new WorkerExecutionContext();
        copy.setDagInstanceId(source.getDagInstanceId());
        copy.setNodeInstanceId(source.getNodeInstanceId());
        copy.setNodeCode(source.getNodeCode());
        copy.setAttemptId(source.getAttemptId());
        copy.setAttemptNo(source.getAttemptNo());
        copy.setRequestId(source.getRequestId());
        copy.setRequestChecksum(source.getRequestChecksum());
        copy.setDispatchGeneration(source.getDispatchGeneration());
        copy.setDispatchToken(source.getDispatchToken());
        copy.setAttemptLeaseVersion(source.getAttemptLeaseVersion());
        copy.setWorkerId(source.getWorkerId());
        copy.setWorkerEpoch(source.getWorkerEpoch());
        return copy;
    }

    private Duration retryDelay(int completedFailures) {
        Duration delay = retryInitial;
        for (int index = 0; index < completedFailures; index++) {
            if (delay.compareTo(retryMaximum.dividedBy(2)) > 0) {
                return retryMaximum;
            }
            delay = delay.multipliedBy(2);
        }
        return delay.compareTo(retryMaximum) > 0 ? retryMaximum : delay;
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static void runSafely(Runnable task) {
        try {
            task.run();
        } catch (RuntimeException ignored) {
            // 后续调度周期会重试持久化终态任务。
        }
    }

    private static final class ProgressState {

        private Instant lastSentAt;
        private TaskProgressReport pending;
    }

    public interface Transport {

        void sendProgress(TaskProgressReport report) throws Exception;

        boolean sendTerminal(TaskResultReport report) throws Exception;
    }

    public static final class RestClientTransport implements Transport {

        private final RestClient restClient;

        public RestClientTransport(RestClient.Builder builder, URI serverUrl) {
            RestClient.Builder transportBuilder = Objects.requireNonNull(builder, "builder").clone();
            if (serverUrl != null) {
                transportBuilder.baseUrl(serverUrl.toString());
            }
            restClient = transportBuilder.build();
        }

        @Override
        public void sendProgress(TaskProgressReport report) {
            restClient.post()
                    .uri("/internal/v1/executions/progress")
                    .body(report)
                    .retrieve()
                    .toBodilessEntity();
        }

        @Override
        public boolean sendTerminal(TaskResultReport report) {
            restClient.post()
                    .uri("/internal/v1/executions/results")
                    .body(report)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        }
    }
}
