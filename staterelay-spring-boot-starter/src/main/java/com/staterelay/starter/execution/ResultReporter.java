package com.staterelay.starter.execution;

import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;
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
        runSafely(this::retryPendingTerminals);
    }

    public void reportProgress(TaskProgressReport report) {
        Objects.requireNonNull(report, "report");
        ProgressState state = progress.computeIfAbsent(report.attemptId(), ignored ->
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
            // The durable due record remains available to the periodic retry task.
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
            acknowledged = transport.sendTerminal(record.terminalReport());
        } catch (Exception ignored) {
            // The complete report remains durable and will be retried.
        }
        Instant now = clock.instant();
        if (acknowledged) {
            store.markTerminalAcknowledged(record.dispatchId(), now);
        } else {
            store.recordTerminalDeliveryFailure(
                    record.dispatchId(), now.plus(retryDelay(record.terminalDeliveryAttempts())));
        }
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
            // A later scheduler tick retries durable terminal work.
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
