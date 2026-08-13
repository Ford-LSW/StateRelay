package com.staterelay.starter.registration;

import com.staterelay.starter.StateRelayProperties;
import com.staterelay.starter.handler.HandlerRegistry;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

public final class WorkerRegistrationClient
        implements ApplicationListener<ApplicationReadyEvent>, AutoCloseable {

    private static final String STARTER_VERSION = "0.1.0";

    private final StateRelayProperties properties;
    private final HandlerRegistry handlers;
    private final WorkerIdentityProvider.WorkerIdentity identity;
    private final RestClient restClient;
    private final TaskScheduler scheduler;
    private final AtomicReference<ActivitySnapshot> activity =
            new AtomicReference<>(new ActivitySnapshot(0, 0, List.of()));
    private volatile ScheduledFuture<?> heartbeatTask;
    private volatile boolean registered;

    public WorkerRegistrationClient(
            StateRelayProperties properties,
            HandlerRegistry handlers,
            WorkerIdentityProvider identityProvider,
            RestClient.Builder restClientBuilder,
            TaskScheduler scheduler) {
        this.properties = properties;
        this.handlers = handlers;
        identity = identityProvider.identity();
        restClient = restClientBuilder.baseUrl(properties.getServerUrl().toString()).build();
        this.scheduler = scheduler;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        validateConfiguration();
        register();
        registered = true;
        heartbeatTask = scheduler.scheduleWithFixedDelay(
                this::heartbeat, properties.getHeartbeatInterval());
    }

    public void reportActivity(
            int activeCount, int queueDepth, List<ExecutionLease> activeLeases) {
        activity.set(new ActivitySnapshot(activeCount, queueDepth, List.copyOf(activeLeases)));
    }

    public void drain() {
        if (!registered) {
            return;
        }
        restClient.post()
                .uri("/internal/v1/workers/{workerId}/drain", identity.workerId())
                .body(new EpochRequest(identity.workerEpoch()))
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public void close() {
        ScheduledFuture<?> current = heartbeatTask;
        if (current != null) {
            current.cancel(false);
        }
        if (registered) {
            restClient.method(org.springframework.http.HttpMethod.DELETE)
                    .uri("/internal/v1/workers/{workerId}", identity.workerId())
                    .body(new EpochRequest(identity.workerEpoch()))
                    .retrieve()
                    .toBodilessEntity();
            registered = false;
        }
    }

    private void register() {
        restClient.post()
                .uri("/internal/v1/workers/register")
                .body(new RegistrationRequest(
                        properties.getAppName(), "default", identity.workerId(),
                        identity.workerEpoch(), identity.podName(), identity.host(),
                        identity.executorPort(), properties.getMaxConcurrency(),
                        properties.getQueueCapacity(), properties.getWorkerLease(),
                        STARTER_VERSION, handlerMetadata()))
                .retrieve()
                .toBodilessEntity();
    }

    private void heartbeat() {
        ActivitySnapshot snapshot = activity.get();
        restClient.post()
                .uri("/internal/v1/workers/{workerId}/heartbeat", identity.workerId())
                .body(new HeartbeatRequest(
                        properties.getAppName(), identity.workerId(), identity.workerEpoch(),
                        "READY", snapshot.activeCount(), snapshot.queueDepth(),
                        properties.getMaxConcurrency(), properties.getQueueCapacity(),
                        properties.getWorkerLease(), STARTER_VERSION, handlerMetadata(),
                        snapshot.activeLeases()))
                .retrieve()
                .toBodilessEntity();
    }

    private Set<HandlerMetadata> handlerMetadata() {
        return handlers.metadata().values().stream()
                .map(metadata -> new HandlerMetadata(
                        metadata.name(), metadata.implementationType()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private void validateConfiguration() {
        if (properties.getServerUrl() == null) {
            throw new IllegalStateException("staterelay.server-url is required");
        }
        if (properties.getAppName() == null || properties.getAppName().isBlank()) {
            throw new IllegalStateException("staterelay.app-name is required");
        }
        if (properties.getHeartbeatInterval().isZero()
                || properties.getHeartbeatInterval().isNegative()) {
            throw new IllegalStateException("staterelay.heartbeat-interval must be positive");
        }
    }

    public record RegistrationRequest(
            String application,
            String environment,
            UUID workerId,
            UUID workerEpoch,
            String podName,
            String podIp,
            int executorPort,
            int maxConcurrency,
            int queueCapacity,
            Duration workerLease,
            String starterVersion,
            Set<HandlerMetadata> handlers) {
    }

    public record HeartbeatRequest(
            String application,
            UUID workerId,
            UUID workerEpoch,
            String status,
            int activeCount,
            int queueDepth,
            int maxConcurrency,
            int queueCapacity,
            Duration workerLease,
            String starterVersion,
            Set<HandlerMetadata> handlers,
            List<ExecutionLease> activeLeases) {
    }

    public record HandlerMetadata(String name, String implementationType) {
    }

    public record ExecutionLease(UUID attemptId, long leaseVersion, UUID workerEpoch) {
    }

    private record EpochRequest(UUID workerEpoch) {
    }

    private record ActivitySnapshot(
            int activeCount, int queueDepth, List<ExecutionLease> activeLeases) {
    }
}
