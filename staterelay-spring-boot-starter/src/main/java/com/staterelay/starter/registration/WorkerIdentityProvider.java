package com.staterelay.starter.registration;

import com.staterelay.starter.StateRelayProperties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public final class WorkerIdentityProvider {

    private final WorkerIdentity identity;

    public WorkerIdentityProvider(StateRelayProperties properties) {
        this(properties, System.getenv(), UUID::randomUUID);
    }

    WorkerIdentityProvider(
            StateRelayProperties properties,
            Map<String, String> environment,
            Supplier<UUID> uuidFactory) {
        Objects.requireNonNull(properties, "properties");
        UUID workerEpoch = uuidFactory.get();
        UUID workerId = uuidFactory.get();
        String podName = trimToNull(environment.get("POD_NAME"));
        String podIp = trimToNull(environment.get("POD_IP"));
        if (properties.getEnvironment() == StateRelayProperties.Environment.KUBERNETES) {
            if (podIp == null) {
                throw new IllegalStateException(
                        "POD_IP is required when staterelay.environment=KUBERNETES");
            }
            if (podName == null) {
                throw new IllegalStateException(
                        "POD_NAME is required when staterelay.environment=KUBERNETES");
            }
        } else {
            podName = firstNonBlank(properties.getPodName(), podName, localHostName());
            podIp = firstNonBlank(properties.getHost(), podIp, "127.0.0.1");
        }
        identity = new WorkerIdentity(
                workerId, workerEpoch, podName, podIp, properties.getExecutorPort());
    }

    public WorkerIdentity identity() {
        return identity;
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException exception) {
            return "localhost";
        }
    }

    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            String value = trimToNull(candidate);
            if (value != null) {
                return value;
            }
        }
        throw new IllegalStateException("worker identity value must not be blank");
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    public record WorkerIdentity(
            UUID workerId, UUID workerEpoch, String podName, String host, int executorPort) {
    }
}
