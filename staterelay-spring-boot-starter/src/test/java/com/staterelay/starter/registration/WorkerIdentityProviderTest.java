package com.staterelay.starter.registration;

import com.staterelay.starter.StateRelayProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerIdentityProviderTest {

    @Test
    void kubernetesIdentityUsesDownwardApiValuesAndOneProcessEpoch() {
        StateRelayProperties properties = new StateRelayProperties();
        properties.setEnvironment(StateRelayProperties.Environment.KUBERNETES);
        properties.setExecutorPort(9080);
        UUID epoch = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID workerId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        AtomicInteger sequence = new AtomicInteger();
        WorkerIdentityProvider provider = new WorkerIdentityProvider(
                properties,
                Map.of("POD_NAME", "orders-7", "POD_IP", "10.20.30.40"),
                () -> sequence.getAndIncrement() == 0 ? epoch : workerId);

        assertThat(provider.identity()).isSameAs(provider.identity());
        assertThat(provider.identity().workerEpoch()).isEqualTo(epoch);
        assertThat(provider.identity().workerId()).isEqualTo(workerId);
        assertThat(provider.identity().podName()).isEqualTo("orders-7");
        assertThat(provider.identity().host()).isEqualTo("10.20.30.40");
        assertThat(provider.identity().executorPort()).isEqualTo(9080);
        assertThat(sequence).hasValue(2);
    }

    @Test
    void kubernetesStartupFailsWithoutPodIp() {
        StateRelayProperties properties = new StateRelayProperties();
        properties.setEnvironment(StateRelayProperties.Environment.KUBERNETES);

        assertThatThrownBy(() -> new WorkerIdentityProvider(
                properties, Map.of("POD_NAME", "orders-7"), UUID::randomUUID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("POD_IP is required");
    }

    @Test
    void localIdentityUsesConfiguredHostPodNameAndPort() {
        StateRelayProperties properties = new StateRelayProperties();
        properties.setEnvironment(StateRelayProperties.Environment.LOCAL);
        properties.setHost("192.168.10.4");
        properties.setPodName("local-orders");
        properties.setExecutorPort(8181);

        WorkerIdentityProvider.WorkerIdentity identity = new WorkerIdentityProvider(
                properties, Map.of(), UUID::randomUUID).identity();

        assertThat(identity.podName()).isEqualTo("local-orders");
        assertThat(identity.host()).isEqualTo("192.168.10.4");
        assertThat(identity.executorPort()).isEqualTo(8181);
    }
}
