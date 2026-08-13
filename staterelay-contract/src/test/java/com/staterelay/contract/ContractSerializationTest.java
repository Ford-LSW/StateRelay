package com.staterelay.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ContractSerializationTest {

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();

    @Test
    void executeCommandRoundTripsWithoutLosingFenceFields() throws Exception {
        ExecuteTaskCommand source = new ExecuteTaskCommand(
                "ti-1", "ta-1", 1, "dispatch-1", 7,
                "order-service", "order-service-pod-a", "epoch-a", "closeExpiredOrders",
                JsonNodeFactory.instance.objectNode().put("tenantId", 42),
                "idem-1", Duration.ofMinutes(10), Instant.parse("2026-08-13T10:00:00Z"));

        String json = objectMapper.writeValueAsString(source);
        ExecuteTaskCommand restored = objectMapper.readValue(json, ExecuteTaskCommand.class);

        assertThat(restored).isEqualTo(source);
        assertThat(restored.leaseVersion()).isEqualTo(7);
        assertThat(restored.dispatchId()).isEqualTo("dispatch-1");
        assertThat(restored.targetWorkerEpoch()).isEqualTo("epoch-a");
    }
}
