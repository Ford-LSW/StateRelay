package com.staterelay.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.staterelay.contract.dag.algorithm.AlgorithmOutputs;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.dag.spatial.VectorLayerRef;
import com.staterelay.contract.dag.spatial.VectorStorageType;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

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
        assertThat(restored.executionContext()).isNull();
    }

    /**
     * 携带 WorkerExecutionContext 的 ExecuteTaskCommand 序列化往返测试（§12）。
     */
    @Test
    void executeCommandWithExecutionContextRoundTrips() throws Exception {
        WorkerExecutionContext ctx = new WorkerExecutionContext(
                50001L, 60002L, "B", "70002", 1,
                "abc-002", 1L, "gis-worker-01", "epoch-001");
        ctx.setRequestChecksum("sha256:abc");
        ctx.setDispatchGeneration(3L);

        ExecuteTaskCommand source = new ExecuteTaskCommand(
                "ti-2", "ta-2", 1, "dispatch-2", 1,
                "gis-app", "gis-worker-01", "epoch-001", "GDAL_INTERSECTION",
                JsonNodeFactory.instance.objectNode().put("sourceLayer", "layerA"),
                "idem-2", Duration.ofMinutes(30), Instant.parse("2026-08-21T10:00:00Z"),
                ctx);

        String json = objectMapper.writeValueAsString(source);
        ExecuteTaskCommand restored = objectMapper.readValue(json, ExecuteTaskCommand.class);

        assertThat(restored.executionContext()).isNotNull();
        assertThat(restored.executionContext().getDagInstanceId()).isEqualTo(50001L);
        assertThat(restored.executionContext().getNodeCode()).isEqualTo("B");
        assertThat(restored.executionContext().getAttemptNo()).isEqualTo(1);
        assertThat(restored.executionContext().getRequestId()).isEqualTo("abc-002");
        assertThat(restored.executionContext().getRequestChecksum()).isEqualTo("sha256:abc");
        assertThat(restored.executionContext().getDispatchGeneration()).isEqualTo(3L);
    }

    /**
     * VectorLayerRef FileGDB 形态序列化往返测试（§4.3）。
     */
    @Test
    void vectorLayerRefFileGdbRoundTrips() throws Exception {
        VectorLayerRef source = VectorLayerRef.fileGdb(
                20001L, "layerA", "MULTIPOLYGON", 4490);

        String json = objectMapper.writeValueAsString(source);
        VectorLayerRef restored = objectMapper.readValue(json, VectorLayerRef.class);

        assertThat(restored.getArtifactId()).isEqualTo(20001L);
        assertThat(restored.getLayerName()).isEqualTo("layerA");
        assertThat(restored.getStorageType()).isEqualTo(VectorStorageType.FILE_GDB);
        assertThat(restored.hasRequiredFields()).isTrue();
    }

    /**
     * VectorLayerRef PostGIS 形态序列化往返测试（§4.3）。
     */
    @Test
    void vectorLayerRefPostgisRoundTrips() throws Exception {
        VectorLayerRef source = VectorLayerRef.postgis(
                101, "public", "pglayer_a", "geom", "MULTIPOLYGON", 4490);

        String json = objectMapper.writeValueAsString(source);
        VectorLayerRef restored = objectMapper.readValue(json, VectorLayerRef.class);

        assertThat(restored.getDataSourceId()).isEqualTo(101);
        assertThat(restored.getSchema()).isEqualTo("public");
        assertThat(restored.getTable()).isEqualTo("pglayer_a");
        assertThat(restored.getGeometryField()).isEqualTo("geom");
        assertThat(restored.getStorageType()).isEqualTo(VectorStorageType.POSTGIS);
        assertThat(restored.hasRequiredFields()).isTrue();
    }

    /**
     * AlgorithmOutputs 多输出 Map 序列化往返测试（§13.1）。
     */
    @Test
    void algorithmOutputsRoundTrips() throws Exception {
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put("resultLayer", VectorLayerRef.fileGdb(30001L, "result", "MULTIPOLYGON", 4490));
        outputs.put("statistics", Map.of("inputCount", 10000, "resultCount", 1200));
        AlgorithmOutputs source = AlgorithmOutputs.of(outputs);

        String json = objectMapper.writeValueAsString(source);
        AlgorithmOutputs restored = objectMapper.readValue(json, AlgorithmOutputs.class);

        assertThat(restored.getOutputs()).hasSize(2);
        assertThat(restored.get("resultLayer")).isNotNull();
        assertThat(restored.get("statistics")).isInstanceOf(Map.class);
    }

    /**
     * AlgorithmOutputs.single() 便捷构造测试。
     */
    @Test
    void algorithmOutputsSingleHelper() {
        VectorLayerRef ref = VectorLayerRef.fileGdb(30001L, "result", "MULTIPOLYGON", 4490);
        AlgorithmOutputs outputs = AlgorithmOutputs.single("resultLayer", ref);

        assertThat(outputs.getOutputs()).hasSize(1);
        assertThat(outputs.get("resultLayer")).isSameAs(ref);
    }
}
