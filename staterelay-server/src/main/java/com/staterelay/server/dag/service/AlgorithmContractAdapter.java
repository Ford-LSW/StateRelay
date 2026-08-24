package com.staterelay.server.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.algorithm.AlgorithmDataType;
import com.staterelay.contract.dag.algorithm.AlgorithmInputDef;
import com.staterelay.contract.dag.algorithm.AlgorithmOutputDef;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 将持久化的算法定义 schema 转换为发布和运行时使用的结构化契约。
 */
@Component
public class AlgorithmContractAdapter {

    private static final Set<String> SUPPORTED_DATA_TYPES = Set.of(
        "STRING", "INTEGER", "NUMBER", "DECIMAL", "BOOLEAN",
        "MAP", "OBJECT", "LIST", "ARRAY", "JSON",
        "VECTOR_LAYER_REF", "VECTOR_DATASET_REF");

    private final ObjectMapper objectMapper;

    public AlgorithmContractAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 将一个算法定义转换为其输入、输出契约。
     *
     * @param definition 持久化算法定义
     * @return 可用于校验的结构化契约
     */
    public AlgorithmContract toContract(AlgorithmDefinitionEntity definition) {
        if (definition == null || definition.getAlgorithmCode() == null || definition.getAlgorithmCode().isBlank()) {
            throw new IllegalArgumentException("Algorithm definition or algorithmCode is blank");
        }
        AlgorithmContract contract = new AlgorithmContract(
            definition.getAlgorithmCode(), definition.getAlgorithmName(), definition.getContractVersion());
        contract.setContractChecksum(definition.getContractChecksum());
        Map<String, AlgorithmInputDef> inputs = readSchema(
            definition.getInputSchemaJson(), new TypeReference<>() { }, "input");
        Map<String, AlgorithmOutputDef> outputs = readSchema(
            definition.getOutputSchemaJson(), new TypeReference<>() { }, "output");
        validateInputPorts(inputs);
        validateOutputPorts(outputs);
        contract.setInputs(inputs);
        contract.setOutputs(outputs);
        return contract;
    }

    private void validateInputPorts(Map<String, AlgorithmInputDef> inputs) {
        for (Map.Entry<String, AlgorithmInputDef> entry : inputs.entrySet()) {
            if (entry.getValue().getDataType() == null || entry.getValue().getDataType().isBlank()) {
                throw new IllegalArgumentException(
                    "Algorithm input schema port " + entry.getKey() + " has blank dataType");
            }
            validateSupportedDataType("input", entry.getKey(), entry.getValue().getDataType());
        }
    }

    private void validateOutputPorts(Map<String, AlgorithmOutputDef> outputs) {
        for (Map.Entry<String, AlgorithmOutputDef> entry : outputs.entrySet()) {
            AlgorithmOutputDef output = entry.getValue();
            if (output.getDataType() == null || output.getDataType().isBlank()) {
                throw new IllegalArgumentException(
                    "Algorithm output schema port " + entry.getKey() + " has blank dataType");
            }
            validateSupportedDataType("output", entry.getKey(), output.getDataType());
            if (output.getCardinality() == null) {
                throw new IllegalArgumentException(
                    "Algorithm output schema port " + entry.getKey() + " has null cardinality");
            }
        }
    }

    private void validateSupportedDataType(String schemaName, String portName, String dataType) {
        if (!SUPPORTED_DATA_TYPES.contains(dataType)
                && !AlgorithmDataType.isSupportedOptionsType(dataType)) {
            throw new IllegalArgumentException(
                "Algorithm " + schemaName + " schema port " + portName
                    + " has unsupported dataType: " + dataType);
        }
    }

    /**
     * 反序列化一份端口 schema；空 schema 表示没有声明端口，非对象 JSON 一律拒绝。
     *
     * @param schemaJson schema JSON
     * @param type 目标 map 类型
     * @param schemaName schema 名称，用于错误信息
     * @param <T> 端口定义类型
     * @return 有序端口定义
     */
    private <T> Map<String, T> readSchema(String schemaJson, TypeReference<Map<String, T>> type, String schemaName) {
        if (schemaJson == null || schemaJson.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            JsonNode root = objectMapper.readTree(schemaJson);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("Algorithm " + schemaName + " schema must be a JSON object");
            }
            Map<String, T> schema = objectMapper.readValue(schemaJson, type);
            if (schema == null) {
                return new LinkedHashMap<>();
            }
            for (Map.Entry<String, T> entry : schema.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null) {
                    throw new IllegalArgumentException("Algorithm " + schemaName + " schema has blank port or null definition");
                }
            }
            return new LinkedHashMap<>(schema);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid algorithm " + schemaName + " schema JSON", ex);
        }
    }
}
