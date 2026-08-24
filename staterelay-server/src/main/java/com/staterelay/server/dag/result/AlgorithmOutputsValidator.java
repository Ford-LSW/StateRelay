package com.staterelay.server.dag.result;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.algorithm.AlgorithmDataType;
import com.staterelay.contract.dag.algorithm.AlgorithmOutputDef;
import com.staterelay.contract.dag.algorithm.OutputCardinality;
import com.staterelay.contract.dag.spatial.VectorDatasetRef;
import com.staterelay.contract.dag.spatial.VectorDataType;
import com.staterelay.contract.dag.spatial.VectorLayerRef;
import com.staterelay.contract.dag.spatial.VectorStorageType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/** 按持久化算法输出契约校验 Worker 上报的 AlgorithmOutputs。 */
@Component
public final class AlgorithmOutputsValidator {

    private final ObjectMapper objectMapper;

    public AlgorithmOutputsValidator(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    public Validation validate(AlgorithmContract contract, JsonNode result) {
        if (contract == null || result == null || !result.isObject()
                || !result.path("outputs").isObject()) {
            return Validation.invalid("result must be an AlgorithmOutputs object");
        }
        JsonNode outputs = result.path("outputs");
        Map<String, AlgorithmOutputDef> definitions = contract.getOutputs();
        if (definitions == null) {
            return Validation.invalid("algorithm output contract is missing");
        }
        Iterator<String> names = outputs.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            if (!definitions.containsKey(name)) {
                return Validation.invalid("undeclared algorithm output: " + name);
            }
        }
        for (Map.Entry<String, AlgorithmOutputDef> entry : definitions.entrySet()) {
            JsonNode value = outputs.get(entry.getKey());
            AlgorithmOutputDef definition = entry.getValue();
            if ((value == null || value.isNull()) && definition.isRequired()) {
                return Validation.invalid("required algorithm output is missing: " + entry.getKey());
            }
            if (value != null && !value.isNull() && !matches(definition, value)) {
                return Validation.invalid("algorithm output type/cardinality mismatch: " + entry.getKey());
            }
        }
        String primary = contract.getPrimaryOutputKey();
        AlgorithmOutputDef primaryDefinition = primary == null ? null : definitions.get(primary);
        String resultRef = isArtifactReference(primaryDefinition)
                ? resultRef(outputs.get(primary)) : null;
        try {
            return new Validation(true, objectMapper.writeValueAsString(result), resultRef, null);
        } catch (JsonProcessingException exception) {
            return Validation.invalid("algorithm outputs cannot be serialized");
        }
    }

    private boolean matches(AlgorithmOutputDef definition, JsonNode value) {
        OutputCardinality cardinality = definition.getCardinality();
        if (cardinality == OutputCardinality.LIST) {
            if (!value.isArray()) {
                return false;
            }
            for (JsonNode element : value) {
                if (!matchesValue(definition.getDataType(), element)) {
                    return false;
                }
            }
            return true;
        }
        if (cardinality == OutputCardinality.MAP) {
            if (!value.isObject()) {
                return false;
            }
            Iterator<JsonNode> values = value.elements();
            while (values.hasNext()) {
                if (!matchesValue(definition.getDataType(), values.next())) {
                    return false;
                }
            }
            return true;
        }
        return matchesValue(definition.getDataType(), value);
    }

    private boolean matchesValue(String dataType, JsonNode value) {
        return switch (dataType) {
            case "STRING" -> value.isTextual();
            case "INTEGER" -> value.isIntegralNumber();
            case "NUMBER", "DECIMAL" -> value.isNumber();
            case "BOOLEAN" -> value.isBoolean();
            case "MAP", "OBJECT" -> value.isObject();
            case "LIST", "ARRAY" -> value.isArray();
            case "VECTOR_LAYER_REF" -> validLayer(value);
            case "VECTOR_DATASET_REF" -> validDataset(value);
            case "JSON" -> true;
            default -> AlgorithmDataType.isSupportedOptionsType(dataType) && value.isObject();
        };
    }

    private boolean validLayer(JsonNode value) {
        if (!explicitEnum(value, "dataType", VectorDataType.VECTOR_LAYER.name())
                || (!explicitEnum(value, "storageType", VectorStorageType.FILE_GDB.name())
                && !explicitEnum(value, "storageType", VectorStorageType.POSTGIS.name()))) {
            return false;
        }
        try {
            VectorLayerRef ref = objectMapper.treeToValue(value, VectorLayerRef.class);
            return ref.getDataType() == VectorDataType.VECTOR_LAYER
                    && (ref.getStorageType() == VectorStorageType.FILE_GDB
                    || ref.getStorageType() == VectorStorageType.POSTGIS)
                    && ref.hasRequiredFields();
        } catch (JsonProcessingException exception) {
            return false;
        }
    }

    private boolean validDataset(JsonNode value) {
        if (!explicitEnum(value, "dataType", VectorDataType.VECTOR_DATASET.name())
                || !explicitEnum(
                value, "storageType", VectorStorageType.OBJECT_STORAGE.name())) {
            return false;
        }
        try {
            VectorDatasetRef ref = objectMapper.treeToValue(value, VectorDatasetRef.class);
            return ref.getDataType() == VectorDataType.VECTOR_DATASET
                    && ref.getStorageType() == VectorStorageType.OBJECT_STORAGE
                    && ref.getArtifactId() != null
                    && ref.getFormat() != null && !ref.getFormat().isBlank();
        } catch (JsonProcessingException exception) {
            return false;
        }
    }

    private static boolean explicitEnum(JsonNode value, String field, String expected) {
        return value.isObject() && value.path(field).isTextual()
                && expected.equals(value.path(field).textValue());
    }

    private String resultRef(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isObject() && value.hasNonNull("artifactId")) {
            return value.path("artifactId").asText();
        }
        return null;
    }

    private static boolean isArtifactReference(AlgorithmOutputDef definition) {
        if (definition == null || definition.getCardinality() != OutputCardinality.SINGLE) {
            return false;
        }
        return AlgorithmDataType.VECTOR_LAYER_REF.equals(definition.getDataType())
                || AlgorithmDataType.VECTOR_DATASET_REF.equals(definition.getDataType());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Validation {
        private boolean valid;
        private String resultJson;
        private String resultRef;
        private String error;

        static Validation invalid(String error) {
            return new Validation(false, null, null, error);
        }
    }
}
