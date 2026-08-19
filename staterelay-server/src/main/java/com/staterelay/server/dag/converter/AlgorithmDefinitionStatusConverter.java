package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import org.springframework.stereotype.Component;

/**
 * {@link AlgorithmDefinitionStatus} 的 JPA AttributeConverter。
 */
@Component
public class AlgorithmDefinitionStatusConverter extends AbstractCodedEnumConverter<AlgorithmDefinitionStatus> {
    public AlgorithmDefinitionStatusConverter() {
        super(AlgorithmDefinitionStatus.class);
    }
}
