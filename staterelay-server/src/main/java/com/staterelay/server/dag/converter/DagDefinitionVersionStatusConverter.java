package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import jakarta.persistence.Converter;

@Converter
public class DagDefinitionVersionStatusConverter extends AbstractCodedEnumConverter<DagDefinitionVersionStatus> {
    public DagDefinitionVersionStatusConverter() {
        super(DagDefinitionVersionStatus.class);
    }
}
