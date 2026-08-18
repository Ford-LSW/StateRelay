package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagArtifactType;
import jakarta.persistence.Converter;

@Converter
public class DagArtifactTypeConverter extends AbstractCodedEnumConverter<DagArtifactType> {
    public DagArtifactTypeConverter() {
        super(DagArtifactType.class);
    }
}
