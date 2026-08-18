package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagInstanceOrchestrationState;
import jakarta.persistence.Converter;

@Converter
public class DagInstanceOrchestrationStateConverter
        extends AbstractCodedEnumConverter<DagInstanceOrchestrationState> {
    public DagInstanceOrchestrationStateConverter() {
        super(DagInstanceOrchestrationState.class);
    }
}
