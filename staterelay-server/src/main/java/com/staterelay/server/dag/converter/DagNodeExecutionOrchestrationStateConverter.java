package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagNodeExecutionOrchestrationState;
import jakarta.persistence.Converter;

@Converter
public class DagNodeExecutionOrchestrationStateConverter
        extends AbstractCodedEnumConverter<DagNodeExecutionOrchestrationState> {
    public DagNodeExecutionOrchestrationStateConverter() {
        super(DagNodeExecutionOrchestrationState.class);
    }
}
