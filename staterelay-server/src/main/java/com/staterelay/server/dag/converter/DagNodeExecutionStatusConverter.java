package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagNodeExecutionStatus;
import jakarta.persistence.Converter;

@Converter
public class DagNodeExecutionStatusConverter extends AbstractCodedEnumConverter<DagNodeExecutionStatus> {
    public DagNodeExecutionStatusConverter() {
        super(DagNodeExecutionStatus.class);
    }
}
