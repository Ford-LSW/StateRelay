package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagInstanceStatus;
import jakarta.persistence.Converter;

@Converter
public class DagInstanceStatusConverter extends AbstractCodedEnumConverter<DagInstanceStatus> {
    public DagInstanceStatusConverter() {
        super(DagInstanceStatus.class);
    }
}
