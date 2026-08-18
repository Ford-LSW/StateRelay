package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagEdgeType;
import jakarta.persistence.Converter;

@Converter
public class DagEdgeTypeConverter extends AbstractCodedEnumConverter<DagEdgeType> {
    public DagEdgeTypeConverter() {
        super(DagEdgeType.class);
    }
}
