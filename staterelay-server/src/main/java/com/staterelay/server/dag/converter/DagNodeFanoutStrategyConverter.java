package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.DagNodeFanoutStrategy;
import jakarta.persistence.Converter;

@Converter
public class DagNodeFanoutStrategyConverter extends AbstractCodedEnumConverter<DagNodeFanoutStrategy> {
    public DagNodeFanoutStrategyConverter() {
        super(DagNodeFanoutStrategy.class);
    }
}
