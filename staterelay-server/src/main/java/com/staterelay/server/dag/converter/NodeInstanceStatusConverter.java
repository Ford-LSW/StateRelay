package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import org.springframework.stereotype.Component;

/**
 * {@link NodeInstanceStatus} 的 JPA AttributeConverter。
 */
@Component
public class NodeInstanceStatusConverter extends AbstractCodedEnumConverter<NodeInstanceStatus> {
    public NodeInstanceStatusConverter() {
        super(NodeInstanceStatus.class);
    }
}
