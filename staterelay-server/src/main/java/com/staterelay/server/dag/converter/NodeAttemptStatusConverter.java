package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import org.springframework.stereotype.Component;

/**
 * {@link NodeAttemptStatus} 的 JPA AttributeConverter。
 */
@Component
public class NodeAttemptStatusConverter extends AbstractCodedEnumConverter<NodeAttemptStatus> {
    public NodeAttemptStatusConverter() {
        super(NodeAttemptStatus.class);
    }
}
