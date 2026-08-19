package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.ExecutorRegistrationStatus;
import org.springframework.stereotype.Component;

/**
 * {@link ExecutorRegistrationStatus} 的 JPA AttributeConverter。
 */
@Component
public class ExecutorRegistrationStatusConverter extends AbstractCodedEnumConverter<ExecutorRegistrationStatus> {
    public ExecutorRegistrationStatusConverter() {
        super(ExecutorRegistrationStatus.class);
    }
}
