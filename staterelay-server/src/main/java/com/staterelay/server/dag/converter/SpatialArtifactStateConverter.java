package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.artifact.SpatialArtifactState;
import org.springframework.stereotype.Component;

/**
 * {@link SpatialArtifactState} 的 JPA AttributeConverter。
 *
 * <p>状态编码：10=CREATING, 20=STAGED, 30=AVAILABLE, 40=ORPHANED, 50=DELETING, 60=DELETED。
 */
@Component
public class SpatialArtifactStateConverter extends AbstractCodedEnumConverter<SpatialArtifactState> {
    public SpatialArtifactStateConverter() {
        super(SpatialArtifactState.class);
    }
}
