package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG Artifact 类型。
 *
 * <p>与 {@code sr_dag_artifact.artifact_type} 列对应（存数值）。
 */
@Getter
@AllArgsConstructor
public enum DagArtifactType implements CodedEnum {
    INLINE(0),
    FILE(1),
    DATABASE(2),
    OBJECT_STORAGE(3);

    private final int code;
}
