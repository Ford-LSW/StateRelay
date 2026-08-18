package com.staterelay.contract.dag;

import com.staterelay.contract.dag.enums.DagArtifactType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Artifact 引用，描述 DAG 节点的产物。
 *
 * <p>实际数据走业务侧存储（如 PostGIS 表、MinIO 对象），这里只保留引用元信息。
 */
@Data
@NoArgsConstructor
public class ArtifactRef {

    /** INLINE / FILE / DATABASE / OBJECT_STORAGE */
    private DagArtifactType artifactType;
    /** 存储类型，如 MINIO / POSTGRES / LOCAL */
    private String storageType;
    /** 数据 URI（FILE/OBJECT_STORAGE 时为路径或 URL） */
    private String uri;
    /** INLINE 类型时的内联值 */
    private Map<String, Object> value;
    /** 业务自定义元信息 */
    private Map<String, Object> metadata;

    public ArtifactRef(DagArtifactType artifactType, String storageType, String uri,
                       Map<String, Object> value, Map<String, Object> metadata) {
        java.util.Objects.requireNonNull(artifactType, "artifactType");
        this.artifactType = artifactType;
        this.storageType = storageType;
        this.uri = uri;
        this.value = value == null ? Map.of() : value;
        this.metadata = metadata == null ? Map.of() : metadata;
    }

    public static ArtifactRef inline(Map<String, Object> value) {
        return new ArtifactRef(DagArtifactType.INLINE, null, null, value, Map.of());
    }

    public static ArtifactRef file(String uri, String storageType) {
        return new ArtifactRef(DagArtifactType.FILE, storageType, uri, Map.of(), Map.of());
    }
}
