package com.staterelay.contract.dag.spatial;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * 空间数据集容器引用（对齐文档 §4.2）。
 *
 * <p>表示一个空间数据容器，例如完整 FileGeodatabase（GDB）压缩包。
 * {@code artifactId} 是<b>容器本身的 ID</b>（自引用），与 {@link VectorLayerRef#getArtifactId()}
 * 引用同一容器时数值相同（文档 §4.4）。
 *
 * <p>仅描述容器定位信息，不包含图层细节。具体图层通过 {@link VectorLayerRef} 引用，
 * 由 {@code artifactId + layerName} 二元组定位同一容器内的不同图层。
 *
 * <p>Artifact 缓存/下载粒度是整个容器；算法输入语义使用 {@link VectorLayerRef}（文档 §4.4 末）。
 *
 * <p>JSON 示例（文档 §4.2）：
 * <pre>{@code
 * {
 *   "artifactId": 20001,
 *   "dataType": "VECTOR_DATASET",
 *   "storageType": "OBJECT_STORAGE",
 *   "format": "FILE_GDB",
 *   "objectKey": "dag/50001/A/attempt-1/source.gdb.zip",
 *   "checksum": "sha256:xxx"
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VectorDatasetRef {

    /** 数据集 Artifact ID（容器本身的 ID，自引用） */
    private Long artifactId;

    /** 固定为 {@link VectorDataType#VECTOR_DATASET} */
    private VectorDataType dataType = VectorDataType.VECTOR_DATASET;

    /** 固定为 {@link VectorStorageType#OBJECT_STORAGE}（容器始终存储在对象存储） */
    private VectorStorageType storageType = VectorStorageType.OBJECT_STORAGE;

    /**
     * 容器物理格式，例如 {@code FILE_GDB}、{@code SHAPEFILE}、{@code GEOJSON}。
     * Worker 依据该字段选择解压/打开方式。
     */
    private String format;

    /**
     * 对象存储 Key，例如 {@code dag/50001/A/attempt-1/source.gdb.zip}。
     * Worker 通过 {@link ArtifactMetadataClient} 用 {@code artifactId} 查询 objectKey，
     * 不直接由调度侧下传 objectKey（避免 URL 失效导致引用不一致）。
     */
    private String objectKey;

    /** 校验和，例如 {@code sha256:xxx}，用于下载后完整性校验 */
    private String checksum;

    public VectorDatasetRef(Long artifactId, String format, String objectKey, String checksum) {
        this.artifactId = Objects.requireNonNull(artifactId, "artifactId");
        this.dataType = VectorDataType.VECTOR_DATASET;
        this.storageType = VectorStorageType.OBJECT_STORAGE;
        this.format = Objects.requireNonNull(format, "format");
        this.objectKey = objectKey;
        this.checksum = checksum;
    }

    /**
     * 构造标准 FileGeodatabase 容器引用。
     *
     * @param artifactId 容器自身 Artifact ID
     * @param objectKey 对象存储 Key
     * @param checksum 校验和
     */
    public static VectorDatasetRef fileGdb(Long artifactId, String objectKey, String checksum) {
        return new VectorDatasetRef(artifactId, "FILE_GDB", objectKey, checksum);
    }
}
