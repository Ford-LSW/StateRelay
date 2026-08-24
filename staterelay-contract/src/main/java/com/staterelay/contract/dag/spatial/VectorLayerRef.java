package com.staterelay.contract.dag.spatial;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * 空间图层引用（对齐文档 §4.3）。
 *
 * <p>表示 GDB 或 PostGIS 中的一个具体图层，是算法输入的实际语义单位。
 * 算法契约统一通过 {@code VECTOR_LAYER_REF} 数据类型接收本对象（文档 §4.3 末）。
 *
 * <p>本类支持两种形态，由 {@link #storageType} 区分：
 *
 * <h3>1. FileGDB 图层（{@link VectorStorageType#FILE_GDB}）</h3>
 * <pre>{@code
 * {
 *   "dataType": "VECTOR_LAYER",
 *   "storageType": "FILE_GDB",
 *   "artifactId": 20001,            // ← 指向承载该图层的容器 VectorDatasetRef
 *   "layerName": "layerA",
 *   "geometryType": "MULTIPOLYGON",
 *   "srid": 4490
 * }
 * }</pre>
 *
 * <p><b>语义说明（文档 §4.4）：</b>
 * {@link #artifactId} 数值与承载该图层的 {@link VectorDatasetRef#getArtifactId()} 相同，
 * 表示"本图层属于该 GDB 容器"，而非本图层自己的 ID。图层自身没有独立 Artifact ID。
 *
 * <h3>2. PostGIS 图层（{@link VectorStorageType#POSTGIS}）</h3>
 * <pre>{@code
 * {
 *   "dataType": "VECTOR_LAYER",
 *   "storageType": "POSTGIS",
 *   "dataSourceId": 101,
 *   "schema": "public",
 *   "table": "pglayer_a",
 *   "geometryField": "geom",
 *   "geometryType": "MULTIPOLYGON",
 *   "srid": 4490
 * }
 * }</pre>
 *
 * <p>PostGIS 形态无 {@code artifactId}，因为 PostGIS 没有独立"容器 Artifact"概念。
 *
 * <p><b>必填校验（文档 §8.3）：</b>
 * <ul>
 *   <li>FileGDB：必须包含 {@code artifactId} + {@code layerName}</li>
 *   <li>PostGIS：必须包含 {@code dataSourceId} + {@code schema} + {@code table} + {@code geometryField}</li>
 * </ul>
 * 通过 {@link #hasRequiredFields()} 在 ParameterResolver 阶段做最终校验。
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VectorLayerRef {

    /** 固定为 {@link VectorDataType#VECTOR_LAYER} */
    private VectorDataType dataType = VectorDataType.VECTOR_LAYER;

    /**
     * 存储类型，决定引用形态：{@link VectorStorageType#FILE_GDB} 或 {@link VectorStorageType#POSTGIS}。
     * Worker 据此决定如何打开真实数据。
     */
    private VectorStorageType storageType;

    // ===== FileGDB 形态字段 =====

    /**
     * FileGDB 形态必填。指向承载该图层的容器 {@link VectorDatasetRef#getArtifactId()}，
     * <b>不是本图层自己的 ID</b>（图层无独立 Artifact ID）。
     */
    private Long artifactId;

    /** FileGDB 形态必填。容器内图层名 */
    private String layerName;

    // ===== PostGIS 形态字段 =====

    /** PostGIS 形态必填。数据源 ID（业务侧配置） */
    private Integer dataSourceId;

    /** PostGIS 形态必填。Schema 名 */
    private String schema;

    /** PostGIS 形态必填。表名 */
    private String table;

    /** PostGIS 形态必填。几何字段名 */
    private String geometryField;

    // ===== 公共字段 =====

    /** 几何类型，例如 {@link GeometryType#MULTIPOLYGON} */
    private String geometryType;

    /** 空间参考 ID，例如 4490（CGCS2000） */
    private Integer srid;

    public VectorLayerRef(VectorStorageType storageType) {
        this.dataType = VectorDataType.VECTOR_LAYER;
        this.storageType = Objects.requireNonNull(storageType, "storageType");
    }

    /**
     * 构造 FileGDB 图层引用。
     *
     * @param artifactId 承载该图层的容器 Artifact ID
     * @param layerName  容器内图层名
     * @param geometryType 几何类型，可空（部分算法可后置填充）
     * @param srid       空间参考 ID，可空
     */
    public static VectorLayerRef fileGdb(
            Long artifactId, String layerName, String geometryType, Integer srid) {
        VectorLayerRef ref = new VectorLayerRef(VectorStorageType.FILE_GDB);
        ref.artifactId = Objects.requireNonNull(artifactId, "artifactId");
        ref.layerName = Objects.requireNonNull(layerName, "layerName");
        ref.geometryType = geometryType;
        ref.srid = srid;
        return ref;
    }

    /**
     * 构造 PostGIS 图层引用。
     *
     * @param dataSourceId   数据源 ID
     * @param schema         Schema 名
     * @param table          表名
     * @param geometryField  几何字段名
     * @param geometryType   几何类型，可空
     * @param srid           空间参考 ID，可空
     */
    public static VectorLayerRef postgis(
            Integer dataSourceId, String schema, String table,
            String geometryField, String geometryType, Integer srid) {
        VectorLayerRef ref = new VectorLayerRef(VectorStorageType.POSTGIS);
        ref.dataSourceId = Objects.requireNonNull(dataSourceId, "dataSourceId");
        ref.schema = Objects.requireNonNull(schema, "schema");
        ref.table = Objects.requireNonNull(table, "table");
        ref.geometryField = Objects.requireNonNull(geometryField, "geometryField");
        ref.geometryType = geometryType;
        ref.srid = srid;
        return ref;
    }

    /**
     * 校验当前 storageType 形态下必填字段是否齐全（文档 §8.3）。
     *
     * @return true 表示必填字段齐全
     */
    public boolean hasRequiredFields() {
        if (storageType == VectorStorageType.FILE_GDB) {
            return artifactId != null && layerName != null && !layerName.isBlank();
        }
        if (storageType == VectorStorageType.POSTGIS) {
            return dataSourceId != null
                    && schema != null && !schema.isBlank()
                    && table != null && !table.isBlank()
                    && geometryField != null && !geometryField.isBlank();
        }
        return false;
    }
}
