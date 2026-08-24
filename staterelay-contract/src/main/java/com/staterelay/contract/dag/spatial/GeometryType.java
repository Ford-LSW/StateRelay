package com.staterelay.contract.dag.spatial;

/**
 * OGC 几何类型常量（对齐 GDAL / PostGIS ST_GeometryType 命名）。
 *
 * <p>使用 {@code String} 而非枚举，便于扩展自定义几何类型（如带 Z/M 的三维几何、
 * 曲线几何）以及避免与具体 GIS 引擎实现强耦合。
 *
 * <p>调用方在 {@link VectorLayerRef#getGeometryType()} 中直接使用本类常量。
 */
public final class GeometryType {

    private GeometryType() {
    }

    public static final String POINT = "POINT";
    public static final String MULTIPOINT = "MULTIPOINT";
    public static final String LINESTRING = "LINESTRING";
    public static final String MULTILINESTRING = "MULTILINESTRING";
    public static final String POLYGON = "POLYGON";
    public static final String MULTIPOLYGON = "MULTIPOLYGON";
    public static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
}
