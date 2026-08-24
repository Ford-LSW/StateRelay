package com.staterelay.contract.dag.spatial;

/**
 * 空间数据存储类型（对齐文档 §4、§20）。
 *
 * <p>第一版限定三种取值，区分承载图层的物理介质：
 * <ul>
 *   <li>{@link #FILE_GDB} —— Esri FileGeodatabase 文件容器，{@code VectorLayerRef} 通过 {@code artifactId + layerName} 定位</li>
 *   <li>{@link #POSTGIS} —— PostGIS 数据库图层，{@code VectorLayerRef} 通过 {@code dataSourceId + schema + table + geometryField} 定位</li>
 *   <li>{@link #OBJECT_STORAGE} —— 对象存储压缩包，仅 {@link VectorDatasetRef} 容器使用</li>
 * </ul>
 *
 * <p>{@link VectorDatasetRef#storageType} 固定为 {@link #OBJECT_STORAGE}；
 * {@link VectorLayerRef#storageType} 取 {@link #FILE_GDB} 或 {@link #POSTGIS}。
 * Worker 根据 {@code storageType} 决定打开真实数据的方式（文档 §4.3）。
 */
public enum VectorStorageType {
    /** FileGeodatabase 文件容器 */
    FILE_GDB,
    /** PostGIS 数据库图层 */
    POSTGIS,
    /** 对象存储压缩包（仅容器使用） */
    OBJECT_STORAGE
}
