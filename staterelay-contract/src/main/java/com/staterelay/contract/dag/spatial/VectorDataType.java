package com.staterelay.contract.dag.spatial;

/**
 * 空间数据引用类型标识（对齐文档 §4.2、§4.3）。
 *
 * <p>用于在 JSON 中区分引用指向的是<b>数据集容器</b>还是<b>具体图层</b>，
 * 与 {@link VectorStorageType} 共同确定引用语义。
 *
 * <ul>
 *   <li>{@link #VECTOR_DATASET} —— 容器引用，对应 {@link VectorDatasetRef}</li>
 *   <li>{@link #VECTOR_LAYER} —— 图层引用，对应 {@link VectorLayerRef}</li>
 * </ul>
 *
 * <p>算法层统一通过 {@code VECTOR_LAYER_REF} 契约类型接收（文档 §4.3 末），
 * 不直接感知 {@link VectorDatasetRef}；容器仅在 Artifact 下载/缓存层使用。
 */
public enum VectorDataType {
    /** 空间数据容器，例如完整 GDB */
    VECTOR_DATASET,
    /** 具体空间图层 */
    VECTOR_LAYER
}
