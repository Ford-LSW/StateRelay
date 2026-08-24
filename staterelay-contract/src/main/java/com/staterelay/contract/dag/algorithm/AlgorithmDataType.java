package com.staterelay.contract.dag.algorithm;

/**
 * 算法输入输出数据类型常量（对齐文档 §3.1）。
 *
 * <p>使用 {@code String} 而非枚举，便于业务侧扩展自定义类型（如各种 OPTIONS 子类型）
 * 以及避免算法集契约被框架枚举锁死。扩展类型仍必须由运行时显式支持；未注册或无法识别的
 * 类型会 fail-closed 拒绝，不能仅凭任意字符串绕过契约校验。
 *
 * <h3>框架内置类型（与 spatial / binding 子包对应）</h3>
 * <ul>
 *   <li>{@link #VECTOR_LAYER_REF} —— 对应 {@link com.staterelay.contract.dag.spatial.VectorLayerRef}</li>
 *   <li>{@link #VECTOR_DATASET_REF} —— 对应 {@link com.staterelay.contract.dag.spatial.VectorDatasetRef}</li>
 * </ul>
 *
 * <h3>业务自定义类型</h3>
 * <p>例如 {@link #INTERSECTION_OPTIONS}、{@link #JSON} 等由算法开发包声明，
 * 框架按已知结构或已注册校验器检查；未知自定义类型必须先提供校验支持。
 */
public final class AlgorithmDataType {

    private AlgorithmDataType() {
    }

    /** 矢量图层引用 */
    public static final String VECTOR_LAYER_REF = "VECTOR_LAYER_REF";
    /** 矢量数据集容器引用 */
    public static final String VECTOR_DATASET_REF = "VECTOR_DATASET_REF";
    /** 通用 JSON 结构 */
    public static final String JSON = "JSON";
    /** GDAL_INTERSECTION 算法选项 */
    public static final String INTERSECTION_OPTIONS = "INTERSECTION_OPTIONS";
    /** GDAL_ERASE 系列算法选项 */
    public static final String ERASE_OPTIONS = "ERASE_OPTIONS";

    /**
     * 判断数据类型是否为框架明确支持的算法选项类型。
     *
     * @param dataType 算法端口数据类型
     * @return 已明确支持时返回 {@code true}
     */
    public static boolean isSupportedOptionsType(String dataType) {
        return INTERSECTION_OPTIONS.equals(dataType) || ERASE_OPTIONS.equals(dataType);
    }
}
