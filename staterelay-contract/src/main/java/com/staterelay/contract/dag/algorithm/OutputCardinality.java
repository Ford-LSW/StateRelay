package com.staterelay.contract.dag.algorithm;

/**
 * 算法输出基数（对齐文档 §13.2）。
 *
 * <p>描述算法输出端口的值的结构：
 * <ul>
 *   <li>{@link #SINGLE} —— 单值，例如 {@code resultLayer: {artifactId, layerName, ...}}</li>
 *   <li>{@link #LIST} —— 有序列表，例如按几何类型拆分的多图层</li>
 *   <li>{@link #MAP} —— 命名映射，例如按业务键组织的多图层</li>
 * </ul>
 *
 * <p>第一版绑定解析支持：
 * <pre>
 *   SINGLE 直接读取
 *   MAP 通过 MAP_KEY 选择
 *   LIST 第一版仅保存结构，暂不实现 LIST_INDEX 绑定选择器（文档 §13.2 末）
 * </pre>
 */
public enum OutputCardinality {
    /** 单值输出 */
    SINGLE,
    /** 有序列表输出 */
    LIST,
    /** 命名映射输出 */
    MAP
}
