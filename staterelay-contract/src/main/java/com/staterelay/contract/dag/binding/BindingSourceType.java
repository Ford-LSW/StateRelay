package com.staterelay.contract.dag.binding;

/**
 * InputBinding 来源类型（对齐文档 §6.1）。
 *
 * <p>第一版 {@code sourceType} 固定为以下三种，描述"本节点的每个算法输入具体从哪里取得"：
 *
 * <ul>
 *   <li>{@link #DAG_INPUT} —— 从本次 {@code DagInstance.input_json} 中读取（§6.2）</li>
 *   <li>{@link #NODE_OUTPUT} —— 从同一 DagInstance 的某上游 NodeInstance 权威输出读取（§6.3）</li>
 *   <li>{@link #CONST} —— 从 DAG 快照中的固定配置读取（§6.4）</li>
 * </ul>
 *
 * <p>Worker 不理解 {@code sourceType}（文档 §8.2），仅接收 {@link InputBinding} 解析后的最终参数。
 * 本枚举是调度中心侧的数据绑定语言。
 */
public enum BindingSourceType {
    /** 从 DagInstance.input_json 读取（§6.2） */
    DAG_INPUT,
    /** 从上游 NodeInstance 权威输出读取（§6.3） */
    NODE_OUTPUT,
    /** 从 DAG 快照固定配置读取（§6.4） */
    CONST
}
