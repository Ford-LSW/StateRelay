package com.staterelay.contract.dag.binding;

/**
 * InputBinding selector 类型（对齐文档 §6.3、§13.2）。
 *
 * <p>当上游 {@code outputKey} 指向复合结构（Map / List）时，selector 描述如何从中
 * 选择一个值。第一版仅支持：
 *
 * <ul>
 *   <li>无 selector —— 上游输出为 {@code SINGLE} 基数，直接读取</li>
 *   <li>{@link #MAP_KEY} —— 上游输出为 {@code MAP} 基数，按 Key 选择（§6.3、§13.2）</li>
 * </ul>
 *
 * <p>第一版<b>暂不实现</b> {@code LIST_INDEX}，避免过度复杂（文档 §13.2 末）。
 */
public enum BindingSelectorType {
    /** 从 MAP 输出中按 key 选择值（§6.3） */
    MAP_KEY
}
