package com.staterelay.contract.dag.binding;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * 复合输出选择器（对齐文档 §6.3、§13.2）。
 *
 * <p>用于 {@link InputBinding} 的 {@code NODE_OUTPUT} 形态，当 {@code outputKey}
 * 指向 Map 或 List 时，进一步定位其中一个值。
 *
 * <p>第一版仅支持 {@link BindingSelectorType#MAP_KEY}，结构如下：
 * <pre>{@code
 * {
 *   "type": "MAP_KEY",
 *   "key": "layerA"
 * }
 * }</pre>
 *
 * <p>{@code selector} 为可选字段：
 * <ul>
 *   <li>无 selector —— 上游输出为 SINGLE 基数，直接读取</li>
 *   <li>{@link BindingSelectorType#MAP_KEY} —— 从 MAP 输出按 key 取值</li>
 * </ul>
 */
@Data
@NoArgsConstructor
public class BindingSelector {

    /** 选择器类型，第一版固定 {@link BindingSelectorType#MAP_KEY} */
    private BindingSelectorType type;

    /** {@link BindingSelectorType#MAP_KEY} 形态下，要选取的 Map Key */
    private String key;

    public BindingSelector(BindingSelectorType type, String key) {
        this.type = Objects.requireNonNull(type, "type");
        this.key = Objects.requireNonNull(key, "key");
    }

    /** 构造 MAP_KEY 选择器 */
    public static BindingSelector mapKey(String key) {
        return new BindingSelector(BindingSelectorType.MAP_KEY, key);
    }
}
