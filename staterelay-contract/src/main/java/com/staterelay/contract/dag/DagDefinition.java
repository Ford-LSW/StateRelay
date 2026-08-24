package com.staterelay.contract.dag;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.staterelay.contract.dag.binding.InputBinding;
import com.staterelay.contract.dag.enums.DagEdgeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * DAG 定义（不可变快照，存入 {@code sr_dag_definition_version.definition_snapshot}）。
 *
 * <p>定义态描述"流程长什么样"，包含节点、边和 DAG 级输入。
 * 边可以分为显式控制边（{@link DagEdgeType#CONTROL}）和数据依赖隐式边
 * （在发布时由 input 中的 {@code ${nodes.X.outputs.Y}} 表达式或
 * {@link InputBinding#NODE_OUTPUT} 绑定推导）。
 *
 * <p><b>双协议并存（对齐 GIS-Worker 设计文档 §6）：</b>
 * <ul>
 *   <li>旧协议：{@link DagNode#getInputs()} 中存放 {@code ${dag.inputs.*}} /
 *       {@code ${nodes.*.outputs.*}} 字符串表达式 + 静态值</li>
 *   <li>新协议：{@link DagNode#getInputBindings()} 中存放结构化 {@link InputBinding}
 *       （sourceType / nodeCode / outputKey / selector）</li>
 * </ul>
 * 同一节点应二选一，DAG 发布校验会拒绝同时配置两种协议的节点。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DagDefinition {

    /** DAG 业务编码 */
    private String dagCode;
    /** DAG 名称 */
    private String dagName;
    /** 版本号 */
    private Integer version;
    /** DAG 级输入声明 */
    private Map<String, DagInput> inputs;
    /** 节点列表 */
    private List<DagNode> nodes;
    /** 边列表（含控制边 + 数据依赖隐式边） */
    private List<DagEdge> edges;

    /** DAG 级输入声明 */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DagInput {
        private String type;
        private boolean required;
    }

    /**
     * DAG 节点定义。
     *
     * <p>双协议并存：
     * <ul>
     *   <li>{@link #getHandler()} + {@link #getInputs()} —— 旧协议，{@code handler}
     *       为执行器名，{@code inputs} 为 {@code ${}} 表达式 + 静态值的扁平 Map</li>
     *   <li>{@link #getAlgorithmCode()} + {@link #getInputBindings()} —— 新协议（GIS），
     *       {@code algorithmCode} 对应 {@link com.staterelay.contract.dag.algorithm.AlgorithmContract}，
     *       {@code inputBindings} 为结构化绑定</li>
     * </ul>
     * 当 {@link #getAlgorithmCode()} 非空时，走新协议；否则走旧协议。
     * 同一节点应二选一，{@link #getHandler()} 与 {@link #getAlgorithmCode()} 不能同时非空。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DagNode {
        /** 节点 ID（节点唯一编码，文档中使用 {@code code}，例如 A、B、C、D） */
        private String id;
        /** 节点名称 */
        private String name;

        /** 旧协议：执行器名称，对应 Worker HandlerRegistry 中的 handlerName */
        private String handler;

        /**
         * 新协议：算法编码，对应 {@link com.staterelay.contract.dag.algorithm.AlgorithmContract#getAlgorithmCode()}。
         * 非空时优先于 {@link #handler} 使用，作为派发 Worker 的 algorithmCode。
         */
        private String algorithmCode;

        /**
         * 旧协议：输入参数扁平 Map。
         * Value 可以是 {@code ${dag.inputs.*}} / {@code ${nodes.*.outputs.*}} 字符串表达式或静态值。
         */
        private Map<String, Object> inputs;

        /**
         * 新协议：结构化输入绑定 Map（对齐文档 §6）。
         * Key 为算法契约输入端口名（必须与 {@link com.staterelay.contract.dag.algorithm.AlgorithmInputDef} 一致），
         * Value 为 {@link InputBinding}（含 sourceType / nodeCode / outputKey / selector / value）。
         */
        private Map<String, InputBinding> inputBindings;

        private Integer timeoutSeconds;
        private RetryConfig retry;
    }

    /** DAG 边定义 */
    @Data
    @NoArgsConstructor
    public static class DagEdge {
        private String from;
        private String to;
        private DagEdgeType edgeType;
        private String condition;

        public DagEdge(String from, String to, DagEdgeType edgeType, String condition) {
            java.util.Objects.requireNonNull(from, "from");
            java.util.Objects.requireNonNull(to, "to");
            java.util.Objects.requireNonNull(edgeType, "edgeType");
            this.from = from;
            this.to = to;
            this.edgeType = edgeType;
            this.condition = condition;
        }

        public static DagEdge control(String from, String to) {
            return new DagEdge(from, to, DagEdgeType.CONTROL, null);
        }

        public static DagEdge data(String from, String to) {
            return new DagEdge(from, to, DagEdgeType.DATA, null);
        }
    }

    /** 节点重试配置 */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RetryConfig {
        private Integer maxAttempts;
        private Integer delaySeconds;
    }
}
