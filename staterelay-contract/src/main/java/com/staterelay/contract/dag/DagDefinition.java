package com.staterelay.contract.dag;

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
 * （在发布时由 input 中的 {@code ${nodes.X.outputs.Y}} 表达式推导）。
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

    /** DAG 节点定义 */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DagNode {
        private String id;
        private String name;
        private String handler;
        private Map<String, Object> inputs;
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
