package com.staterelay.contract.dag;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DAG 节点执行结果（业务侧 Handler 通过 {@code TaskResult.success(outputs)} 携带）。
 */
@Data
@NoArgsConstructor
public class NodeResult {

    public enum NodeStatus { SUCCESS, FAILED, SKIPPED }

    /** 节点终态 */
    private NodeStatus status;
    /** 产物映射，key = output port 名称 */
    private Map<String, ArtifactRef> outputs;
    /** 描述信息 */
    private String message;
    /** 失败时的错误信息 */
    private String errorMessage;

    public NodeResult(NodeStatus status, Map<String, ArtifactRef> outputs,
                      String message, String errorMessage) {
        java.util.Objects.requireNonNull(status, "status");
        this.status = status;
        this.outputs = outputs == null ? Map.of() : outputs;
        this.message = message;
        this.errorMessage = errorMessage;
    }

    public static NodeResult success() {
        return new NodeResult(NodeStatus.SUCCESS, Map.of(), null, null);
    }

    public static NodeResult success(Map<String, ArtifactRef> outputs) {
        return new NodeResult(NodeStatus.SUCCESS, outputs, null, null);
    }

    public static NodeResult failed(String errorMessage) {
        return new NodeResult(NodeStatus.FAILED, Map.of(), null, errorMessage);
    }

    public static NodeResult skipped(String reason) {
        return new NodeResult(NodeStatus.SKIPPED, Map.of(), reason, null);
    }
}
