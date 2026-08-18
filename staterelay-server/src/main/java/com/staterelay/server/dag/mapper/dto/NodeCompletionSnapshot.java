package com.staterelay.server.dag.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * NodeCompletionHandler 处理完成节点时使用的快照数据。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeCompletionSnapshot {
    private Long nodeExecutionId;
    private Long dagInstanceId;
    private String nodeId;
    private String handlerName;
    private Long taskInstanceId;
    private Instant finishedAt;
}
