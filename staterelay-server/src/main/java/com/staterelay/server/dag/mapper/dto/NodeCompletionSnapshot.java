package com.staterelay.server.dag.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * NodeCompletionHandler 处理完成节点时使用的快照数据。
 *
 * <p>对齐文档 §29.1 / §29.4：markSuccess 的 RETURNING 子句返回 NodeInstance 关键字段，
 * 供 Artifact 持久化、后继推进使用。
 *
 * <p>字段同时兼容：
 * <ul>
 *   <li>新表 {@code sr_dag_node_instance}：{@link #nodeInstanceId} / {@link #resultJson} / {@link #resultRef}</li>
 *   <li>老表 {@code sr_dag_node_execution}：{@link #nodeExecutionId} / {@link #taskInstanceId}</li>
 * </ul>
 * 后续老表下线时清理兼容字段。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeCompletionSnapshot {
    /** 新表 NodeInstance 主键（sr_dag_node_instance.id） */
    private Long nodeInstanceId;
    /** 老表 NodeExecution 主键（sr_dag_node_execution.id），保留兼容 */
    private Long nodeExecutionId;
    /** 所属 DagInstance 主键 */
    private Long dagInstanceId;
    /** DAG 定义中的 node_id（业务标识，非主键） */
    private String nodeId;
    /** 节点对应的 handler 名称，用于路由 Artifact 写入 */
    private String handlerName;
    /** 执行结果 JSON（attempt.result_json 回写，新表使用） */
    private String resultJson;
    /** 执行结果引用（如外部存储 URL，新表使用） */
    private String resultRef;
    /** 老表关联的 task_instance_id，保留兼容 */
    private Long taskInstanceId;
    /** 完成时间 */
    private Instant finishedAt;
}
