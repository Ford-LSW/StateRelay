package com.staterelay.server.dag.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Scheduler 领取 READY 节点时的快照（FOR UPDATE SKIP LOCKED）。
 *
 * <p>对齐文档 §26：Scheduler 抢占 READY → DISPATCHING 时持有此快照，
 * 用于后续 Worker 调用、租约续约、卡住回退。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeInstanceLease {
    /** NodeInstance 主键 */
    private Long id;
    /** 所属 DagInstance 主键 */
    private Long dagInstanceId;
    /** DAG 定义中的 node_id（业务标识） */
    private String nodeId;
    /** 节点名称 */
    private String nodeName;
    /** 节点对应的 handler 名称，用于 Worker 路由 */
    private String handlerName;
    /** 抢占者标识（workerId + threadId 等） */
    private String dispatchOwner;
    /** 调度租约过期时间；过期未推进则回退 READY */
    private Instant dispatchLeaseExpireTime;
}
