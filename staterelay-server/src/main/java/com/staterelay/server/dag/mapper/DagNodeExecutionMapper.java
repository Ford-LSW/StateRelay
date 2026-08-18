package com.staterelay.server.dag.mapper;

import com.staterelay.server.dag.mapper.dto.NodeCompletionSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 节点执行 MyBatis Mapper，处理状态推进、CAS、binding 快照等。
 *
 * <p>对应 XML：{@code resources/mapper/DagNodeExecutionMapper.xml}
 *
 * <p>状态/编排子状态以 INTEGER 存入数据库，编码见
 * {@link com.staterelay.contract.dag.enums.DagNodeExecutionStatus} 与
 * {@link com.staterelay.contract.dag.enums.DagNodeExecutionOrchestrationState}。
 */
@Mapper
public interface DagNodeExecutionMapper {

    /**
     * 批量创建节点执行实例（DAG 实例启动时一次性插入所有节点）。
     */
    int batchInsert(@Param("dagInstanceId") Long dagInstanceId,
                    @Param("nodes") List<NodeInsert> nodes,
                    @Param("now") Instant now);

    /**
     * DagOrchestratorService 推进 PENDING(0) → WAITING(1)。
     * CAS: WHERE status = 0
     */
    int markWaiting(@Param("nodeExecutionId") Long nodeExecutionId, @Param("now") Instant now);

    /**
     * 节点推进 WAITING(1) → READY(2)（解析 binding 并冻结快照）。
     * CAS: WHERE status = 1
     */
    int markReady(@Param("nodeExecutionId") Long nodeExecutionId,
                  @Param("bindings") String bindings,
                  @Param("now") Instant now);

    /**
     * 节点 READY(2) → orch_state QUEUED(2)（关联 task_instance_id）。
     * CAS: WHERE status = 2 AND orch_state = 1 AND task_instance_id IS NULL
     */
    int markQueued(@Param("nodeExecutionId") Long nodeExecutionId,
                   @Param("taskInstanceId") Long taskInstanceId,
                   @Param("now") Instant now);

    /**
     * DispatchScanner 领取后，节点 READY(2) → RUNNING(3), orch_state QUEUED(2) → EXECUTING(3)。
     * 由 NodeCompletionHandler 监听 TaskInstance 状态变更时反向触发，CAS 推进。
     */
    int markRunning(@Param("nodeExecutionId") Long nodeExecutionId,
                    @Param("now") Instant now);

    /**
     * 节点 orch_state EXECUTING(3) → WAITING_RESULT(4)（Executor ACK 后）。
     */
    int markWaitingResult(@Param("nodeExecutionId") Long nodeExecutionId, @Param("now") Instant now);

    /**
     * 节点 orch_state WAITING_RESULT(4) → DONE(5), status RUNNING(3) → SUCCESS(4)。
     * 返回快照用于写 Artifact + 触发重编排。
     */
    NodeCompletionSnapshot markSuccess(@Param("nodeExecutionId") Long nodeExecutionId,
                                       @Param("now") Instant now);

    /**
     * 节点 orch_state WAITING_RESULT(4) → DONE(5), status RUNNING(3) → FAILED(5)。
     */
    int markFailed(@Param("nodeExecutionId") Long nodeExecutionId,
                   @Param("errorCode") String errorCode,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    /**
     * 取消未启动的节点（status 0/1/2 → 8=CANCELLED）。
     */
    int cancelUnstartedNodes(@Param("dagInstanceId") Long dagInstanceId, @Param("now") Instant now);

    /**
     * 取消运行中的节点（status 3=RUNNING → 7=CANCELLING）。
     */
    int cancelRunningNodes(@Param("dagInstanceId") Long dagInstanceId, @Param("now") Instant now);

    /**
     * 节点重试：status FAILED(5) → READY(2), orch_state DONE(5) → READY(1)（重新解析 binding 刷新快照）。
     */
    int retryNode(@Param("nodeExecutionId") Long nodeExecutionId,
                  @Param("bindings") String bindings,
                  @Param("now") Instant now);

    /**
     * 用于批量插入的节点参数。
     */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class NodeInsert {
        private String nodeId;
        private String nodeName;
        private String handlerName;
    }
}
