package com.staterelay.server.dag.mapper;

import com.staterelay.server.dag.mapper.dto.NodeCompletionSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 节点实例 MyBatis Mapper。
 *
 * <p>对应 XML：{@code resources/mapper/NodeInstanceMapper.xml}
 *
 * <p>状态机对齐文档：
 * <ul>
 *   <li>WAITING(0)：创建时统一初始化</li>
 *   <li>READY(10)：DAG Engine CAS 推进（根节点首轮 / 前驱全部 SUCCESS）</li>
 *   <li>DISPATCHING(20)：Scheduler CAS 抢占</li>
 *   <li>DISPATCHED(30)：NodeAttempt 已创建，远程调用已发出</li>
 *   <li>RUNNING(40)：Worker ACK</li>
 *   <li>SUCCESS(50)/FAILED(60)/CANCELLED(70)/SKIPPED(80)/TIMEOUT(90)：终态或过渡态</li>
 * </ul>
 */
@Mapper
public interface NodeInstanceMapper {

    /**
     * 批量创建节点实例（DAG 实例启动时一次性插入所有节点，统一初始化为 WAITING）。
     */
    int batchInsert(@Param("dagInstanceId") Long dagInstanceId,
                    @Param("nodes") List<NodeInsert> nodes,
                    @Param("now") Instant now);

    /**
     * 查询可推进到 READY 的 WAITING 节点：
     * <ul>
     *   <li>无前驱的根节点（天然满足）</li>
     *   <li>前驱全部 SUCCESS 的后继节点</li>
     * </ul>
     * DAG Engine 每轮扫描统一调用。
     */
    List<Long> findReadyableNodes(@Param("dagInstanceId") Long dagInstanceId);

    /**
     * CAS: WAITING(0) → READY(10)，冻结 binding 快照。
     */
    int markReady(@Param("nodeInstanceId") Long nodeInstanceId,
                  @Param("bindings") String bindings,
                  @Param("now") Instant now);

    /**
     * CAS: READY(10) → DISPATCHING(20)（Scheduler 抢占）。
     */
    int claimForDispatch(@Param("nodeInstanceId") Long nodeInstanceId,
                         @Param("dispatchOwner") String dispatchOwner,
                         @Param("dispatchLeaseExpireTime") Instant dispatchLeaseExpireTime,
                         @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) → DISPATCHED(30)（NodeAttempt 已创建）。
     */
    int markDispatched(@Param("nodeInstanceId") Long nodeInstanceId,
                       @Param("now") Instant now);

    /**
     * CAS: DISPATCHED(30) → RUNNING(40)（Worker ACK）。
     */
    int markRunning(@Param("nodeInstanceId") Long nodeInstanceId,
                    @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → SUCCESS(50)，返回快照用于写 Artifact + 触发后继推进。
     * 在同一事务内与 Attempt → SUCCESS、后继 → READY 一起完成。
     */
    NodeCompletionSnapshot markSuccess(@Param("nodeInstanceId") Long nodeInstanceId,
                                       @Param("resultJson") String resultJson,
                                       @Param("resultRef") String resultRef,
                                       @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → FAILED(60)。
     */
    int markFailed(@Param("nodeInstanceId") Long nodeInstanceId,
                   @Param("errorCode") String errorCode,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    /**
     * CAS: WAITING(0) → SKIPPED(80)（前驱失败链式跳过）。
     */
    int markSkipped(@Param("nodeInstanceId") Long nodeInstanceId,
                    @Param("now") Instant now);

    /**
     * CAS: 回退到 READY(10)（重试或派发失败回退）。
     * 从 DISPATCHING(20) / DISPATCHED(30) / RUNNING(40) / TIMEOUT(90) 回退。
     */
    int revertToReady(@Param("nodeInstanceId") Long nodeInstanceId,
                      @Param("retryCount") int retryCount,
                      @Param("currentAttemptNo") int currentAttemptNo,
                      @Param("nextScheduleTime") Instant nextScheduleTime,
                      @Param("now") Instant now);

    /**
     * 扫描 DISPATCHING 卡住节点（dispatch_lease_expire_time 过期），回退到 READY。
     */
    List<Long> scanStuckDispatching(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 取消未启动节点（WAITING/READY/DISPATCHING/DISPATCHED → CANCELLED）。
     */
    int cancelUnstartedNodes(@Param("dagInstanceId") Long dagInstanceId, @Param("now") Instant now);

    /**
     * 取消运行中节点（RUNNING → CANCELLED）。
     */
    int cancelRunningNodes(@Param("dagInstanceId") Long dagInstanceId, @Param("now") Instant now);

    /**
     * 统计实例下各状态节点数。
     */
    NodeStatusCount countByStatus(@Param("dagInstanceId") Long dagInstanceId);

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

    /**
     * 节点状态统计。
     */
    @lombok.Data
    static class NodeStatusCount {
        private Integer totalCount;
        private Integer successCount;
        private Integer failedCount;
        private Integer skippedCount;
        private Integer activeCount;  // 非终态节点数
    }
}
