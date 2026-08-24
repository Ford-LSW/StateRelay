package com.staterelay.server.dag.mapper;

import com.staterelay.server.dag.mapper.dto.NodeCompletionSnapshot;
import com.staterelay.server.dag.mapper.dto.NodeInstanceLease;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 节点实例 MyBatis Mapper。
 *
 * <p>对应 XML：{@code resources/mapper/NodeInstanceMapper.xml}
 *
 * <p>状态机对齐文档 §11 / §37.3：
 * <ul>
 *   <li>WAITING(0)：创建时统一初始化</li>
 *   <li>READY(10)：DAG Engine CAS 推进（根节点首轮 / 前驱全部 SUCCESS）</li>
 *   <li>DISPATCHING(20)：Scheduler CAS 抢占</li>
 *   <li>DISPATCHED(30)：异步模式下使用；同步模式跳过（§11）</li>
 *   <li>RUNNING(40)：Worker 执行中</li>
 *   <li>SUCCESS(50)/FAILED(60)/CANCELLED(70)/SKIPPED(80)/TIMEOUT(90)：终态</li>
 * </ul>
 *
 * <p>同步模式状态推进：DISPATCHING → RUNNING（跳过 DISPATCHED）。
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
     * Scheduler 扫描可领取的 READY 节点（§26）。
     *
     * <p>领取条件：
     * <ul>
     *   <li>status = READY(10)</li>
     *   <li>next_schedule_time IS NULL 或 next_schedule_time &lt;= now（退避到期）</li>
     * </ul>
     */
    List<NodeInstanceLease> scanReadyNodes(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * CAS: READY(10) → DISPATCHING(20)（Scheduler 抢占，§26）。
     */
    int claimForDispatch(@Param("nodeInstanceId") Long nodeInstanceId,
                         @Param("dispatchOwner") String dispatchOwner,
                         @Param("dispatchLeaseExpireTime") Instant dispatchLeaseExpireTime,
                         @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) → DISPATCHED(30)（仅异步模式使用，§11）。
     */
    int markDispatched(@Param("nodeInstanceId") Long nodeInstanceId,
                       @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) / DISPATCHED(30) → RUNNING(40)（Worker 开始执行，§11 / §27 step 7）。
     *
     * <p>同步模式：DISPATCHING → RUNNING；异步模式：DISPATCHED → RUNNING。
     *
     * <p>同时固化 {@code current_attempt_no = #{attemptNo}}（GIS-Worker 设计文档 §16.2 围栏校验依据）：
     * NodeAttemptSyncService 在 Attempt SUCCESS 时校验
     * {@code attempt.attemptNo == node.currentAttemptNo} 以识别晚到 Attempt。
     */
    int markRunning(@Param("nodeInstanceId") Long nodeInstanceId,
                    @Param("attemptNo") Integer attemptNo,
                    @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → SUCCESS(50)，返回快照用于写 Artifact + 触发后继推进。
     * 在同一事务内与 Attempt → SUCCESS、后继 → READY 一起完成。
     *
     * <p>事务边界 T2：调用方在同事务内调 {@link DagInstanceMapper#incrementFinishedCount}（§29.1 / §39.1）。
     *
     * <p><b>Attempt 围栏（GIS-Worker 设计文档 §16.2）：</b>
     * CAS 条件增加 {@code current_attempt_no = #{attemptNo}}，防止晚到 Attempt 的 SUCCESS
     * 覆盖当前权威 Attempt。若 CAS 返回 null（snapshot == null），调用方应将本 Attempt
     * 的 STAGED Artifact 标记为 ORPHANED。
     */
    NodeCompletionSnapshot markSuccess(@Param("nodeInstanceId") Long nodeInstanceId,
                                       @Param("attemptNo") Integer attemptNo,
                                       @Param("resultJson") String resultJson,
                                       @Param("resultRef") String resultRef,
                                       @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → FAILED(60)（执行失败且重试耗尽，§29.3）。
     *
     * <p>事务边界 T4：调用方在同事务内调 {@link DagInstanceMapper#incrementFinishedCount}。
     */
    int markFailed(@Param("nodeInstanceId") Long nodeInstanceId,
                   @Param("errorCode") String errorCode,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → TIMEOUT(90)（执行超时且重试耗尽，§29.4）。
     *
     * <p>事务边界 T4：调用方在同事务内调 {@link DagInstanceMapper#incrementFinishedCount}。
     */
    int markTimeout(@Param("nodeInstanceId") Long nodeInstanceId,
                    @Param("errorCode") String errorCode,
                    @Param("errorMessage") String errorMessage,
                    @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) → FAILED(60)（参数解析失败，§17）。
     *
     * <p>参数解析失败属于配置错误，不重试，不增加 retry_count。
     * 同事务调 {@link DagInstanceMapper#incrementFinishedCount}（§39.1 T6）。
     */
    int markFailedFromDispatching(@Param("nodeInstanceId") Long nodeInstanceId,
                                  @Param("errorCode") String errorCode,
                                  @Param("errorMessage") String errorMessage,
                                  @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) → FAILED(60)（无可用 Worker 且达上限，§9.1 / §27 step 5）。
     *
     * <p>同事务调 {@link DagInstanceMapper#incrementFinishedCount}。
     */
    int markFailedForNoWorker(@Param("nodeInstanceId") Long nodeInstanceId,
                              @Param("scheduleFailCount") int scheduleFailCount,
                              @Param("errorCode") String errorCode,
                              @Param("errorMessage") String errorMessage,
                              @Param("now") Instant now);

    /**
     * CAS: WAITING(0) → SKIPPED(80)（前驱失败链式跳过，§20.2，第一版预留，主流程不使用）。
     */
    int markSkipped(@Param("nodeInstanceId") Long nodeInstanceId,
                    @Param("now") Instant now);

    /**
     * CAS: 回退到 READY(10)（重试，§29.2 / §29.4）。
     *
     * <p>从 DISPATCHING(20) / DISPATCHED(30) / RUNNING(40) / TIMEOUT(90) 回退。
     * 重试场景下不增加 finished_node_count（未进入终态）。
     *
     * <p>SQL 只增加 retry_count；current_attempt_no 由下一次真实 Attempt 创建事务写入，
     * 避免重试计数和物理 Attempt 编号各自加一次。
     */
    int revertToReady(@Param("nodeInstanceId") Long nodeInstanceId,
                      @Param("nextScheduleTime") Instant nextScheduleTime,
                      @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(20) → READY(10)（无可用 Worker 但未达上限，§27 step 5）。
     *
     * <p>schedule_fail_count 自增；next_schedule_time = 退避后时间。
     * 不创建 NodeAttempt，不增加 retry_count。
     */
    int revertToReadyForNoWorker(@Param("nodeInstanceId") Long nodeInstanceId,
                                 @Param("nextScheduleTime") Instant nextScheduleTime,
                                 @Param("errorCode") String errorCode,
                                 @Param("errorMessage") String errorMessage,
                                 @Param("now") Instant now);

    /**
     * CAS: RUNNING(40) → READY(10)（Worker 本地容量拒绝，§27.2）。
     *
     * <p>对齐文档 §27.2：已创建 Attempt 并 DISPATCHING → RUNNING 之后，
     * Worker 在进入 Handler 前返回 CAPACITY_REJECTED。
     * 与无 Worker 区分：Attempt FAILED + 走 T6C 而非 T3 重试；
     * schedule_fail_count 自增、不计 retry_count、不创建新 Attempt。
     *
     * <p>同步模式 Attempt 已是 RUNNING(30)，这里只回退 NodeInstance 维度。
     *
     * @return 受影响行数
     */
    int revertToReadyForCapacityRejected(@Param("nodeInstanceId") Long nodeInstanceId,
                                          @Param("scheduleFailCount") int scheduleFailCount,
                                          @Param("nextScheduleTime") Instant nextScheduleTime,
                                          @Param("errorCode") String errorCode,
                                          @Param("errorMessage") String errorMessage,
                                          @Param("now") Instant now);

    /**
     * 重置 schedule_fail_count = 0（§9.1 重置时机）。
     *
     * <p>触发时机：
     * <ul>
     *   <li>heartbeat 证明 Handler 已运行（T8A 同事务调用）</li>
     *   <li>Worker 返回非容量拒绝的 SUCCESS / FAILED 结果</li>
     *   <li>非容量拒绝 Attempt 按 TIMEOUT 收敛</li>
     * </ul>
     *
     * <p>幂等：CAS 仅在 schedule_fail_count > 0 时更新。
     */
    int resetScheduleFailCount(@Param("nodeInstanceId") Long nodeInstanceId,
                                @Param("now") Instant now);

    /**
     * 扫描 DISPATCHING 卡住节点（dispatch_lease_expire_time 过期），回退到 READY。
     */
    List<Long> scanStuckDispatching(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 取消未开始节点（WAITING/READY/DISPATCHING → CANCELLED），写 cancel_reason（§33.3 / §33.6 步骤 2）。
     *
     * <p>返回受影响行数（实际进入 CANCELLED 的节点数）；调用方据此 +1 finished_node_count。
     */
    int cancelUnstartedNodes(@Param("dagInstanceId") Long dagInstanceId,
                              @Param("cancelReason") String cancelReason,
                              @Param("now") Instant now);

    /**
     * 取消运行中节点（RUNNING → CANCELLED），写 cancel_reason（§33.4 / §33.6 步骤 3）。
     *
     * <p>由 Scheduler Scanner 在发现 CANCELLING 实例时调用 Worker cancel 成功后调用。
     * 返回受影响行数；调用方据此 +1 finished_node_count。
     */
    int cancelRunningNodes(@Param("dagInstanceId") Long dagInstanceId,
                           @Param("cancelReason") String cancelReason,
                           @Param("now") Instant now);

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
        private Integer timeoutCount;
        private Integer cancelledCount;
        private Integer skippedCount;
        private Integer activeCount;  // 非终态节点数
    }
}
