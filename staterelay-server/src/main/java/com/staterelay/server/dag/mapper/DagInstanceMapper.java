package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 实例 MyBatis Mapper，处理租约抢占、CAS 推进、唤醒等复杂操作。
 *
 * <p>对应 XML：{@code resources/mapper/DagInstanceMapper.xml}
 *
 * <p>所有状态参数使用 {@link com.staterelay.contract.dag.enums.CodedEnum} 枚举，
 * XML 中以 {@code #{xxx.code}} 取数值编码写入 INTEGER 列。
 */
@Mapper
public interface DagInstanceMapper {

    /**
     * DagOrchestratorScanner 领取可推进的 DAG 实例（FOR UPDATE SKIP LOCKED）。
     *
     * <p>领取条件：
     * <ul>
     *   <li>status = RUNNING(1)</li>
     *   <li>orchestration_state = READY(0)</li>
     *   <li>next_schedule_time &lt;= NOW</li>
     *   <li>lease_expire_time &gt; NOW（租约有效）</li>
     * </ul>
     */
    List<DagInstanceLease> scanDueInstances(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * CAS 推进 dag_instance.orchestration_state: READY(0) → QUEUED(1)（version+1）。
     *
     * @return 1 表示抢占成功，0 表示已被其他实例抢占或租约失效
     */
    int tryEnqueueForAdvance(@Param("dagInstanceId") Long dagInstanceId,
                              @Param("workerId") String workerId,
                              @Param("leaseVersion") Long leaseVersion,
                              @Param("now") Instant now,
                              @Param("deadline") Instant deadline);

    /**
     * 抢占成功后查询当前 orchestration_version（用于 Runnable 启动时校验）。
     */
    Long getCurrentOrchestrationVersion(@Param("dagInstanceId") Long dagInstanceId);

    /**
     * Runnable 启动时校验版本，CAS: QUEUED(1) → EXECUTING(2)。
     *
     * @return 1 表示成功（Runnable 仍有效），0 表示过期
     */
    int tryEnterExecuting(@Param("dagInstanceId") Long dagInstanceId,
                          @Param("expectedVersion") Long expectedVersion,
                          @Param("workerId") String workerId,
                          @Param("leaseVersion") Long leaseVersion,
                          @Param("now") Instant now);

    /**
     * CAS: EXECUTING(2) → WAITING_NODES(3)（version+1），表示本轮推进结束，等待节点完成。
     */
    int tryEnterWaitingNodes(@Param("dagInstanceId") Long dagInstanceId,
                             @Param("now") Instant now);

    /**
     * 统一唤醒入口：CAS: WAITING_NODES(3) → READY(0)（version+1）。
     *
     * <p>幂等：0 行返回表示 dag_instance 已终态/已取消/已被唤醒。
     */
    int tryReenqueue(@Param("dagInstanceId") Long dagInstanceId,
                     @Param("now") Instant now);

    /**
     * DAG 实例终态化：标记 SUCCESS / FAILED / CANCELLED，写入 finished_at 和统计字段。
     */
    int finalizeInstance(@Param("dagInstanceId") Long dagInstanceId,
                         @Param("status") DagInstanceStatus status,
                         @Param("successCount") int successCount,
                         @Param("failedCount") int failedCount,
                         @Param("skippedCount") int skippedCount,
                         @Param("errorCode") String errorCode,
                         @Param("errorMessage") String errorMessage,
                         @Param("now") Instant now);

    /**
     * 取消 DAG 实例：status → CANCELLING(4), lease_version+1（旧 Orchestrator 立即失效）。
     */
    int cancelInstance(@Param("dagInstanceId") Long dagInstanceId,
                       @Param("now") Instant now);

    /**
     * 更新心跳与租约。
     */
    int updateLease(@Param("dagInstanceId") Long dagInstanceId,
                    @Param("workerId") String workerId,
                    @Param("leaseVersion") Long leaseVersion,
                    @Param("now") Instant now,
                    @Param("leaseExpireTime") Instant leaseExpireTime);
}
