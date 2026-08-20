package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.server.dag.mapper.dto.DagInstanceLease;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 实例 MyBatis Mapper，处理 CAS 推进、租约、扫描。
 *
 * <p>对应 XML：{@code resources/mapper/DagInstanceMapper.xml}
 *
 * <p>状态机对齐文档 §33.1 / §37.2：
 * <ul>
 *   <li>INIT(0)：实例已建，NodeInstance 尚未全部创建</li>
 *   <li>RUNNING(10)：NodeInstance 全部创建完，DAG Engine 轮询推进</li>
 *   <li>CANCELLING(15)：用户取消中间态，等待 RUNNING 节点收敛</li>
 *   <li>FAILING(18)：失败中间态，等待清理后 → FAILED</li>
 *   <li>SUCCESS(20) / FAILED(30) / CANCELLED(40)：终态</li>
 * </ul>
 */
@Mapper
public interface DagInstanceMapper {

    /**
     * 扫描 INIT 实例（NodeInstance 创建完后 CAS 到 RUNNING）。
     *
     * <p>领取条件：
     * <ul>
     *   <li>status = INIT(0)</li>
     *   <li>next_schedule_time &lt;= NOW</li>
     * </ul>
     */
    List<DagInstanceLease> scanInitInstances(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * CAS: INIT(0) → RUNNING(10)（NodeInstance 全部创建完后调用）。
     *
     * @return 1 表示成功，0 表示已被其他实例处理
     */
    int tryStartInstance(@Param("dagInstanceId") Long dagInstanceId,
                         @Param("now") Instant now);

    /**
     * 扫描活跃实例（RUNNING / CANCELLING / FAILING），DAG Engine 轮询推进。
     *
     * <p>对齐文档 §25.1 / §35.1：scanner 需要扫这三种状态，统一做终态判断。
     *
     * <p>领取条件：
     * <ul>
     *   <li>status IN (10, 15, 18)</li>
     *   <li>lease_expire_time &gt; NOW（租约有效）或 lease_expire_time IS NULL</li>
     * </ul>
     */
    List<DagInstanceLease> scanActiveInstances(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * DAG 实例终态化：CAS 从中间态到终态（SUCCESS/FAILED/CANCELLED）。
     *
     * <p>支持：
     * <ul>
     *   <li>RUNNING → SUCCESS（全节点 SUCCESS）</li>
     *   <li>FAILING → FAILED（清理完成）</li>
     *   <li>CANCELLING → CANCELLED（取消完成）</li>
     * </ul>
     *
     * @param expectedFromStatus CAS 前置状态：RUNNING(10) / FAILING(18) / CANCELLING(15)
     */
    int finalizeInstance(@Param("dagInstanceId") Long dagInstanceId,
                         @Param("expectedFromStatus") DagInstanceStatus expectedFromStatus,
                         @Param("status") DagInstanceStatus status,
                         @Param("successCount") int successCount,
                         @Param("failedCount") int failedCount,
                         @Param("skippedCount") int skippedCount,
                         @Param("errorCode") String errorCode,
                         @Param("errorMessage") String errorMessage,
                         @Param("now") Instant now);

    /**
     * 启动取消事务：CAS RUNNING(10) → CANCELLING(15)，写 cancel_reason（§33.6 步骤 1）。
     *
     * <p>仅允许 RUNNING → CANCELLING。若 DagInstance 已经 FAILING（节点失败先发生）则 CAS 失败，返回 0。
     *
     * @return 1 表示 CAS 成功（取消生效）；0 表示状态已非 RUNNING（已被其他流程推进）
     */
    int startCancel(@Param("dagInstanceId") Long dagInstanceId,
                    @Param("cancelReason") String cancelReason,
                    @Param("now") Instant now);

    /**
     * 启动失败清理：CAS RUNNING(10) → FAILING(18)（§31.1 / §39.1 T7）。
     *
     * <p>仅允许 RUNNING → FAILING。若 DagInstance 已 CANCELLING（用户取消先发生）则 CAS 失败，返回 0。
     *
     * @return 1 表示 CAS 成功（进入失败清理）；0 表示状态已非 RUNNING
     */
    int startFailing(@Param("dagInstanceId") Long dagInstanceId,
                     @Param("now") Instant now);

    /**
     * 更新心跳与租约。
     */
    int updateLease(@Param("dagInstanceId") Long dagInstanceId,
                    @Param("workerId") String workerId,
                    @Param("leaseVersion") Long leaseVersion,
                    @Param("now") Instant now,
                    @Param("leaseExpireTime") Instant leaseExpireTime);

    /**
     * 同事务增加 finished_node_count（§39.1 T2/T4/T5/T6/T7 都需要）。
     *
     * <p>调用方在 NodeInstance CAS 成功进入终态后，在同一个 @Transactional 内调用本方法 +1。
     *
     * @param delta 本次新增的已完成节点数（通常 1；批量取消时为 N）
     */
    int incrementFinishedCount(@Param("dagInstanceId") Long dagInstanceId,
                               @Param("delta") int delta,
                               @Param("now") Instant now);
}
