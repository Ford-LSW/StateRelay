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
 * <p>状态机对齐文档：
 * <ul>
 *   <li>INIT(0)：实例已建，NodeInstance 尚未全部创建</li>
 *   <li>RUNNING(10)：NodeInstance 全部创建完，DAG Engine 轮询推进</li>
 *   <li>SUCCESS(20)/FAILED(30)/CANCELLED(40)：终态</li>
 * </ul>
 *
 * <p>去掉了 orchestration_state 子状态机，由 status 唯一驱动。
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
     * 扫描 RUNNING 实例（DAG Engine 轮询推进）。
     *
     * <p>领取条件：
     * <ul>
     *   <li>status = RUNNING(10)</li>
     *   <li>lease_expire_time &gt; NOW（租约有效）或 lease_expire_time IS NULL</li>
     * </ul>
     */
    List<DagInstanceLease> scanRunningInstances(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * DAG 实例终态化：CAS RUNNING → SUCCESS/FAILED/CANCELLED。
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
     * 取消 DAG 实例：CAS → CANCELLED(40)，lease_version+1。
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
