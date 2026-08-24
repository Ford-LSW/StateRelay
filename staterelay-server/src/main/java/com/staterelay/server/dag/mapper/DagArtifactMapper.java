package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.artifact.ArtifactMetadataResponse;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG Artifact MyBatis Mapper（对应 XML：{@code resources/mapper/DagArtifactMapper.xml}）。
 *
 * <p>承担 GIS-Worker 设计文档 §16.1 / §16.2 / §15.1 中的复杂状态机 CAS 与元数据查询：
 * <ul>
 *   <li>{@link #promoteStagedToAvailableByAttempt}：当前权威 Attempt SUCCESS 后批量 STAGED → AVAILABLE（§16.2）</li>
 *   <li>{@link #markStagedAsOrphanedByAttempt}：晚到 / 失败 Attempt 的 STAGED → ORPHANED（§16.2 / §16.3）</li>
 *   <li>{@link #findMetadataById}：Worker 通过 artifactId 查询容器元数据（§15.1 末段）</li>
 *   <li>{@link #scanOrphaned}：Artifact Cleanup Scanner 定期回收 ORPHANED（§19.4）</li>
 * </ul>
 *
 * <p>简单 CRUD（insert / findById / findByDagInstanceIdAndNodeId）走 JPA Repository；
 * 状态机 CAS、按 Attempt 批量推进、元数据投影走本 Mapper。
 */
@Mapper
public interface DagArtifactMapper {

    /**
     * 批量 CAS：STAGED(20) → AVAILABLE(30)，按当前权威 Attempt 精确推进（§16.2）。
     *
     * <p>调用时机：NodeAttemptSyncService 在 Attempt SUCCESS 且通过围栏校验后，同事务调用。
     *
     * <p>围栏条件：
     * <ul>
     *   <li>{@code dag_instance_id = #{dagInstanceId}}</li>
     *   <li>{@code node_id = #{nodeId}}</li>
     *   <li>{@code attempt_no = #{attemptNo}}</li>
     *   <li>{@code status = 20 (STAGED)}</li>
     * </ul>
     *
     * @return 推进为 AVAILABLE 的行数
     */
    int promoteStagedToAvailableByAttempt(@Param("dagInstanceId") Long dagInstanceId,
                                          @Param("nodeId") String nodeId,
                                          @Param("attemptNo") Integer attemptNo,
                                          @Param("now") Instant now);

    /**
     * 批量 CAS：STAGED(20) → ORPHANED(40)，标记晚到 / 失败 Attempt 的非权威成果（§16.2 / §16.3）。
     *
     * <p>调用时机：
     * <ul>
     *   <li>NodeAttemptSyncService 发现晚到 Attempt（attemptNo != currentAttemptNo）</li>
     *   <li>Attempt 终态为 FAILED / TIMEOUT 且不重试</li>
     * </ul>
     *
     * @return 标记为 ORPHANED 的行数
     */
    int markStagedAsOrphanedByAttempt(@Param("dagInstanceId") Long dagInstanceId,
                                       @Param("nodeId") String nodeId,
                                       @Param("attemptNo") Integer attemptNo,
                                       @Param("now") Instant now);

    /**
     * 按 artifactId 查询容器元数据（§15.1 末段）。
     *
     * <p>调度侧不下传 {@code objectKey} 给 Worker，统一以 {@code artifactId} 作为稳定 ID 查询。
     * Worker 通过 {@code ArtifactMetadataClient} SPI 调用 HTTP 端点，最终落到本方法。
     *
     * <p>仅返回 {@code status = AVAILABLE(30)} 的记录，下游读取前置条件（§16.2）。
     *
     * @return 元数据响应；不存在或不可读时返回 null
     */
    ArtifactMetadataResponse findMetadataById(@Param("artifactId") Long artifactId);

    /**
     * 扫描 ORPHANED(40) 待回收 Artifact（§19.4）。
     *
     * <p>由 Artifact Cleanup Scanner 定期调用，按 {@code updated_at} 排序回收。
     *
     * @param before    只扫描 {@code updated_at < before} 的记录（避免回收刚进入 ORPHANED 的记录）
     * @param batchSize 单批扫描上限
     * @return Artifact ID 列表
     */
    List<Long> scanOrphaned(@Param("before") Instant before, @Param("batchSize") int batchSize);

    /**
     * CAS: ORPHANED(40) → DELETING(50)（Cleanup Scanner 标记待物理删除，§19.4）。
     */
    int markOrphanedAsDeleting(@Param("artifactId") Long artifactId, @Param("now") Instant now);

    /**
     * CAS: DELETING(50) → DELETED(60)（物理删除完成，§19.4）。
     */
    int markDeletingAsDeleted(@Param("artifactId") Long artifactId, @Param("now") Instant now);
}
