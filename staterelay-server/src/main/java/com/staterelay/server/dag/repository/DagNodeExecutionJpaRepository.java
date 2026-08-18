package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * DAG 节点执行 JPA Repository。
 *
 * <p>仅处理简单查询；状态推进、CAS、binding 快照更新走 MyBatis Mapper。
 */
public interface DagNodeExecutionJpaRepository extends JpaRepository<DagNodeExecutionEntity, Long> {

    Optional<DagNodeExecutionEntity> findByDagInstanceIdAndNodeId(Long dagInstanceId, String nodeId);

    List<DagNodeExecutionEntity> findByDagInstanceId(Long dagInstanceId);

    List<DagNodeExecutionEntity> findByDagInstanceIdAndStatusIn(Long dagInstanceId, List<String> statuses);
}
