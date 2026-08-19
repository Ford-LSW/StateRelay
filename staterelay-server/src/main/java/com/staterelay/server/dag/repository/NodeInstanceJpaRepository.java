package com.staterelay.server.dag.repository;

import com.staterelay.contract.dag.enums.NodeInstanceStatus;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * DAG 节点实例 JPA Repository。
 *
 * <p>仅处理简单查询；状态推进、CAS、binding 快照更新走 MyBatis Mapper。
 */
public interface NodeInstanceJpaRepository extends JpaRepository<NodeInstanceEntity, Long> {

    Optional<NodeInstanceEntity> findByDagInstanceIdAndNodeId(Long dagInstanceId, String nodeId);

    List<NodeInstanceEntity> findByDagInstanceId(Long dagInstanceId);

    List<NodeInstanceEntity> findByDagInstanceIdAndStatusIn(Long dagInstanceId, List<NodeInstanceStatus> statuses);
}
