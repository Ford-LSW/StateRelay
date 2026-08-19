package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.NodeAttemptEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * DAG 节点执行尝试 JPA Repository。
 */
public interface NodeAttemptRepository extends JpaRepository<NodeAttemptEntity, Long> {

    Optional<NodeAttemptEntity> findByNodeInstanceIdAndAttemptNo(Long nodeInstanceId, Integer attemptNo);

    Optional<NodeAttemptEntity> findByRequestId(String requestId);
}
