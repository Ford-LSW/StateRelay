package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.DagEdgeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * DAG 边 JPA Repository。
 */
public interface DagEdgeRepository extends JpaRepository<DagEdgeEntity, Long> {

    List<DagEdgeEntity> findByDagDefinitionVersionId(Long dagDefinitionVersionId);

    List<DagEdgeEntity> findByDagDefinitionVersionIdAndToNodeId(Long dagDefinitionVersionId, String toNodeId);
}
