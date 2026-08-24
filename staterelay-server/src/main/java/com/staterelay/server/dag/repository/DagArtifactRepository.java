package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.DagArtifactEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * DAG Artifact JPA Repository。
 */
public interface DagArtifactRepository extends JpaRepository<DagArtifactEntity, Long> {

    List<DagArtifactEntity> findByDagInstanceIdAndNodeId(Long dagInstanceId, String nodeId);

    List<DagArtifactEntity> findByDagInstanceId(Long dagInstanceId);

    /** 按物理 Attempt 和输出端口查询幂等登记记录。 */
    Optional<DagArtifactEntity> findByAttemptIdAndOutputName(String attemptId, String outputName);
}
