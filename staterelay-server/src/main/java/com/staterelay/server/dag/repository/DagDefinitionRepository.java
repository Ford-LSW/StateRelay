package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.DagDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * DAG 定义 JPA Repository，处理简单 CRUD。
 */
public interface DagDefinitionRepository extends JpaRepository<DagDefinitionEntity, Long> {

    Optional<DagDefinitionEntity> findByAppIdAndDagCode(Long appId, String dagCode);
}
