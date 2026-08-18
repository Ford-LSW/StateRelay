package com.staterelay.server.dag.repository;

import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import com.staterelay.server.dag.entity.DagDefinitionVersionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * DAG 定义版本 JPA Repository。
 *
 * <p>仅处理简单查询；发布流转（DRAFT→PUBLISHED）涉及事务和 CAS，走 MyBatis Mapper。
 */
public interface DagDefinitionVersionRepository extends JpaRepository<DagDefinitionVersionEntity, Long> {

    Optional<DagDefinitionVersionEntity> findTopByDagDefinitionIdAndStatusOrderByVersionNoDesc(
        Long dagDefinitionId, DagDefinitionVersionStatus status);

    Optional<DagDefinitionVersionEntity> findByDagDefinitionIdAndVersionNo(
        Long dagDefinitionId, Integer versionNo);

    int countByDagDefinitionId(Long dagDefinitionId);
}
