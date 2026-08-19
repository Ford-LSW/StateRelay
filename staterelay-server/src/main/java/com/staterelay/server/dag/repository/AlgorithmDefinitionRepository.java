package com.staterelay.server.dag.repository;

import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * 算法定义 JPA Repository。
 */
public interface AlgorithmDefinitionRepository extends JpaRepository<AlgorithmDefinitionEntity, Long> {

    Optional<AlgorithmDefinitionEntity> findByAlgorithmCode(String algorithmCode);

    Optional<AlgorithmDefinitionEntity> findByAlgorithmCodeAndStatus(String algorithmCode, AlgorithmDefinitionStatus status);
}
