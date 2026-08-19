package com.staterelay.server.dag.repository;

import com.staterelay.contract.dag.enums.ExecutorRegistrationStatus;
import com.staterelay.server.dag.entity.ExecutorRegistrationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Worker 注册 JPA Repository。
 */
public interface ExecutorRegistrationRepository extends JpaRepository<ExecutorRegistrationEntity, Long> {

    List<ExecutorRegistrationEntity> findByExecutorGroupCodeAndStatus(
            String executorGroupCode, ExecutorRegistrationStatus status);

    Optional<ExecutorRegistrationEntity> findByExecutorGroupCodeAndWorkerId(
            String executorGroupCode, String workerId);
}
