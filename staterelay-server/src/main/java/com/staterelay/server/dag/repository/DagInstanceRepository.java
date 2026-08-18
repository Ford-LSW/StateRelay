package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.DagInstanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * DAG 实例 JPA Repository。
 *
 * <p>仅处理简单查询；租约抢占、CAS 推进走 MyBatis Mapper。
 */
public interface DagInstanceRepository extends JpaRepository<DagInstanceEntity, Long> {

    Optional<DagInstanceEntity> findByIdempotencyKey(String idempotencyKey);
}
