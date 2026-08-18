package com.staterelay.server.dag.repository;

import com.staterelay.server.dag.entity.HandlerDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Handler 定义 JPA Repository。
 *
 * <p>用于发布期校验 {@code dag_node.handler} 引用是否存在。
 */
public interface HandlerDefinitionRepository extends JpaRepository<HandlerDefinitionEntity, Long> {

    Optional<HandlerDefinitionEntity> findByAppIdAndHandlerNameAndHandlerVersionAndIsActiveTrue(
            Long appId, String handlerName, Integer handlerVersion);

    Optional<HandlerDefinitionEntity> findByAppIdAndHandlerNameAndIsActiveTrue(
            Long appId, String handlerName);
}
