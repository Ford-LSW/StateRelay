package com.staterelay.server.dag.service;

import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import com.staterelay.server.dag.mapper.DagNodeExecutionMapper;
import com.staterelay.server.dag.repository.DagNodeExecutionJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DAG 节点 Service，负责节点查询、重试等。
 *
 * <p>状态推进涉及 CAS 和 binding 快照更新，由 {@link DagNodeExecutionMapper} 完成；
 * 简单查询走 JPA。
 */
@Service
public class DagNodeExecutionService {

    private final DagNodeExecutionJpaRepository jpaRepository;
    private final DagNodeExecutionMapper mapper;

    public DagNodeExecutionService(DagNodeExecutionJpaRepository jpaRepository,
                                   DagNodeExecutionMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    public Optional<DagNodeExecutionEntity> findNode(Long dagInstanceId, String nodeId) {
        return jpaRepository.findByDagInstanceIdAndNodeId(dagInstanceId, nodeId);
    }

    public List<DagNodeExecutionEntity> listNodes(Long dagInstanceId) {
        return jpaRepository.findByDagInstanceId(dagInstanceId);
    }

    /**
     * 重试失败节点（重新解析 binding 由上层 Orchestration 完成）。
     */
    @Transactional
    public int retryNode(Long nodeExecutionId, String bindingsJson) {
        return mapper.retryNode(nodeExecutionId, bindingsJson, Instant.now());
    }
}
