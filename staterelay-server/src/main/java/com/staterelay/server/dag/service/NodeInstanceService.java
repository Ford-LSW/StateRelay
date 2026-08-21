package com.staterelay.server.dag.service;

import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DAG 节点实例 Service（对齐文档 NodeInstance 模型）。
 *
 * <p>简单查询走 JPA；状态推进、CAS、binding 快照更新走 MyBatis Mapper。
 */
@Service
public class NodeInstanceService {

    private final NodeInstanceJpaRepository jpaRepository;
    private final NodeInstanceMapper mapper;

    public NodeInstanceService(NodeInstanceJpaRepository jpaRepository,
                                NodeInstanceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    public Optional<NodeInstanceEntity> findNode(Long dagInstanceId, String nodeId) {
        return jpaRepository.findByDagInstanceIdAndNodeId(dagInstanceId, nodeId);
    }

    public List<NodeInstanceEntity> listNodes(Long dagInstanceId) {
        return jpaRepository.findByDagInstanceId(dagInstanceId);
    }

    /**
     * 重试失败节点：CAS 当前状态 → READY，retry_count + 1，attempt_no + 1。
     * 对齐文档 §20.1：重试由 Scheduler 触发（持有 max_retry 配置）。
     *
     * <p>retry_count / current_attempt_no 由 SQL 自增（避免并发丢失更新），
     * 无需 Java 传值。
     */
    @Transactional
    public int retryNode(Long nodeInstanceId) {
        Instant now = Instant.now();
        return mapper.revertToReady(nodeInstanceId, now.plusSeconds(1), now);
    }
}
