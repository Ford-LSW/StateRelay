package com.staterelay.server.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.contract.dag.StartDagInstanceRequest;
import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.server.dag.entity.DagDefinitionEntity;
import com.staterelay.server.dag.entity.DagDefinitionVersionEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.NodeInstanceMapper;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.NodeInstanceJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DAG 实例 Service。
 *
 * <p>对齐文档 §14：
 * <ol>
 *   <li>创建 DagInstance（status = INIT）</li>
 *   <li>同事务批量创建全部 NodeInstance（统一初始化为 WAITING，不解析拓扑）</li>
 *   <li>同事务 CAS DagInstance: INIT → RUNNING</li>
 * </ol>
 *
 * <p>避免出现"实例 RUNNING 但节点还没建好"的不一致窗口。
 */
@Service
public class DagInstanceService {

    private static final Duration DEFAULT_NEXT_SCHEDULE_DELAY = Duration.ofSeconds(5);

    private final DagDefinitionService definitionService;
    private final DagInstanceRepository instanceRepository;
    private final NodeInstanceJpaRepository nodeInstanceRepository;
    private final DagInstanceMapper instanceMapper;
    private final NodeInstanceMapper nodeInstanceMapper;
    private final ObjectMapper objectMapper;

    public DagInstanceService(DagDefinitionService definitionService,
                              DagInstanceRepository instanceRepository,
                              NodeInstanceJpaRepository nodeInstanceRepository,
                              DagInstanceMapper instanceMapper,
                              NodeInstanceMapper nodeInstanceMapper,
                              ObjectMapper objectMapper) {
        this.definitionService = definitionService;
        this.instanceRepository = instanceRepository;
        this.nodeInstanceRepository = nodeInstanceRepository;
        this.instanceMapper = instanceMapper;
        this.nodeInstanceMapper = nodeInstanceMapper;
        this.objectMapper = objectMapper;
    }

    /**
     * 启动 DAG 实例（幂等：相同 idempotency_key 直接返回已存在的实例）。
     *
     * <p>事务内：
     * <ol>
     *   <li>创建 DagInstance（INIT）</li>
     *   <li>批量创建全部 NodeInstance（WAITING）</li>
     *   <li>CAS DagInstance: INIT → RUNNING</li>
     * </ol>
     */
    @Transactional
    public DagInstanceEntity startInstance(Long appId, StartDagInstanceRequest request) {
        Optional<DagInstanceEntity> existing = instanceRepository.findByIdempotencyKey(request.getIdempotencyKey());
        if (existing.isPresent()) {
            return existing.get();
        }

        DagDefinitionEntity definition = definitionService.findDefinition(appId, request.getDagCode())
            .orElseThrow(() -> new IllegalArgumentException("DAG not found: " + request.getDagCode()));

        DagDefinitionVersionEntity version;
        if (request.getVersion() != null) {
            version = definitionService.findVersion(definition.getId(), request.getVersion())
                .orElseThrow(() -> new IllegalArgumentException("DAG version not found: " + request.getVersion()));
        } else {
            version = definitionService.findLatestPublished(definition.getId())
                .orElseThrow(() -> new IllegalStateException("No published version for dag: " + request.getDagCode()));
        }
        // 对齐文档 §3.2 / §29：只有 ENABLED 状态的 DagDefinition 才允许创建 DagInstance
        if (!DagDefinitionVersionStatus.ENABLED.equals(version.getStatus())) {
            throw new IllegalStateException("DAG version is not ENABLED: " + version.getId());
        }

        DagDefinition snapshot = definitionService.loadSnapshot(version);

        Instant now = Instant.now();
        DagInstanceEntity instance = new DagInstanceEntity();
        instance.setDagDefinitionVersionId(version.getId());
        instance.setDagCodeSnapshot(snapshot.getDagCode());
        instance.setDagVersionSnapshot(snapshot.getVersion());
        instance.setDefinitionHashSnapshot(version.getDefinitionHash());
        instance.setBusinessId(request.getBusinessId());
        instance.setIdempotencyKey(request.getIdempotencyKey());
        instance.setInputJson(writeJson(request.getInputs()));
        instance.setStatus(DagInstanceStatus.INIT);
        instance.setNextScheduleTime(now.plus(DEFAULT_NEXT_SCHEDULE_DELAY));
        instance.setTotalNodeCount(snapshot.getNodes().size());
        instance.setCreatedAt(now);
        instance.setUpdatedAt(now);
        DagInstanceEntity saved = instanceRepository.save(instance);

        // 同事务批量创建全部 NodeInstance，统一初始化为 WAITING，不解析拓扑（对齐文档 §5.1）
        if (snapshot.getNodes() != null && !snapshot.getNodes().isEmpty()) {
            List<NodeInstanceMapper.NodeInsert> nodes = snapshot.getNodes().stream()
                .map(n -> new NodeInstanceMapper.NodeInsert(n.getId(), n.getName(), n.getHandler()))
                .toList();
            nodeInstanceMapper.batchInsert(saved.getId(), nodes, now);
        }

        // NodeInstance 全部落盘后，CAS DagInstance: INIT → RUNNING（对齐文档 §14.2）
        int started = instanceMapper.tryStartInstance(saved.getId(), now);
        if (started == 0) {
            throw new IllegalStateException("Failed to start DAG instance " + saved.getId()
                + ": INIT → RUNNING CAS failed (concurrent start?)");
        }
        saved.setStatus(DagInstanceStatus.RUNNING);
        saved.setStartedAt(now);
        return saved;
    }

    /**
     * 查询 DAG 实例。
     */
    public Optional<DagInstanceEntity> findInstance(Long instanceId) {
        return instanceRepository.findById(instanceId);
    }

    /**
     * 查询 DAG 实例的所有节点。
     */
    public List<NodeInstanceEntity> listNodes(Long instanceId) {
        return nodeInstanceRepository.findByDagInstanceId(instanceId);
    }

    /**
     * 取消 DAG 实例（对齐文档）：
     * <ol>
     *   <li>CAS DagInstance → CANCELLED(40)，lease_version+1</li>
     *   <li>同事务把所有未终态节点 CAS 到 CANCELLED</li>
     * </ol>
     */
    @Transactional
    public int cancelInstance(Long instanceId) {
        Instant now = Instant.now();
        int updated = instanceMapper.cancelInstance(instanceId, now);
        if (updated > 0) {
            // 取消成功，同时取消所有未终态节点
            nodeInstanceMapper.cancelUnstartedNodes(instanceId, now);
            nodeInstanceMapper.cancelRunningNodes(instanceId, now);
        }
        return updated;
    }

    private String writeJson(Object value) {
        if (value == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize inputs", e);
        }
    }
}
