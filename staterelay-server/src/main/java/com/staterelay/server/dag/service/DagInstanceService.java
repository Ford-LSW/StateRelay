package com.staterelay.server.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.contract.dag.StartDagInstanceRequest;
import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import com.staterelay.contract.dag.enums.DagInstanceOrchestrationState;
import com.staterelay.contract.dag.enums.DagInstanceStatus;
import com.staterelay.server.dag.entity.DagDefinitionEntity;
import com.staterelay.server.dag.entity.DagDefinitionVersionEntity;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import com.staterelay.server.dag.mapper.DagInstanceMapper;
import com.staterelay.server.dag.mapper.DagNodeExecutionMapper;
import com.staterelay.server.dag.repository.DagInstanceRepository;
import com.staterelay.server.dag.repository.DagNodeExecutionJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DAG 实例 Service，负责启动实例（幂等）、查询实例状态、取消实例。
 *
 */
@Service
public class DagInstanceService {

    private static final DagInstanceStatus STATUS_PENDING = DagInstanceStatus.PENDING;
    private static final DagInstanceStatus STATUS_RUNNING = DagInstanceStatus.RUNNING;
    private static final DagInstanceStatus STATUS_CANCELLING = DagInstanceStatus.CANCELLING;
    private static final DagInstanceStatus STATUS_CANCELLED = DagInstanceStatus.CANCELLED;
    private static final DagInstanceStatus STATUS_SUCCESS = DagInstanceStatus.SUCCESS;
    private static final DagInstanceStatus STATUS_FAILED = DagInstanceStatus.FAILED;

    private static final Duration DEFAULT_LEASE = Duration.ofSeconds(60);
    private static final Duration DEFAULT_NEXT_SCHEDULE_DELAY = Duration.ofSeconds(5);

    private final DagDefinitionService definitionService;
    private final DagInstanceRepository instanceRepository;
    private final DagNodeExecutionJpaRepository nodeExecutionRepository;
    private final DagInstanceMapper instanceMapper;
    private final DagNodeExecutionMapper nodeExecutionMapper;
    private final ObjectMapper objectMapper;

    public DagInstanceService(DagDefinitionService definitionService,
                              DagInstanceRepository instanceRepository,
                              DagNodeExecutionJpaRepository nodeExecutionRepository,
                              DagInstanceMapper instanceMapper,
                              DagNodeExecutionMapper nodeExecutionMapper,
                              ObjectMapper objectMapper) {
        this.definitionService = definitionService;
        this.instanceRepository = instanceRepository;
        this.nodeExecutionRepository = nodeExecutionRepository;
        this.instanceMapper = instanceMapper;
        this.nodeExecutionMapper = nodeExecutionMapper;
        this.objectMapper = objectMapper;
    }

    /**
     * 启动 DAG 实例（幂等：相同 idempotency_key 直接返回已存在的实例）。
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
        if (!DagDefinitionVersionStatus.PUBLISHED.equals(version.getStatus())) {
            throw new IllegalStateException("DAG version is not PUBLISHED: " + version.getId());
        }

        DagDefinition snapshot = definitionService.loadSnapshot(version);

        DagInstanceEntity instance = new DagInstanceEntity();
        instance.setDagDefinitionVersionId(version.getId());
        instance.setDagCodeSnapshot(snapshot.getDagCode());
        instance.setDagVersionSnapshot(snapshot.getVersion());
        instance.setDefinitionHashSnapshot(version.getDefinitionHash());
        instance.setBusinessId(request.getBusinessId());
        instance.setIdempotencyKey(request.getIdempotencyKey());
        instance.setInputJson(writeJson(request.getInputs()));
        instance.setStatus(STATUS_RUNNING);
        instance.setOrchestrationState(DagInstanceOrchestrationState.READY);
        instance.setOrchestrationVersion(0L);
        Instant now = Instant.now();
        instance.setNextScheduleTime(now.plus(DEFAULT_NEXT_SCHEDULE_DELAY));
        instance.setStartedAt(now);
        instance.setLeaseExpireTime(now.plus(DEFAULT_LEASE));
        instance.setTotalNodeCount(snapshot.getNodes().size());
        instance.setCreatedAt(now);
        instance.setUpdatedAt(now);
        DagInstanceEntity saved = instanceRepository.save(instance);

        // 批量创建节点执行实例
        if (snapshot.getNodes() != null && !snapshot.getNodes().isEmpty()) {
            List<DagNodeExecutionMapper.NodeInsert> nodes = snapshot.getNodes().stream()
                .map(n -> new DagNodeExecutionMapper.NodeInsert(n.getId(), n.getName(), n.getHandler()))
                .toList();
            nodeExecutionMapper.batchInsert(saved.getId(), nodes, now);
        }
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
    public List<DagNodeExecutionEntity> listNodes(Long instanceId) {
        return nodeExecutionRepository.findByDagInstanceId(instanceId);
    }

    /**
     * 取消 DAG 实例（status → CANCELLING, lease_version+1）。
     * 节点取消交由 Orchestrator 在下一轮推进时处理。
     */
    @Transactional
    public int cancelInstance(Long instanceId) {
        return instanceMapper.cancelInstance(instanceId, Instant.now());
    }

    public DagInstanceEntity toRunning(DagInstanceEntity instance) {
        if (STATUS_PENDING.equals(instance.getStatus())) {
            instance.setStatus(STATUS_RUNNING);
        }
        return instance;
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
