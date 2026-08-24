package com.staterelay.server.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.contract.dag.algorithm.AlgorithmContract;
import com.staterelay.contract.dag.enums.AlgorithmDefinitionStatus;
import com.staterelay.contract.dag.enums.DagDefinitionVersionStatus;
import com.staterelay.server.dag.entity.AlgorithmDefinitionEntity;
import com.staterelay.server.dag.entity.DagDefinitionEntity;
import com.staterelay.server.dag.entity.DagDefinitionVersionEntity;
import com.staterelay.server.dag.entity.DagEdgeEntity;
import com.staterelay.server.dag.mapper.DagEdgeMapper;
import com.staterelay.server.dag.repository.DagDefinitionRepository;
import com.staterelay.server.dag.repository.DagDefinitionVersionRepository;
import com.staterelay.server.dag.repository.DagEdgeRepository;
import com.staterelay.server.dag.repository.AlgorithmDefinitionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DAG 定义管理 Service，负责 DAG 元信息、版本发布。
 */
@Service
public class DagDefinitionService {

    private static final DagDefinitionVersionStatus VERSION_STATUS_DRAFT = DagDefinitionVersionStatus.DRAFT;
    private static final DagDefinitionVersionStatus VERSION_STATUS_ENABLED = DagDefinitionVersionStatus.ENABLED;

    private final DagDefinitionRepository definitionRepository;
    private final DagDefinitionVersionRepository versionRepository;
    private final DagEdgeRepository edgeRepository;
    private final DagEdgeMapper edgeMapper;
    private final DagDefinitionValidator validator;
    private final ObjectMapper objectMapper;
    private final AlgorithmDefinitionRepository algorithmDefinitionRepository;
    private final AlgorithmContractAdapter algorithmContractAdapter;

    public DagDefinitionService(DagDefinitionRepository definitionRepository,
                                DagDefinitionVersionRepository versionRepository,
                                DagEdgeRepository edgeRepository,
                                DagEdgeMapper edgeMapper,
                                DagDefinitionValidator validator,
                                ObjectMapper objectMapper,
                                AlgorithmDefinitionRepository algorithmDefinitionRepository,
                                AlgorithmContractAdapter algorithmContractAdapter) {
        this.definitionRepository = definitionRepository;
        this.versionRepository = versionRepository;
        this.edgeRepository = edgeRepository;
        this.edgeMapper = edgeMapper;
        this.validator = validator;
        this.objectMapper = objectMapper;
        this.algorithmDefinitionRepository = algorithmDefinitionRepository;
        this.algorithmContractAdapter = algorithmContractAdapter;
    }

    /**
     * 创建 DAG 定义（不含版本）。
     */
    @Transactional
    public DagDefinitionEntity createDefinition(Long appId, String dagCode, String dagName, String description) {
        DagDefinitionEntity entity = new DagDefinitionEntity();
        entity.setAppId(appId);
        entity.setDagCode(dagCode);
        entity.setDagName(dagName);
        entity.setDescription(description);
        entity.setEnabled(true);
        Instant now = Instant.now();
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return definitionRepository.save(entity);
    }

    /**
     * 保存草稿版本，同时持久化 DAG 边到 {@code sr_dag_edge}。
     */
    @Transactional
    public DagDefinitionVersionEntity saveDraft(Long dagDefinitionId, DagDefinition definition) {
        String snapshot = writeJson(definition);
        String hash = sha256Hex(snapshot);

        DagDefinitionVersionEntity version = new DagDefinitionVersionEntity();
        version.setDagDefinitionId(dagDefinitionId);
        version.setVersionNo(nextVersionNo(dagDefinitionId));
        version.setStatus(VERSION_STATUS_DRAFT);
        version.setDefinitionSnapshot(snapshot);
        version.setDefinitionHash(hash);
        Instant now = Instant.now();
        version.setCreatedAt(now);
        version.setUpdatedAt(now);
        DagDefinitionVersionEntity saved = versionRepository.save(version);

        // 边单独持久化，便于 Mapper 查询
        if (definition.getEdges() != null && !definition.getEdges().isEmpty()) {
            List<DagEdgeMapper.EdgeInsert> edges = definition.getEdges().stream()
                .map(e -> new DagEdgeMapper.EdgeInsert(
                    e.getFrom(), e.getTo(), e.getEdgeType(), e.getCondition()))
                .toList();
            edgeMapper.batchInsertEdges(saved.getId(), edges);
        }
        return saved;
    }

    /**
     * 发布草稿版本（对齐文档 §3.2：发布时强制做拓扑校验，不通过则状态保持 DRAFT）。
     */
    @Transactional
    public DagDefinitionVersionEntity publish(Long versionId) {
        DagDefinitionVersionEntity version = versionRepository.findById(versionId)
            .orElseThrow(() -> new IllegalArgumentException("DAG version not found: " + versionId));
        if (!VERSION_STATUS_DRAFT.equals(version.getStatus())) {
            throw new IllegalStateException("Only DRAFT version can be published");
        }
        // 发布前强制拓扑与结构化绑定校验，失败抛 IllegalArgumentException，状态保持 DRAFT
        DagDefinition snapshot = loadSnapshot(version);
        validator.validate(snapshot);
        validator.validateInputBindings(snapshot, loadAlgorithmContracts(snapshot));
        version.setStatus(VERSION_STATUS_ENABLED);
        version.setPublishedAt(Instant.now());
        version.setUpdatedAt(Instant.now());
        return versionRepository.save(version);
    }

    /**
     * 查询 DAG 定义。
     */
    public Optional<DagDefinitionEntity> findDefinition(Long appId, String dagCode) {
        return definitionRepository.findByAppIdAndDagCode(appId, dagCode);
    }

    /**
     * 查询最新发布版本。
     */
    public Optional<DagDefinitionVersionEntity> findLatestPublished(Long dagDefinitionId) {
        return versionRepository.findTopByDagDefinitionIdAndStatusOrderByVersionNoDesc(
            dagDefinitionId, VERSION_STATUS_ENABLED);
    }

    /**
     * 查询指定版本。
     */
    public Optional<DagDefinitionVersionEntity> findVersion(Long dagDefinitionId, Integer versionNo) {
        return versionRepository.findByDagDefinitionIdAndVersionNo(dagDefinitionId, versionNo);
    }

    /**
     * 按版本 ID 查询版本实体。
     */
    public Optional<DagDefinitionVersionEntity> findVersionEntity(Long versionId) {
        return versionRepository.findById(versionId);
    }

    /**
     * 加载 DAG 定义快照为 {@link DagDefinition}。
     */
    public DagDefinition loadSnapshot(DagDefinitionVersionEntity version) {
        try {
            return objectMapper.readValue(version.getDefinitionSnapshot(), DagDefinition.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse dag definition snapshot", e);
        }
    }

    public List<DagEdgeEntity> findEdges(Long dagDefinitionVersionId) {
        return edgeRepository.findByDagDefinitionVersionId(dagDefinitionVersionId);
    }

    private Map<String, AlgorithmContract> loadAlgorithmContracts(DagDefinition snapshot) {
        Map<String, AlgorithmContract> contracts = new LinkedHashMap<>();
        for (DagDefinition.DagNode node : snapshot.getNodes()) {
            String algorithmCode = node.getAlgorithmCode();
            if (algorithmCode == null || algorithmCode.isBlank() || contracts.containsKey(algorithmCode)) {
                continue;
            }
            AlgorithmDefinitionEntity definition = algorithmDefinitionRepository
                .findByAlgorithmCodeAndStatus(algorithmCode, AlgorithmDefinitionStatus.ENABLED)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Enabled algorithm definition not found: " + algorithmCode));
            contracts.put(algorithmCode, algorithmContractAdapter.toContract(definition));
        }
        return contracts;
    }

    private Integer nextVersionNo(Long dagDefinitionId) {
        return versionRepository.countByDagDefinitionId(dagDefinitionId) + 1;
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize dag definition", e);
        }
    }

    private String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
