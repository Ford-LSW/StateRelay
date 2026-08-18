package com.staterelay.server.dag.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.ArtifactRef;
import com.staterelay.server.dag.entity.DagArtifactEntity;
import com.staterelay.server.dag.entity.DagNodeExecutionEntity;
import com.staterelay.server.dag.repository.DagArtifactRepository;
import com.staterelay.server.dag.repository.DagNodeExecutionJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * DAG Artifact Service，负责节点完成时持久化 Artifact。
 */
@Service
public class DagArtifactService {

    private final DagArtifactRepository artifactRepository;
    private final DagNodeExecutionJpaRepository nodeExecutionRepository;
    private final ObjectMapper objectMapper;

    public DagArtifactService(DagArtifactRepository artifactRepository,
                              DagNodeExecutionJpaRepository nodeExecutionRepository,
                              ObjectMapper objectMapper) {
        this.artifactRepository = artifactRepository;
        this.nodeExecutionRepository = nodeExecutionRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * 持久化节点的所有输出 Artifact。
     */
    @Transactional
    public void saveArtifacts(Long dagInstanceId, String nodeId, Map<String, ArtifactRef> outputs) {
        if (outputs == null || outputs.isEmpty()) {
            return;
        }
        DagNodeExecutionEntity node = nodeExecutionRepository.findByDagInstanceIdAndNodeId(dagInstanceId, nodeId)
            .orElseThrow(() -> new IllegalStateException("Node not found: " + nodeId));
        Instant now = Instant.now();
        outputs.forEach((name, ref) -> {
            DagArtifactEntity entity = new DagArtifactEntity();
            entity.setDagInstanceId(dagInstanceId);
            entity.setNodeId(nodeId);
            entity.setOutputName(name);
            entity.setArtifactType(ref.getArtifactType());
            entity.setStorageType(ref.getStorageType());
            entity.setUri(ref.getUri());
            entity.setValueJson(writeJson(ref.getValue()));
            entity.setMetadata(writeJson(ref.getMetadata()));
            entity.setCreatedAt(now);
            artifactRepository.save(entity);
        });
    }

    /**
     * 查询节点的所有 Artifact。
     */
    public List<DagArtifactEntity> findNodeArtifacts(Long dagInstanceId, String nodeId) {
        return artifactRepository.findByDagInstanceIdAndNodeId(dagInstanceId, nodeId);
    }

    private String writeJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize artifact value", e);
        }
    }
}
