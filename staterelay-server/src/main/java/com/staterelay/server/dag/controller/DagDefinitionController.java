package com.staterelay.server.dag.controller;

import com.staterelay.contract.dag.DagDefinition;
import com.staterelay.server.dag.entity.DagDefinitionEntity;
import com.staterelay.server.dag.entity.DagDefinitionVersionEntity;
import com.staterelay.server.dag.service.DagDefinitionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * DAG 定义 Controller。
 */
@RestController
@RequestMapping("/api/v1/dag/definitions")
public class DagDefinitionController {

    private final DagDefinitionService definitionService;

    public DagDefinitionController(DagDefinitionService definitionService) {
        this.definitionService = definitionService;
    }

    @PostMapping
    public DagDefinitionEntity createDefinition(@RequestParam Long appId,
                                                @RequestParam String dagCode,
                                                @RequestParam String dagName,
                                                @RequestParam(required = false) String description) {
        return definitionService.createDefinition(appId, dagCode, dagName, description);
    }

    @PostMapping("/{definitionId}/versions")
    public DagDefinitionVersionEntity saveDraft(@PathVariable Long definitionId,
                                                 @RequestBody DagDefinition definition) {
        return definitionService.saveDraft(definitionId, definition);
    }

    @PostMapping("/versions/{versionId}/publish")
    public DagDefinitionVersionEntity publish(@PathVariable Long versionId) {
        return definitionService.publish(versionId);
    }

    @PostMapping("/{definitionId}/publish-latest")
    public ResponseEntity<DagDefinitionVersionEntity> publishLatest(@PathVariable Long definitionId) {
        return definitionService.findLatestPublished(definitionId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
