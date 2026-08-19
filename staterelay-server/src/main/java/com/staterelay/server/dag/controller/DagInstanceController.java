package com.staterelay.server.dag.controller;

import com.staterelay.contract.dag.StartDagInstanceRequest;
import com.staterelay.server.dag.entity.DagInstanceEntity;
import com.staterelay.server.dag.entity.NodeInstanceEntity;
import com.staterelay.server.dag.service.DagInstanceService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * DAG 实例 Controller。
 */
@RestController
@RequestMapping("/api/v1/dag/instances")
@Validated
public class DagInstanceController {

    private final DagInstanceService instanceService;

    public DagInstanceController(DagInstanceService instanceService) {
        this.instanceService = instanceService;
    }

    @PostMapping("/start")
    public DagInstanceEntity start(@RequestParam Long appId,
                                   @RequestBody StartDagInstanceRequest request) {
        return instanceService.startInstance(appId, request);
    }

    @GetMapping("/{instanceId}")
    public DagInstanceEntity getInstance(@PathVariable Long instanceId) {
        return instanceService.findInstance(instanceId)
            .orElseThrow(() -> new IllegalArgumentException("DAG instance not found: " + instanceId));
    }

    @GetMapping("/{instanceId}/nodes")
    public List<NodeInstanceEntity> listNodes(@PathVariable Long instanceId) {
        return instanceService.listNodes(instanceId);
    }

    @PostMapping("/{instanceId}/cancel")
    public int cancel(@PathVariable Long instanceId) {
        return instanceService.cancelInstance(instanceId);
    }
}
