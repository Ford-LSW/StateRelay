package com.staterelay.server.definition;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public final class TaskDefinitionController {

    private final TaskDefinitionService service;

    public TaskDefinitionController(TaskDefinitionService service) {
        this.service = service;
    }

    /**
     * Creates a definition together with its first draft version.
     */
    @PostMapping("/task-definitions")
    public TaskDefinitionService.DefinitionVersion create(
            @RequestBody CreateDefinitionRequest request) {
        return service.createDraft(request.applicationId(), request.name(), request.handlerName(),
                request.configuration(), request.parameterSchema());
    }

    /**
     * Creates a new draft version while leaving every earlier version unchanged.
     */
    @PostMapping("/task-definitions/{id}/versions")
    public TaskDefinitionService.DefinitionVersion createRevision(
            @PathVariable("id") UUID definitionId,
            @RequestBody VersionRequest request) {
        return service.createRevision(definitionId, request.handlerName(),
                request.configuration(), request.parameterSchema());
    }

    /**
     * Publishes one draft version and switches the definition's current version pointer.
     */
    @PostMapping("/task-definition-versions/{versionId}/publish")
    public TaskDefinitionService.DefinitionVersion publish(
            @PathVariable UUID versionId) {
        return service.publish(versionId);
    }

    public record CreateDefinitionRequest(
            UUID applicationId,
            String name,
            String handlerName,
            JsonNode configuration,
            JsonNode parameterSchema) {
    }

    public record VersionRequest(
            String handlerName, JsonNode configuration, JsonNode parameterSchema) {
    }
}
