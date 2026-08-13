package com.staterelay.server.trigger;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/task-definitions")
public final class ManualTriggerController {

    private final TriggerService triggerService;

    public ManualTriggerController(TriggerService triggerService) {
        this.triggerService = triggerService;
    }

    /**
     * Creates an immediately-ready instance, optionally deduplicated by a caller business key.
     */
    @PostMapping("/{id}/trigger")
    public TriggerResponse trigger(
            @PathVariable("id") UUID definitionId, @RequestBody TriggerRequest request) {
        return new TriggerResponse(triggerService.triggerNow(
                definitionId, request.idempotencyKey(), request.payload()));
    }

    public record TriggerRequest(String idempotencyKey, JsonNode payload) {
    }

    public record TriggerResponse(UUID instanceId) {
    }
}
