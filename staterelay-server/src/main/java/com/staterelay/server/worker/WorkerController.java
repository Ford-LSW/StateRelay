package com.staterelay.server.worker;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/internal/v1/workers")
public final class WorkerController {

    private final WorkerService service;

    public WorkerController(WorkerService service) {
        this.service = service;
    }

    /** Registers a concrete Worker process lifetime. */
    @PostMapping("/register")
    public WorkerService.WorkerRegistration register(
            @RequestBody WorkerService.RegistrationRequest request) {
        return service.register(request);
    }

    /** Reports liveness and renews all fully fenced active Attempt leases. */
    @PostMapping("/{workerId}/heartbeat")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void heartbeat(
            @PathVariable UUID workerId,
            @RequestBody WorkerService.HeartbeatRequest request) {
        service.heartbeat(workerId, request);
    }

    /** Removes a live Worker from new-task routing while allowing current work to finish. */
    @PostMapping("/{workerId}/drain")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void drain(
            @PathVariable UUID workerId,
            @RequestBody WorkerService.EpochRequest request) {
        service.drain(workerId, request);
    }

    /** Offlines a live Worker process without deleting its durable history. */
    @DeleteMapping("/{workerId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(
            @PathVariable UUID workerId,
            @RequestBody WorkerService.EpochRequest request) {
        service.delete(workerId, request);
    }
}
