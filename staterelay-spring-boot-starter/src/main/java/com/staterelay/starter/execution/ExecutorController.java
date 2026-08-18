package com.staterelay.starter.execution;

import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@RequestMapping("/staterelay/internal/v1")
public final class ExecutorController {

    private final ExecutionCoordinator coordinator;

    public ExecutorController(ExecutionCoordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @PostMapping(value = "/executions", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public DispatchAck execute(@RequestBody ExecuteTaskCommand command) {
        return coordinator.execute(command);
    }
}
