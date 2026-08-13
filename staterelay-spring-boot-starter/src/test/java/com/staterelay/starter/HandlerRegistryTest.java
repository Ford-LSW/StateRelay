package com.staterelay.starter;

import com.staterelay.contract.handler.DistributedTask;
import com.staterelay.contract.handler.TaskContext;
import com.staterelay.contract.handler.TaskHandler;
import com.staterelay.contract.handler.TaskResult;
import com.staterelay.starter.handler.HandlerRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class HandlerRegistryTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(StateRelayAutoConfiguration.class));

    @Test
    void duplicateHandlerNamesFailApplicationStartup() {
        contextRunner.withBean("first", TaskHandler.class, FirstHandler::new)
                .withBean("second", TaskHandler.class, SecondHandler::new)
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining(
                                "duplicate handler name: closeExpiredOrders"));
    }

    @Test
    void requireReturnsTheDiscoveredAnnotatedHandler() {
        contextRunner.withBean("first", TaskHandler.class, FirstHandler::new)
                .run(context -> assertThat(context).hasNotFailed()
                        .getBean(HandlerRegistry.class).satisfies(registry ->
                                assertThat(registry.require("closeExpiredOrders"))
                                        .isInstanceOf(FirstHandler.class)));
    }

    @Test
    void propertiesBindTheDocumentedWorkerConfiguration() {
        contextRunner.withPropertyValues(
                        "staterelay.enabled=true",
                        "staterelay.server-url=http://staterelay-server:8080",
                        "staterelay.app-name=order-service",
                        "staterelay.executor-port=9080",
                        "staterelay.max-concurrency=16",
                        "staterelay.queue-capacity=64",
                        "staterelay.heartbeat-interval=10s",
                        "staterelay.worker-lease=35s")
                .withPropertyValues("staterelay.enabled=false")
                .run(context -> assertThat(context).hasNotFailed()
                        .getBean(StateRelayProperties.class).satisfies(properties -> {
                            assertThat(properties.isEnabled()).isFalse();
                            assertThat(properties.getServerUrl())
                                    .isEqualTo(URI.create("http://staterelay-server:8080"));
                            assertThat(properties.getAppName()).isEqualTo("order-service");
                            assertThat(properties.getExecutorPort()).isEqualTo(9080);
                            assertThat(properties.getMaxConcurrency()).isEqualTo(16);
                            assertThat(properties.getQueueCapacity()).isEqualTo(64);
                            assertThat(properties.getHeartbeatInterval())
                                    .isEqualTo(Duration.ofSeconds(10));
                            assertThat(properties.getWorkerLease())
                                    .isEqualTo(Duration.ofSeconds(35));
                        }));
    }

    @DistributedTask("closeExpiredOrders")
    static final class FirstHandler implements TaskHandler<Object, Object> {
        @Override
        public TaskResult<Object> execute(TaskContext context, Object parameter) {
            return TaskResult.success(parameter);
        }
    }

    @DistributedTask("closeExpiredOrders")
    static final class SecondHandler implements TaskHandler<Object, Object> {
        @Override
        public TaskResult<Object> execute(TaskContext context, Object parameter) {
            return TaskResult.success(parameter);
        }
    }
}
