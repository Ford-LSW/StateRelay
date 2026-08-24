package com.staterelay.starter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.handler.spi.ArtifactClient;
import com.staterelay.contract.handler.spi.ArtifactMetadataClient;
import com.staterelay.starter.artifact.RestClientArtifactClient;
import com.staterelay.starter.artifact.RestClientArtifactMetadataClient;
import com.staterelay.starter.execution.ExecutionCoordinator;
import com.staterelay.starter.execution.ExecutorController;
import com.staterelay.starter.execution.InMemoryRequestIdStore;
import com.staterelay.starter.execution.LocalDispatchStore;
import com.staterelay.starter.execution.RequestIdStore;
import com.staterelay.starter.execution.ResultReporter;
import com.staterelay.starter.execution.WorkDirectoryManager;
import com.staterelay.starter.handler.HandlerRegistry;
import com.staterelay.starter.registration.WorkerIdentityProvider;
import com.staterelay.starter.registration.WorkerRegistrationClient;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.client.RestClient;

import java.nio.file.Paths;
import java.time.Clock;

@AutoConfiguration
@EnableConfigurationProperties(StateRelayProperties.class)
public class StateRelayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    HandlerRegistry stateRelayHandlerRegistry(ListableBeanFactory beanFactory) {
        return new HandlerRegistry(beanFactory);
    }

    /**
     * Worker 端 requestId 去重存储 SPI（对齐文档 §44）。
     *
     * <p>默认提供 {@link InMemoryRequestIdStore}（PROCESS_LOCAL 策略），
     * 业务可自定义 DURABLE 实现（JDBC/Redis/本地文件等）覆盖此 Bean。
     */
    @Bean
    @ConditionalOnMissingBean
    RequestIdStore stateRelayRequestIdStore() {
        return new InMemoryRequestIdStore();
    }

    @Bean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    WorkerIdentityProvider stateRelayWorkerIdentityProvider(StateRelayProperties properties) {
        return new WorkerIdentityProvider(properties);
    }

    @Bean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    WorkerRegistrationClient stateRelayWorkerRegistrationClient(
            StateRelayProperties properties,
            HandlerRegistry handlers,
            WorkerIdentityProvider identityProvider,
            RestClient.Builder restClientBuilder,
            TaskScheduler stateRelayTaskScheduler) {
        return new WorkerRegistrationClient(
                properties, handlers, identityProvider, restClientBuilder, stateRelayTaskScheduler);
    }

    @Bean
    @ConditionalOnMissingBean(name = "stateRelayTaskScheduler")
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ThreadPoolTaskScheduler stateRelayTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("staterelay-heartbeat-");
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    RestClient.Builder stateRelayRestClientBuilder() {
        return RestClient.builder();
    }

    /**
     * Worker 端共享 RestClient（baseUrl 指向调度中心，对齐 GIS-Worker 设计文档 §15.1）。
     *
     * <p>Artifact 元数据查询 / 数据下载 / 数据上传均通过本 RestClient 调用调度中心端点。
     * 业务可自定义 RestClient 覆盖本 Bean（如添加拦截器、超时、鉴权等）。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    RestClient stateRelayWorkerRestClient(
            StateRelayProperties properties,
            RestClient.Builder restClientBuilder) {
        return restClientBuilder.baseUrl(properties.getServerUrl().toString()).build();
    }

    /**
     * Worker 侧 {@link ArtifactMetadataClient} SPI 实现（对齐文档 §15.1）。
     *
     * <p>通过 {@code GET /internal/v1/artifacts/{artifactId}/metadata} 查询元数据，
     * 通过 {@code POST /internal/v1/artifacts} 登记 STAGED Artifact。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ArtifactMetadataClient stateRelayArtifactMetadataClient(RestClient stateRelayWorkerRestClient) {
        return new RestClientArtifactMetadataClient(stateRelayWorkerRestClient);
    }

    /**
     * Worker 侧 {@link ArtifactClient} SPI 实现（对齐文档 §15.1）。
     *
     * <p>通过 {@code GET /internal/v1/artifacts/{artifactId}/data} 下载数据流，
     * 通过 {@code POST /internal/v1/artifacts/data} 上传 multipart 数据流。
     * 下载后校验 checksum 与元数据一致。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ArtifactClient stateRelayArtifactClient(
            RestClient stateRelayWorkerRestClient,
            ArtifactMetadataClient artifactMetadataClient) {
        return new RestClientArtifactClient(stateRelayWorkerRestClient, artifactMetadataClient);
    }

    /**
     * Worker 端工作目录管理器（对齐 GIS-Worker 设计文档 §14.1）。
     *
     * <p>为每次 Attempt 创建隔离的工作目录（input / temp / output），
     * 供 {@link com.staterelay.contract.handler.spi.AlgorithmExecutor} 使用。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    WorkDirectoryManager stateRelayWorkDirectoryManager(StateRelayProperties properties) {
        return new WorkDirectoryManager(properties.getWorkBaseDirectory());
    }

    /**
     * Worker 端本地派发存储（对齐文档 §44）。
     *
     * <p>持久化 dispatch 状态和 terminal 结果，支持重发和去重。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    LocalDispatchStore stateRelayLocalDispatchStore(
            StateRelayProperties properties,
            ObjectMapper objectMapper) {
        return new LocalDispatchStore(
                Paths.get(properties.getWorkBaseDirectory()), objectMapper);
    }

    /**
     * Worker 端结果回报器（对齐文档 §43 / §45）。
     *
     * <p>通过 HTTP 向调度中心上报进度和终态结果，支持重试和去重。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ResultReporter stateRelayResultReporter(
            LocalDispatchStore store,
            RestClient.Builder restClientBuilder,
            StateRelayProperties properties,
            TaskScheduler stateRelayTaskScheduler) {
        ResultReporter reporter = new ResultReporter(
                store,
                new ResultReporter.RestClientTransport(
                        restClientBuilder, properties.getServerUrl()),
                Clock.systemUTC(),
                java.time.Duration.ofSeconds(5),
                java.time.Duration.ofSeconds(1),
                java.time.Duration.ofSeconds(10),
                java.time.Duration.ofMinutes(5));
        reporter.start(stateRelayTaskScheduler);
        return reporter;
    }

    /**
     * Worker 端执行协调器（对齐文档 §12 / §14 / §15）。
     *
     * <p>接收 {@link com.staterelay.contract.protocol.ExecuteTaskCommand}，
     * 按 handlerName 查找 {@link com.staterelay.contract.handler.TaskHandler} 或
     * {@link com.staterelay.contract.handler.spi.AlgorithmExecutor} 执行。
     *
     * <p>当 handler 是 AlgorithmExecutor 且 command 携带 executionContext 时，
     * 自动注入 WorkDirectory + ArtifactClient + ArtifactMetadataClient（§12）。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ExecutionCoordinator stateRelayExecutionCoordinator(
            StateRelayProperties properties,
            WorkerIdentityProvider identityProvider,
            HandlerRegistry handlers,
            ObjectMapper objectMapper,
            LocalDispatchStore store,
            ResultReporter reporter,
            WorkDirectoryManager workDirectoryManager,
            ArtifactClient artifactClient,
            ArtifactMetadataClient artifactMetadataClient,
            WorkerRegistrationClient registrationClient) {
        ExecutionCoordinator coordinator = new ExecutionCoordinator(
                properties, identityProvider, handlers, objectMapper, store, reporter,
                new WorkerActivityListener(registrationClient),
                Clock.systemUTC(),
                workDirectoryManager, artifactClient, artifactMetadataClient);
        return coordinator;
    }

    /**
     * Worker 端执行端点控制器（接收调度侧 HTTP 派发，§43）。
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "staterelay", name = "enabled", havingValue = "true")
    ExecutorController stateRelayExecutorController(ExecutionCoordinator coordinator) {
        return new ExecutorController(coordinator);
    }

    /**
     * 桥接 {@link WorkerRegistrationClient} 作为 {@link ExecutionCoordinator.ActivityListener}。
     *
     * <p>将 Executor 的活跃统计上报到 RegistrationClient，
     * 由心跳回报到调度中心。
     */
    static final class WorkerActivityListener implements ExecutionCoordinator.ActivityListener {

        private final WorkerRegistrationClient registrationClient;

        WorkerActivityListener(WorkerRegistrationClient registrationClient) {
            this.registrationClient = registrationClient;
        }

        @Override
        public void report(
                int activeCount,
                int queueDepth,
                java.util.List<WorkerRegistrationClient.ExecutionLease> activeLeases) {
            registrationClient.reportActivity(activeCount, queueDepth, activeLeases);
        }
    }
}
