package com.staterelay.starter;

import com.staterelay.starter.execution.InMemoryRequestIdStore;
import com.staterelay.starter.execution.RequestIdStore;
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
}
