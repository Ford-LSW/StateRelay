package com.staterelay.starter;

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
