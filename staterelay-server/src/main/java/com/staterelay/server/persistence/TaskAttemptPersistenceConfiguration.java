package com.staterelay.server.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** 普通任务 Attempt 持久化组件的 Spring 装配。 */
@Configuration(proxyBeanMethods = false)
public class TaskAttemptPersistenceConfiguration {

    /** 创建供调度与普通任务上报共同使用的 Attempt 仓储。 */
    @Bean
    public TaskAttemptRepository taskAttemptRepository(
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactions,
            OutboxRepository outbox,
            ObjectMapper objectMapper) {
        return new TaskAttemptRepository(jdbc, transactions, outbox, objectMapper);
    }
}
