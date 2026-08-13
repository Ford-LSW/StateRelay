package com.staterelay.starter.handler;

import com.staterelay.contract.handler.DistributedTask;
import com.staterelay.contract.handler.TaskHandler;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public final class HandlerRegistry {

    private final Map<String, TaskHandler<?, ?>> handlers;
    private final Map<String, HandlerMetadata> metadata;

    public HandlerRegistry(ListableBeanFactory beanFactory) {
        Map<String, TaskHandler> discovered = new TreeMap<>(
                beanFactory.getBeansOfType(TaskHandler.class));
        Map<String, TaskHandler<?, ?>> byHandlerName = new LinkedHashMap<>();
        Map<String, HandlerMetadata> discoveredMetadata = new LinkedHashMap<>();
        discovered.forEach((beanName, handler) -> {
            Class<?> handlerType = AopUtils.getTargetClass(handler);
            DistributedTask annotation = AnnotatedElementUtils.findMergedAnnotation(
                    handlerType, DistributedTask.class);
            if (annotation == null) {
                throw new IllegalStateException(
                        "TaskHandler bean is missing @DistributedTask: " + beanName);
            }
            String handlerName = annotation.value().trim();
            if (handlerName.isEmpty()) {
                throw new IllegalStateException("handler name must not be blank: " + beanName);
            }
            if (byHandlerName.putIfAbsent(handlerName, handler) != null) {
                throw new IllegalStateException("duplicate handler name: " + handlerName);
            }
            discoveredMetadata.put(handlerName,
                    new HandlerMetadata(handlerName, handlerType.getName()));
        });
        handlers = Collections.unmodifiableMap(byHandlerName);
        metadata = Collections.unmodifiableMap(discoveredMetadata);
    }

    public TaskHandler<?, ?> require(String handlerName) {
        TaskHandler<?, ?> handler = handlers.get(handlerName);
        if (handler == null) {
            throw new NoSuchElementException("unknown handler: " + handlerName);
        }
        return handler;
    }

    public Map<String, HandlerMetadata> metadata() {
        return metadata;
    }

    public record HandlerMetadata(String name, String implementationType) {
    }
}
