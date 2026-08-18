package com.staterelay.starter.handler;

import com.staterelay.contract.handler.DistributedTask;
import com.staterelay.contract.handler.TaskHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;

public final class HandlerRegistry {

    private final Map<String, TaskHandler<?, ?>> handlers;
    private final Map<String, HandlerMetadata> metadata;
    private final Map<String, HandlerBinding> bindings;

    public HandlerRegistry(ListableBeanFactory beanFactory) {
        Map<String, TaskHandler> discovered = new TreeMap<>(
                beanFactory.getBeansOfType(TaskHandler.class));
        Map<String, TaskHandler<?, ?>> byHandlerName = new LinkedHashMap<>();
        Map<String, HandlerMetadata> discoveredMetadata = new LinkedHashMap<>();
        Map<String, HandlerBinding> discoveredBindings = new LinkedHashMap<>();
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
            java.lang.reflect.Type parameterType = ResolvableType
                    .forClass(handlerType)
                    .as(TaskHandler.class)
                    .getGeneric(0)
                    .resolve(Object.class);
            discoveredBindings.put(handlerName,
                    new HandlerBinding(handler, parameterType));
        });
        handlers = Collections.unmodifiableMap(byHandlerName);
        metadata = Collections.unmodifiableMap(discoveredMetadata);
        bindings = Collections.unmodifiableMap(discoveredBindings);
    }

    public TaskHandler<?, ?> require(String handlerName) {
        TaskHandler<?, ?> handler = handlers.get(handlerName);
        if (handler == null) {
            throw new NoSuchElementException("unknown handler: " + handlerName);
        }
        return handler;
    }

    public Optional<HandlerBinding> findBinding(String handlerName) {
        return Optional.ofNullable(bindings.get(handlerName));
    }

    public Map<String, HandlerMetadata> metadata() {
        return metadata;
    }

    public record HandlerMetadata(String name, String implementationType) {
    }

    @Data
    @AllArgsConstructor
    public static class HandlerBinding {
        private final TaskHandler<?, ?> handler;
        private final java.lang.reflect.Type parameterType;
    }
}
