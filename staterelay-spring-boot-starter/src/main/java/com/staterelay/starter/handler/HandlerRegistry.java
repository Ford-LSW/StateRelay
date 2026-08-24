package com.staterelay.starter.handler;

import com.staterelay.contract.handler.DistributedTask;
import com.staterelay.contract.handler.TaskHandler;
import com.staterelay.contract.handler.spi.AlgorithmExecutor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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

/**
 * Worker 侧 Handler 注册表（对齐文档 §2、§3.2、§12）。
 *
 * <p>扫描所有 {@link TaskHandler} Bean（通过 {@link DistributedTask} 注解注册），
 * 同时识别 {@link AlgorithmExecutor} 子类型并暴露算法契约元数据
 * （{@code algorithmCode} / {@code contractVersion} / {@code contractChecksum}），
 * 供 Worker 心跳上报、调度中心校验契约一致性（文档 §3.2）。
 */
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

            // 识别 AlgorithmExecutor 并提取契约元数据（§3.2）
            AlgorithmContract contract = extractAlgorithmContract(handler, handlerType, handlerName);

            discoveredMetadata.put(handlerName,
                    new HandlerMetadata(handlerName, handlerType.getName(), contract));

            // 优先使用 AlgorithmExecutor.parameterType()，退化到反射解析（§12）
            java.lang.reflect.Type parameterType = resolveParameterType(handler, handlerType);
            discoveredBindings.put(handlerName,
                    new HandlerBinding(handler, parameterType, contract));
        });
        handlers = Collections.unmodifiableMap(byHandlerName);
        metadata = Collections.unmodifiableMap(discoveredMetadata);
        bindings = Collections.unmodifiableMap(discoveredBindings);
    }

    /**
     * 若 handler 是 {@link AlgorithmExecutor}，校验 algorithmCode 与 @DistributedTask 一致，
     * 并提取契约元数据（§3.2）。
     */
    private AlgorithmContract extractAlgorithmContract(
            TaskHandler<?, ?> handler, Class<?> handlerType, String handlerName) {
        if (!AlgorithmExecutor.class.isInstance(handler)) {
            return null;
        }
        AlgorithmExecutor<?> executor = (AlgorithmExecutor<?>) handler;
        String algorithmCode = executor.algorithmCode();
        // 校验 @DistributedTask.value() == algorithmCode()（§2 注册约定）
        if (!handlerName.equals(algorithmCode)) {
            throw new IllegalStateException(
                    "AlgorithmExecutor algorithmCode mismatch: @DistributedTask=\"" + handlerName
                            + "\" but algorithmCode()=\"" + algorithmCode
                            + "\" for type " + handlerType.getName());
        }
        return new AlgorithmContract(
                algorithmCode,
                executor.contractVersion(),
                executor.contractChecksum(),
                executor.implementationVersion());
    }

    /**
     * 解析参数类型：AlgorithmExecutor 优先用 parameterType()，否则用反射（§12）。
     */
    private java.lang.reflect.Type resolveParameterType(
            TaskHandler<?, ?> handler, Class<?> handlerType) {
        if (handler instanceof AlgorithmExecutor<?> executor) {
            Class<?> declared = executor.parameterType();
            if (declared != null && declared != Object.class) {
                return declared;
            }
        }
        return ResolvableType
                .forClass(handlerType)
                .as(TaskHandler.class)
                .getGeneric(0)
                .resolve(Object.class);
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

    /**
     * Handler 元数据，包含算法契约信息（当 handler 是 {@link AlgorithmExecutor} 时）。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HandlerMetadata {
        private String name;
        private String implementationType;
        private AlgorithmContract algorithmContract;

        /**
         * 向后兼容的构造器（无算法契约）。
         */
        public HandlerMetadata(String name, String implementationType) {
            this(name, implementationType, null);
        }
    }

    /**
     * 算法契约元数据（§3.2），用于 Worker 心跳上报供调度中心校验。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AlgorithmContract {
        private String algorithmCode;
        private String contractVersion;
        private String contractChecksum;
        private String implementationVersion;
    }

    @Data
    @AllArgsConstructor
    public static class HandlerBinding {
        private final TaskHandler<?, ?> handler;
        private final java.lang.reflect.Type parameterType;
        private final AlgorithmContract algorithmContract;

        /**
         * 向后兼容的构造器（无算法契约）。
         */
        public HandlerBinding(TaskHandler<?, ?> handler, java.lang.reflect.Type parameterType) {
            this(handler, parameterType, null);
        }

        /**
         * 是否为算法执行器（§12）。
         */
        public boolean isAlgorithmExecutor() {
            return algorithmContract != null;
        }
    }
}
