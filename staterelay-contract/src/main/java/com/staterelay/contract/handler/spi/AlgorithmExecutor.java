package com.staterelay.contract.handler.spi;

import com.staterelay.contract.dag.algorithm.AlgorithmOutputs;
import com.staterelay.contract.handler.DistributedTask;
import com.staterelay.contract.handler.TaskContext;
import com.staterelay.contract.handler.TaskHandler;
import com.staterelay.contract.handler.TaskResult;

/**
 * 算法执行器 SPI（对齐文档 §2、§3.2、§12）。
 *
 * <p>每个 GIS 算法（如 {@code GIS_INPUT} / {@code GDAL_INTERSECTION} /
 * {@code GDAL_ERASE_WITH_ORIGINAL} / {@code PACKAGE_GDB}）实现一个
 * {@link AlgorithmExecutor}，注册为 Worker 侧 Handler。
 *
 * <p><b>与 {@link TaskHandler} 的关系：</b>
 * 本接口继承 {@link TaskHandler<P, AlgorithmOutputs>}，复用 {@link DistributedTask}
 * 注解、{@code HandlerRegistry} 注册和 {@code ExecutionCoordinator} 调度链路，
 * 仅扩展以下能力：
 * <ul>
 *   <li>固定结果类型为 {@link AlgorithmOutputs}（多输出 Map）</li>
 *   <li>暴露 {@link #algorithmCode()} / {@link #contractVersion()} / {@link #contractChecksum()}
 *       供 Worker 心跳上报，调度中心校验契约一致性（文档 §3.2）</li>
 * </ul>
 *
 * <p><b>注册约定：</b>
 * 实现类必须同时标注 {@link DistributedTask} 注解，且 {@link DistributedTask#value()}
 * 必须等于 {@link #algorithmCode()}，使 HandlerRegistry 的 handlerName 即为 algorithmCode。
 * <pre>{@code
 * @DistributedTask("GDAL_INTERSECTION")
 * public class GdalIntersectionExecutor
 *         implements AlgorithmExecutor<GdalIntersectionParams> {
 *     @Override public String algorithmCode() { return "GDAL_INTERSECTION"; }
 *     @Override public String contractVersion() { return "1.0"; }
 *     @Override public String contractChecksum() { return "sha256:xxx"; }
 *
 *     @Override
 *     public TaskResult<AlgorithmOutputs> execute(TaskContext context, GdalIntersectionParams params) {
 *         AlgorithmExecutionContext ctx = (AlgorithmExecutionContext) context;
 *         // 使用 ctx.workDirectory() / ctx.artifactClient() / ctx.artifactMetadataClient()
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <p><b>契约一致性校验（文档 §3.2）：</b>
 * Worker 心跳上报 {@code algorithmCode + contractVersion + contractChecksum}，
 * 调度中心与已发布的 {@code AlgorithmDefinition} 校验。
 * 不一致时：
 * <pre>
 *   该 Worker 不得接收该算法的新任务
 *   + 记录 ALGORITHM_CONTRACT_MISMATCH 告警
 * </pre>
 *
 * @param <P> 算法参数类型，由 ParameterResolver 解析后的 JSON 反序列化得到
 */
public interface AlgorithmExecutor<P> extends TaskHandler<P, AlgorithmOutputs> {

    /** 算法编码，例如 {@code GDAL_INTERSECTION}，与 {@link DistributedTask#value()} 一致 */
    String algorithmCode();

    /** 契约版本号，例如 {@code 1.0}，Worker 心跳上报供调度中心校验 */
    String contractVersion();

    /**
     * 契约 checksum，例如 {@code sha256:xxx}，由 contractVersion + inputs + outputs
     * 序列化后哈希得到。可空，未实现时可返回 null（校验降级为版本号匹配）。
     */
    default String contractChecksum() {
        return null;
    }

    /**
     * 实现版本号，标识本 Worker 实现的具体版本。可空。
     */
    default String implementationVersion() {
        return null;
    }

    /**
     * 参数类型，供 ExecutionCoordinator 反序列化 dispatch JSON。
     * 默认实现返回 {@link Object}，子类可重写返回具体参数类以获得强类型校验。
     */
    default Class<P> parameterType() {
        @SuppressWarnings("unchecked")
        Class<P> type = (Class<P>) Object.class;
        return type;
    }
}
