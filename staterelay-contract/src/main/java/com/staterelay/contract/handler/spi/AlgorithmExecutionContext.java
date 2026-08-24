package com.staterelay.contract.handler.spi;

import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.handler.TaskContext;

/**
 * 算法执行上下文（对齐文档 §12、§14、§15）。
 *
 * <p>扩展 {@link TaskContext}，注入 Worker 侧运行时能力：
 * <ul>
 *   <li>{@link #executionContext()} —— 调度和围栏信息（dagInstanceId / nodeCode / attemptId / leaseVersion 等）</li>
 *   <li>{@link #workDirectory()} —— Attempt 独立工作目录（input / temp / output）</li>
 *   <li>{@link #artifactClient()} —— Artifact 下载 / 上传</li>
 *   <li>{@link #artifactMetadataClient()} —— Artifact 元数据查询与 STAGED 登记</li>
 * </ul>
 *
 * <p>{@link AlgorithmExecutor} 实现通过本上下文访问调度侧信息和 Worker 侧能力。
 * ExecutionCoordinator 在调用 {@link AlgorithmExecutor#execute} 时，
 * 总是提供 {@link AlgorithmExecutionContext} 实现（非普通 TaskContext）。
 *
 * <p>用法示例：
 * <pre>{@code
 * @Algorithm(code = "GDAL_INTERSECTION", contractVersion = "1.0")
 * public class GdalIntersectionExecutor implements AlgorithmExecutor<GdalIntersectionParams> {
 *     @Override
 *     public TaskResult<AlgorithmOutputs> execute(TaskContext context, GdalIntersectionParams params) {
 *         AlgorithmExecutionContext ctx = (AlgorithmExecutionContext) context;
 *         WorkDirectory workDir = ctx.workDirectory();
 *         ArtifactClient client = ctx.artifactClient();
 *         // 下载 sourceLayer 容器
 *         Path gdb = client.download(params.sourceLayer().getArtifactId(), workDir.inputDir().resolve("source.gdb.zip"));
 *         // ...执行算法...
 *         // 上传结果
 *         ArtifactStageRequest req = client.upload(workDir.outputDir().resolve("result.gdb"),
 *             new ArtifactStageRequest(ctx.executionContext().getDagInstanceId(),
 *                 ctx.executionContext().getNodeCode(), ctx.executionContext().getAttemptId(),
 *                 ctx.executionContext().getAttemptNo(), "resultLayer", VectorStorageType.OBJECT_STORAGE));
 *         ArtifactStageResponse resp = ctx.artifactMetadataClient().stage(req);
 *         // 构造返回
 *         VectorLayerRef resultLayerRef = VectorLayerRef.fileGdb(resp.getArtifactId(),
 *             "dag_50001_node_B_attempt_1_resultLayer", GeometryType.MULTIPOLYGON, 4490);
 *         return TaskResult.success(AlgorithmOutputs.single("resultLayer", resultLayerRef));
 *     }
 * }
 * }</pre>
 */
public interface AlgorithmExecutionContext extends TaskContext {

    /** Worker 执行上下文，包含调度和围栏信息 */
    WorkerExecutionContext executionContext();

    /** Attempt 独立工作目录 */
    WorkDirectory workDirectory();

    /** Artifact 数据传输客户端 */
    ArtifactClient artifactClient();

    /** Artifact 元数据查询客户端 */
    ArtifactMetadataClient artifactMetadataClient();
}
