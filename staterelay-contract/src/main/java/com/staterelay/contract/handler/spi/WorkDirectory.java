package com.staterelay.contract.handler.spi;

import java.nio.file.Path;

/**
 * NodeAttempt 独立工作目录（对齐文档 §14.1）。
 *
 * <p>每次 Attempt 使用独立工作目录，目录结构如下：
 * <pre>
 *   /work/{dagInstanceId}/{nodeCode}/{attemptId}/
 *   ├── input/   ← 上游 Artifact 解压后存放
 *   ├── temp/    ← 算法执行临时数据
 *   └── output/  ← 本 Attempt 生成的结果
 * </pre>
 *
 * <p>隔离目标：
 * <ul>
 *   <li>B、C 并行互不覆盖（文档 §14.2）</li>
 *   <li>同一节点的不同 Attempt 互不覆盖</li>
 *   <li>晚到 Attempt 不能修改新 Attempt 的文件</li>
 *   <li>失败后可以按 Attempt 精确清理</li>
 *   <li>方便追踪磁盘占用和故障现场</li>
 * </ul>
 *
 * <p>Worker 通过 {@link AlgorithmExecutionContext#workDirectory()} 访问。
 */
public interface WorkDirectory {

    /** Attempt 工作目录根（即 {@code /work/{dagInstanceId}/{nodeCode}/{attemptId}/}） */
    Path root();

    /** 输入目录，上游 Artifact 解压后存放（{@code root/input}） */
    Path inputDir();

    /** 临时目录，算法执行临时数据（{@code root/temp}） */
    Path tempDir();

    /** 输出目录，本 Attempt 生成的结果（{@code root/output}） */
    Path outputDir();

    /** 当前 DagInstance ID */
    Long dagInstanceId();

    /** 当前节点编码 */
    String nodeCode();

    /** 当前 Attempt ID */
    String attemptId();
}
