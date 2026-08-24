package com.staterelay.starter.execution;

import com.staterelay.contract.handler.spi.WorkDirectory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * NodeAttempt 独立工作目录管理器（对齐 GIS-Worker 设计文档 §14.1 / §14.2）。
 *
 * <p>为每次 Attempt 创建隔离的工作目录：
 * <pre>
 *   {workBaseDirectory}/{dagInstanceId}/{nodeCode}/{attemptId}/
 *   ├── input/   ← 上游 Artifact 解压后存放
 *   ├── temp/    ← 算法执行临时数据
 *   └── output/  ← 本 Attempt 生成的结果
 * </pre>
 *
 * <p><b>隔离目标（§14.1）：</b>
 * <ul>
 *   <li>B、C 并行互不覆盖（不同 nodeCode 子目录）</li>
 *   <li>同一节点的不同 Attempt 互不覆盖（不同 attemptId 子目录）</li>
 *   <li>晚到 Attempt 不能修改新 Attempt 的文件</li>
 *   <li>失败后可以按 Attempt 精确清理</li>
 *   <li>方便追踪磁盘占用和故障现场</li>
 * </ul>
 *
 * <p><b>原始 GDB 只读（§14.2）：</b>
 * A 产生的源 GDB Artifact 作为只读输入，不允许 B、C 原地修改。
 * B、C 读取 source.gdb 后生成独立结果 GDB，写入各自 output 目录。
 */
@Component
public class WorkDirectoryManager {

    private static final Logger log = Logger.getLogger(WorkDirectoryManager.class.getName());

    private final Path workBaseDir;

    public WorkDirectoryManager(
            @Value("${staterelay.work-base-directory:./work}") String workBaseDirectory) {
        this.workBaseDir = Paths.get(workBaseDirectory).toAbsolutePath().normalize();
        log.info("WorkDirectoryManager initialized: base=" + workBaseDir);
    }

    /**
     * 为指定 Attempt 创建独立工作目录（§14.1）。
     *
     * <p>目录结构：{@code {base}/{dagInstanceId}/{nodeCode}/{attemptId}/{input|temp|output}}
     *
     * <p>幂等：若目录已存在则直接复用，不报错。
     *
     * @param dagInstanceId DAG 实例 ID
     * @param nodeCode      节点编码
     * @param attemptId     Attempt ID（字符串形式，作为物理命名层）
     * @return 工作目录接口
     */
    public WorkDirectory createWorkDirectory(Long dagInstanceId, String nodeCode, String attemptId) {
        Objects.requireNonNull(dagInstanceId, "dagInstanceId");
        Objects.requireNonNull(nodeCode, "nodeCode");
        Objects.requireNonNull(attemptId, "attemptId");

        Path root = workBaseDir
            .resolve(String.valueOf(dagInstanceId))
            .resolve(nodeCode)
            .resolve(attemptId);

        Path inputDir = root.resolve("input");
        Path tempDir = root.resolve("temp");
        Path outputDir = root.resolve("output");

        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(tempDir);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to create work directory: " + root + " — " + e.getMessage(), e);
        }

        log.fine("Work directory ready: dagInstanceId=" + dagInstanceId
            + ", nodeCode=" + nodeCode + ", attemptId=" + attemptId + ", root=" + root);

        return new DefaultWorkDirectory(root, inputDir, tempDir, outputDir,
            dagInstanceId, nodeCode, attemptId);
    }

    /**
     * 清理指定 Attempt 的工作目录（§19.4）。
     *
     * <p>Attempt 结束后按保留策略删除；失败现场可按需保留用于诊断。
     * 本方法做 best-effort 清理，不抛异常。
     */
    public void cleanupWorkDirectory(Long dagInstanceId, String nodeCode, String attemptId) {
        Path root = workBaseDir
            .resolve(String.valueOf(dagInstanceId))
            .resolve(nodeCode)
            .resolve(attemptId);
        try {
            deleteRecursively(root);
            log.info("Work directory cleaned: dagInstanceId=" + dagInstanceId
                + ", nodeCode=" + nodeCode + ", attemptId=" + attemptId);
        } catch (IOException e) {
            log.warning("Failed to clean work directory " + root + ": " + e.getMessage());
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        log.warning("Failed to delete " + p + ": " + e.getMessage());
                    }
                });
        }
    }

    /**
     * 默认 {@link WorkDirectory} 实现。
     */
    private static class DefaultWorkDirectory implements WorkDirectory {

        private final Path root;
        private final Path inputDir;
        private final Path tempDir;
        private final Path outputDir;
        private final Long dagInstanceId;
        private final String nodeCode;
        private final String attemptId;

        DefaultWorkDirectory(Path root, Path inputDir, Path tempDir, Path outputDir,
                              Long dagInstanceId, String nodeCode, String attemptId) {
            this.root = root;
            this.inputDir = inputDir;
            this.tempDir = tempDir;
            this.outputDir = outputDir;
            this.dagInstanceId = dagInstanceId;
            this.nodeCode = nodeCode;
            this.attemptId = attemptId;
        }

        @Override
        public Path root() { return root; }

        @Override
        public Path inputDir() { return inputDir; }

        @Override
        public Path tempDir() { return tempDir; }

        @Override
        public Path outputDir() { return outputDir; }

        @Override
        public Long dagInstanceId() { return dagInstanceId; }

        @Override
        public String nodeCode() { return nodeCode; }

        @Override
        public String attemptId() { return attemptId; }

        @Override
        public String toString() {
            return "WorkDirectory{root=" + root + ", dagInstanceId=" + dagInstanceId
                + ", nodeCode=" + nodeCode + ", attemptId=" + attemptId + "}";
        }
    }
}
