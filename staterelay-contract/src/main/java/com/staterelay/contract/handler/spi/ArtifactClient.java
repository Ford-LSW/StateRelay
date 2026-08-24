package com.staterelay.contract.handler.spi;

import com.staterelay.contract.dag.artifact.ArtifactStageRequest;

import java.nio.file.Path;

/**
 * Worker 侧 Artifact 数据传输 SPI（对齐文档 §15.1）。
 *
 * <p>负责把对象存储的 GDB zip 下载到 Attempt input 目录、
 * 把 output 目录中的结果压缩包上传到对象存储。
 *
 * <p><b>第一版正确性流程（文档 §15.1）：</b>
 * <pre>
 *   Worker 根据 artifactId 通过 ArtifactMetadataClient 查询元数据
 *       ↓
 *   取得 objectKey 和 checksum
 *       ↓
 *   调用 {@link #download} 下载到本地 input 目录
 *       ↓
 *   校验 checksum
 *       ↓
 *   解压
 *       ↓
 *   Handler 读取指定 layerName 并执行
 *       ↓
 *   结果写入 output 目录
 *       ↓
 *   压缩并通过 {@link #upload} 上传结果 GDB
 *       ↓
 *   调用 ArtifactMetadataClient.stage 登记 STAGED artifact
 * </pre>
 *
 * <p><b>并发约束（文档 §14.2）：</b>
 * 原始 GDB Artifact 只读，禁止 B、C 并发原地修改。
 * 推荐每个节点生成独立结果 GDB，最终由 PACKAGE_GDB 节点组装。
 */
public interface ArtifactClient {

    /**
     * 下载指定 Artifact 到本地路径。
     *
     * <p>实现需：
     * <ul>
     *   <li>从对象存储按 objectKey 下载</li>
     *   <li>校验 checksum（与 {@code ArtifactMetadataResponse.checksum} 比对）</li>
     *   <li>返回本地文件路径</li>
     * </ul>
     *
     * <p>下载失败抛 {@code ArtifactDownloadException}，由调用方按 §19.1 处理：
     * 网络临时异常可重试，checksum 不一致且无法恢复时最终失败。
     *
     * @param artifactId 容器 Artifact ID
     * @param targetPath 目标本地路径（通常位于 {@link WorkDirectory#inputDir()}）
     * @return 下载后的本地文件路径
     */
    Path download(Long artifactId, Path targetPath);

    /**
     * 上传本地结果文件到对象存储。
     *
     * <p>实现需：
     * <ul>
     *   <li>计算 checksum</li>
     *   <li>压缩结果目录（GDB 场景）</li>
     *   <li>上传到对象存储</li>
     *   <li>填充 {@link ArtifactStageRequest#getObjectKey()} 与 {@link ArtifactStageRequest#getChecksum()}</li>
     * </ul>
     *
     * @param localPath 本地结果文件 / 目录路径（通常位于 {@link WorkDirectory#outputDir()}）
     * @param request   Stage 请求，实现填充 objectKey 和 checksum 后回传
     * @return 填充了 objectKey + checksum 的请求对象
     */
    ArtifactStageRequest upload(Path localPath, ArtifactStageRequest request);
}
