package com.staterelay.starter.artifact;

/**
 * Artifact 下载失败异常（对齐文档 §15.1）。
 *
 * <p>由 {@link com.staterelay.contract.handler.spi.ArtifactClient#download} 抛出：
 * <ul>
 *   <li>网络临时异常可重试</li>
 *   <li>checksum 不一致且无法恢复时最终失败</li>
 * </ul>
 *
 * <p>调用方（AlgorithmExecutor）按文档 §19.1 处理：临时异常重试，
 * 不可恢复异常上抛导致 Attempt FAILED。
 */
public class ArtifactDownloadException extends RuntimeException {

    private final Long artifactId;

    public ArtifactDownloadException(Long artifactId, String message) {
        super(message);
        this.artifactId = artifactId;
    }

    public ArtifactDownloadException(Long artifactId, String message, Throwable cause) {
        super(message, cause);
        this.artifactId = artifactId;
    }

    public Long getArtifactId() {
        return artifactId;
    }
}
