package com.staterelay.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;

@ConfigurationProperties("staterelay")
public final class StateRelayProperties {

    private boolean enabled;
    private URI serverUrl;
    private String appName;
    private int executorPort = 8080;
    private int maxConcurrency = 16;
    private int queueCapacity = 64;
    private Duration heartbeatInterval = Duration.ofSeconds(10);
    private Duration workerLease = Duration.ofSeconds(35);
    private Environment environment = Environment.LOCAL;
    private String host = "127.0.0.1";
    private String podName;

    /** Worker 工作目录根路径（GIS-Worker 设计文档 §14.1） */
    private String workBaseDirectory = "./work";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public URI getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(URI serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public int getExecutorPort() {
        return executorPort;
    }

    public void setExecutorPort(int executorPort) {
        this.executorPort = executorPort;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getWorkerLease() {
        return workerLease;
    }

    public void setWorkerLease(Duration workerLease) {
        this.workerLease = workerLease;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public String getWorkBaseDirectory() {
        return workBaseDirectory;
    }

    public void setWorkBaseDirectory(String workBaseDirectory) {
        this.workBaseDirectory = workBaseDirectory;
    }

    public enum Environment {
        LOCAL,
        KUBERNETES
    }
}
