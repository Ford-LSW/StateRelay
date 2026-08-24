package com.staterelay.starter.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link RequestIdStore} 的 MEMORY 实现（对齐文档 §44 PROCESS_LOCAL 策略）。
 *
 * <p>仅保证当前 Java 进程存活期间去重；Pod 重启后记录丢失。
 * 适用于测试环境或不要求跨进程恢复的场景。
 *
 * <p>所有 CAS 操作通过 {@link ConcurrentHashMap#compute} 保证原子性。
 *
 * <p>rebindFence 升级规则（对齐文档 §19.1 / §44 / §47.1）：
 * <ul>
 *   <li>仅当 {@code newFence.leaseVersion() > stored.leaseVersion()} 时升级</li>
 *   <li>requestId / requestChecksum / attemptId / workerId 完整围栏匹配</li>
 *   <li>状态 ∈ {RUNNING, SUCCESS, FAILED}</li>
 *   <li>升级后内部 leaseVersion / workerEpoch 同步更新为 newFence</li>
 * </ul>
 */
public class InMemoryRequestIdStore implements RequestIdStore, RequestIdStore.AtomicLifecycle {

    private final ConcurrentHashMap<String, RequestIdRecord> records = new ConcurrentHashMap<>();

    @Override
    public DedupCapability capability() {
        return DedupCapability.PROCESS_LOCAL;
    }

    @Override
    public boolean tryStart(String requestId, String requestChecksum, RequestFence fence, Instant now) {
        RequestIdRecord existing = records.get(requestId);
        if (existing != null) {
            // 重复 requestId：必须 fail-closed 校验 checksum 一致（§44 协议规则第 4 条）
            if (!existing.requestChecksum().equals(requestChecksum)) {
                throw new IllegalStateException(
                        "requestId=" + requestId + " checksum mismatch: stored=" + existing.requestChecksum()
                                + " incoming=" + requestChecksum);
            }
            return false;
        }
        RequestIdRecord record = new RequestIdRecord(
                requestId,
                requestChecksum,
                fence.attemptId(),
                fence.workerId(),
                fence.workerEpoch(),
                fence.leaseVersion(),
                RequestState.RUNNING,
                now,
                now,
                null,
                null,
                null,
                null);
        RequestIdRecord prev = records.putIfAbsent(requestId, record);
        return prev == null;
    }

    @Override
    public Optional<RequestIdRecord> find(String requestId) {
        return Optional.ofNullable(records.get(requestId));
    }

    @Override
    public void markSuccess(String requestId, String resultJson, String resultRef, Instant now) {
        records.computeIfPresent(requestId, (id, current) -> {
            if (current.state() != RequestState.RUNNING) {
                throw new IllegalStateException(
                        "cannot markSuccess on non-RUNNING record: " + id + " state=" + current.state());
            }
            return new RequestIdRecord(
                    current.requestId(),
                    current.requestChecksum(),
                    current.attemptId(),
                    current.workerId(),
                    current.workerEpoch(),
                    current.leaseVersion(),
                    RequestState.SUCCESS,
                    current.createdAt(),
                    now,
                    resultJson,
                    resultRef,
                    null,
                    null);
        });
    }

    @Override
    public void markFailed(String requestId, String errorCode, String errorMessage, Instant now) {
        records.computeIfPresent(requestId, (id, current) -> {
            if (current.state() != RequestState.RUNNING) {
                throw new IllegalStateException(
                        "cannot markFailed on non-RUNNING record: " + id + " state=" + current.state());
            }
            return new RequestIdRecord(
                    current.requestId(),
                    current.requestChecksum(),
                    current.attemptId(),
                    current.workerId(),
                    current.workerEpoch(),
                    current.leaseVersion(),
                    RequestState.FAILED,
                    current.createdAt(),
                    now,
                    null,
                    null,
                    errorCode,
                    errorMessage);
        });
    }

    @Override
    public boolean rebindFence(String requestId, RequestFence newFence, Instant now) {
        return rebindAtomically(requestId, null, newFence, now);
    }

    @Override
    public boolean compareAndRebind(
            String requestId,
            String requestChecksum,
            RequestFence newFence,
            Instant now) {
        return rebindAtomically(requestId, requestChecksum, newFence, now);
    }

    private boolean rebindAtomically(
            String requestId,
            String requestChecksum,
            RequestFence newFence,
            Instant now) {
        // compute 保证原子性（§44：跨 Scheduler 并发升级只允许一个成功）
        // 注意：ConcurrentHashMap.computeIfPresent 返回 null 会删除 entry，所以不升级时必须返回 current
        AtomicBoolean upgraded = new AtomicBoolean(false);
        records.computeIfPresent(requestId, (id, current) -> {
            if (requestChecksum != null
                    && !current.requestChecksum().equals(requestChecksum)) {
                throw new IllegalArgumentException(
                        "rebindFence checksum mismatch: stored=" + current.requestChecksum()
                                + " incoming=" + requestChecksum);
            }
            // 1. 围栏完整性校验（attemptId / workerId 必须匹配）
            if (!current.attemptId().equals(newFence.attemptId())) {
                throw new IllegalArgumentException(
                        "rebindFence attemptId mismatch: stored=" + current.attemptId()
                                + " incoming=" + newFence.attemptId());
            }
            if (!current.workerId().equals(newFence.workerId())) {
                throw new IllegalArgumentException(
                        "rebindFence workerId mismatch: stored=" + current.workerId()
                                + " incoming=" + newFence.workerId());
            }
            // 2. 单调递增校验：仅当 newLeaseVersion > storedLeaseVersion 才升级
            //    防止旧 Scheduler 在网络分区恢复后反向覆盖已升级的版本
            if (newFence.leaseVersion() <= current.leaseVersion()) {
                // 不升级：返回原记录（不能返回 null，否则会删除 entry）
                return current;
            }
            // 3. 状态校验：仅 RUNNING / SUCCESS / FAILED 允许升级
            //    CREATED / 不存在状态不允许（终态也要允许升级，§47 边界场景）
            if (current.state() == RequestState.RUNNING
                    || current.state() == RequestState.SUCCESS
                    || current.state() == RequestState.FAILED) {
                upgraded.set(true);
                return new RequestIdRecord(
                        current.requestId(),
                        current.requestChecksum(),
                        current.attemptId(),
                        newFence.workerId(),
                        newFence.workerEpoch(),
                        newFence.leaseVersion(),
                        current.state(),
                        current.createdAt(),
                        now,
                        current.resultJson(),
                        current.resultRef(),
                        current.errorCode(),
                        current.errorMessage());
            }
            // 状态不合法，不升级：返回原记录
            return current;
        });
        return upgraded.get();
    }

    @Override
    public boolean compareAndAbort(
            String requestId, String requestChecksum, RequestFence fence) {
        AtomicBoolean aborted = new AtomicBoolean(false);
        records.computeIfPresent(requestId, (id, current) -> {
            if (current.state() == RequestState.RUNNING
                    && Objects.equals(current.requestChecksum(), requestChecksum)
                    && Objects.equals(current.attemptId(), fence.attemptId())
                    && Objects.equals(current.workerId(), fence.workerId())
                    && Objects.equals(current.workerEpoch(), fence.workerEpoch())
                    && Objects.equals(current.leaseVersion(), fence.leaseVersion())) {
                aborted.set(true);
                return null;
            }
            return current;
        });
        return aborted.get();
    }

    @Override
    public int cleanup(Duration retention, Instant now) {
        if (retention.isNegative()) {
            throw new IllegalArgumentException("terminal retention must not be negative");
        }
        AtomicInteger removed = new AtomicInteger(0);
        records.forEach((id, record) -> {
            // 只清理已确认终态（SUCCESS / FAILED）且 retention 已到期的记录
            // RUNNING 状态不能直接删除（§44：过期但仍为 RUNNING 的记录不能直接删除后重新执行）
            if ((record.state() == RequestState.SUCCESS || record.state() == RequestState.FAILED)
                    && record.updatedAt().plus(retention).isBefore(now)) {
                if (records.remove(id, record)) {
                    removed.incrementAndGet();
                }
            }
        });
        return removed.get();
    }

    /**
     * 仅供测试：获取当前所有记录数。
     */
    int sizeForTesting() {
        return records.size();
    }
}
