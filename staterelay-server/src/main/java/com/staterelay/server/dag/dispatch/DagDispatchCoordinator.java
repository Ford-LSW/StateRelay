package com.staterelay.server.dag.dispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.dag.algorithm.WorkerExecutionContext;
import com.staterelay.contract.protocol.DispatchAck;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.server.dag.worker.WorkerHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** 协调持久化 DAG 分配与事务提交后的 HTTP 投递。 */
@Service
public class DagDispatchCoordinator implements DagDispatchGateway {

    private final DagDispatchStore store;
    private final WorkerHttpClient workerHttpClient;
    private final ObjectMapper objectMapper;
    private final Duration transportRetryDelay;
    private final Duration sendLease;

    public DagDispatchCoordinator(
            DagDispatchStore store,
            WorkerHttpClient workerHttpClient,
            ObjectMapper objectMapper,
            @Value("${staterelay.dag.dispatch.transport-retry-delay:PT2S}") Duration transportRetryDelay,
            @Value("${staterelay.dag.dispatch.send-lease:PT5S}") Duration sendLease) {
        this.store = Objects.requireNonNull(store, "store");
        this.workerHttpClient = Objects.requireNonNull(workerHttpClient, "workerHttpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.transportRetryDelay = requirePositive(transportRetryDelay, "transportRetryDelay");
        this.sendLease = requirePositive(sendLease, "sendLease");
    }

    /** 在一个事务中完成预留并提交，随后才执行首次 HTTP 发送。 */
    @Override
    public Outcome dispatch(DagDispatchStore.ReservationRequest request, Instant now) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(now, "now");
        Optional<DagDispatchStore.Assignment> assignment = store.reserve(request, now);
        return assignment.map(value -> deliver(value, now)).orElse(Outcome.NO_WORKER);
    }

    /** 重发到期的不确定请求，不创建新的执行尝试。 */
    @Override
    public int retryUncertain(Instant now, int batchSize) {
        Objects.requireNonNull(now, "now");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        List<DagDispatchStore.Assignment> due = store.findDueUncertain(now, batchSize);
        int claimed = 0;
        for (DagDispatchStore.Assignment assignment : due) {
            Outcome outcome = deliver(assignment, now);
            if (outcome != Outcome.NOT_DUE) {
                claimed++;
            }
        }
        return claimed;
    }

    private Outcome deliver(DagDispatchStore.Assignment assignment, Instant now) {
        Optional<DagDispatchStore.SendClaim> claimed = store.claimSend(
                assignment, now, now.plus(sendLease));
        if (claimed.isEmpty()) {
            return Outcome.NOT_DUE;
        }
        DagDispatchStore.SendClaim send = claimed.get();
        try {
            DispatchAck ack = workerHttpClient.execute(
                    assignment.getWorkerAddress(), command(assignment));
            if (!matchesFence(assignment, ack)) {
                store.markUncertain(send, "mismatched or empty dispatch acknowledgement",
                        now.plus(transportRetryDelay));
                return Outcome.UNCERTAIN;
            }
            if (ack.status() == DispatchAck.AckStatus.ACCEPTED
                    || ack.status() == DispatchAck.AckStatus.DUPLICATE) {
                return store.markAccepted(send, now) ? Outcome.ACCEPTED : Outcome.UNCERTAIN;
            }
            return store.markRejected(send, ack.status(), ack.message(), now,
                    now.plus(transportRetryDelay)) ? Outcome.REJECTED : Outcome.UNCERTAIN;
        } catch (RuntimeException exception) {
            store.markUncertain(send, exception.toString(), now.plus(transportRetryDelay));
            return Outcome.UNCERTAIN;
        }
    }

    private ExecuteTaskCommand command(DagDispatchStore.Assignment assignment) {
        WorkerExecutionContext context = new WorkerExecutionContext();
        context.setDagInstanceId(assignment.getDagInstanceId());
        context.setNodeInstanceId(assignment.getNodeInstanceId());
        context.setNodeCode(assignment.getNodeCode());
        context.setAttemptId(assignment.getAttemptId().toString());
        context.setAttemptNo(assignment.getAttemptNo());
        context.setRequestId(assignment.getRequestId());
        context.setRequestChecksum(assignment.getRequestChecksum());
        context.setDispatchGeneration(assignment.getDispatchGeneration());
        context.setDispatchToken(assignment.getDispatchToken());
        context.setAttemptLeaseVersion(assignment.getAttemptLeaseVersion());
        context.setWorkerId(assignment.getWorkerId());
        context.setWorkerEpoch(assignment.getWorkerEpoch());
        return new ExecuteTaskCommand(
                assignment.getDagInstanceId().toString(), assignment.getAttemptId().toString(),
                assignment.getAttemptNo(), assignment.getRequestId(), assignment.getAttemptLeaseVersion(),
                assignment.getApplication(), assignment.getWorkerId(), assignment.getWorkerEpoch(),
                assignment.getExecutionCode(), read(assignment.getRequestJson()), assignment.getRequestId(),
                assignment.getLeaseDuration(), assignment.getDispatchedAt(), context);
    }

    private boolean matchesFence(DagDispatchStore.Assignment assignment, DispatchAck ack) {
        return ack != null
                && Objects.equals(assignment.getRequestId(), ack.dispatchId())
                && Objects.equals(assignment.getAttemptId().toString(), ack.attemptId())
                && Objects.equals(assignment.getWorkerId(), ack.workerId())
                && Objects.equals(assignment.getWorkerEpoch(), ack.workerEpoch());
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json == null ? "{}" : json);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    public enum Outcome {
        NO_WORKER,
        ACCEPTED,
        REJECTED,
        UNCERTAIN,
        NOT_DUE
    }
}
