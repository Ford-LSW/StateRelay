package com.staterelay.server.dag.dispatch;

import com.staterelay.contract.protocol.DispatchAck;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DAG 调度持久化边界。
 *
 * <p>JDBC 实现负责短时的预留和确认事务；HTTP 调用不属于该接口，
 * 从而确保只能在事务提交后执行。</p>
 */
public interface DagDispatchStore {

    /** 原子预留匹配的 {@code sr_worker}，并创建唯一有效的执行尝试。 */
    Optional<Assignment> reserve(ReservationRequest request, Instant now);

    /** 获取一次有界传输发送权，不改变逻辑请求标识。 */
    Optional<SendClaim> claimSend(Assignment candidate, Instant now, Instant leaseExpiresAt);

    /** 按完整逻辑调度围栏记录接受结果。 */
    boolean markAccepted(SendClaim claim, Instant now);

    /** 记录明确拒绝，并确保预留容量仅释放一次。 */
    boolean markRejected(
            SendClaim claim,
            DispatchAck.AckStatus status,
            String message,
            Instant now,
            Instant nextScheduleTime);

    /** 记录归属于当前传输代次的不确定传输结果。 */
    boolean markUncertain(SendClaim claim, String error, Instant nextTransportAt);

    /** 查询到期的不确定发送；返回的分配保持稳定的请求与尝试标识。 */
    List<Assignment> findDueUncertain(Instant now, int batchSize);

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class ReservationRequest {
        private Long dagInstanceId;
        private Long nodeInstanceId;
        private String nodeCode;
        private String executionCode;
        private boolean algorithmProtocol;
        private String contractVersion;
        private String contractChecksum;
        private String executorGroupCode;
        private String requestJson;
        private String requestChecksum;
        private Duration attemptLease;
        private Duration executionTimeout;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class Assignment {
        private Long dagInstanceId;
        private Long nodeInstanceId;
        private String nodeCode;
        private Long attemptId;
        private int attemptNo;
        private String requestId;
        private String requestChecksum;
        private long dispatchGeneration;
        private String dispatchToken;
        private long attemptLeaseVersion;
        private String workerId;
        private String workerEpoch;
        private String workerAddress;
        private String application;
        private String executionCode;
        private String requestJson;
        private Duration leaseDuration;
        private Instant dispatchedAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class SendClaim {
        private Assignment assignment;
        private long transportGeneration;
    }
}
