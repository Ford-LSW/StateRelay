package com.staterelay.server.dag.dispatch;

import com.staterelay.contract.protocol.DispatchAck;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Durable DAG dispatch boundary.
 *
 * <p>The JDBC implementation owns the short reservation and acknowledgement transactions;
 * HTTP is deliberately kept outside this interface so it can only run after commit.</p>
 */
public interface DagDispatchStore {

    /** Atomically reserves a matching {@code sr_worker} and creates one authoritative attempt. */
    Optional<Assignment> reserve(ReservationRequest request, Instant now);

    /** Claims one bounded transport send without changing the logical request identity. */
    Optional<SendClaim> claimSend(Assignment candidate, Instant now, Instant leaseExpiresAt);

    /** Records positive acceptance for the complete logical dispatch fence. */
    boolean markAccepted(SendClaim claim, Instant now);

    /** Records explicit non-acceptance and releases the reserved capacity exactly once. */
    boolean markRejected(
            SendClaim claim,
            DispatchAck.AckStatus status,
            String message,
            Instant now,
            Instant nextScheduleTime);

    /** Records an uncertain transport result owned by the current transport generation. */
    boolean markUncertain(SendClaim claim, String error, Instant nextTransportAt);

    /** Finds due uncertain sends; returned assignments retain their stable request and attempt IDs. */
    List<Assignment> findDueUncertain(Instant now, int batchSize);

    record ReservationRequest(
            Long dagInstanceId,
            Long nodeInstanceId,
            String nodeCode,
            String executionCode,
            boolean algorithmProtocol,
            String contractVersion,
            String contractChecksum,
            String executorGroupCode,
            String requestJson,
            String requestChecksum,
            Duration attemptLease,
            Duration executionTimeout) {
    }

    record Assignment(
            Long dagInstanceId,
            Long nodeInstanceId,
            String nodeCode,
            Long attemptId,
            int attemptNo,
            String requestId,
            String requestChecksum,
            long dispatchGeneration,
            String dispatchToken,
            long attemptLeaseVersion,
            String workerId,
            String workerEpoch,
            String workerAddress,
            String application,
            String executionCode,
            String requestJson,
            Duration leaseDuration,
            Instant dispatchedAt) {
    }

    record SendClaim(Assignment assignment, long transportGeneration) {
    }
}
