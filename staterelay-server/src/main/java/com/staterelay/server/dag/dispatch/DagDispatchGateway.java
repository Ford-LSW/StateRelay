package com.staterelay.server.dag.dispatch;

import java.time.Instant;

/** Application-facing boundary for durable DAG assignment and delivery. */
public interface DagDispatchGateway {

    /** Reserves a Worker and delivers the command after the reservation transaction commits. */
    DagDispatchCoordinator.Outcome dispatch(
            DagDispatchStore.ReservationRequest request, Instant now);

    /** Retransmits due uncertain commands with their existing logical request identity. */
    int retryUncertain(Instant now, int batchSize);
}
