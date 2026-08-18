package com.staterelay.starter.execution;

import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.starter.registration.WorkerIdentityProvider;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DispatchDeduplicator {

    private static final int LOCK_STRIPES = 64;

    private final LocalDispatchStore store;
    private final Semaphore admissionPermits;
    private final Object[] dispatchLocks = new Object[LOCK_STRIPES];

    public DispatchDeduplicator(LocalDispatchStore store, int admissionCapacity) {
        this.store = Objects.requireNonNull(store, "store");
        if (admissionCapacity <= 0) {
            throw new IllegalArgumentException("admission capacity must be positive");
        }
        admissionPermits = new Semaphore(admissionCapacity);
        for (int index = 0; index < dispatchLocks.length; index++) {
            dispatchLocks[index] = new Object();
        }
    }

    public Admission tryAdmit(
            ExecuteTaskCommand command,
            WorkerIdentityProvider.WorkerIdentity identity,
            Instant acceptedAt) {
        Object lock = dispatchLocks[Math.floorMod(command.dispatchId().hashCode(), LOCK_STRIPES)];
        synchronized (lock) {
            Optional<LocalDispatchStore.DispatchRecord> existing =
                    store.find(command.dispatchId());
            if (existing.isPresent()) {
                requireSameLogicalDispatch(existing.get(), command, identity);
                return Admission.duplicate();
            }
            if (!admissionPermits.tryAcquire()) {
                return Admission.capacityRejected();
            }
            boolean created = false;
            try {
                created = store.createAccepted(command, identity, acceptedAt);
                if (!created) {
                    requireSameLogicalDispatch(store.require(command.dispatchId()), command,
                            identity);
                    return Admission.duplicate();
                }
                return Admission.accepted();
            } finally {
                if (!created) {
                    admissionPermits.release();
                }
            }
        }
    }

    public void rollback(Admission admission, String dispatchId) {
        if (admission.release()) {
            try {
                store.remove(dispatchId);
            } finally {
                admissionPermits.release();
            }
        }
    }

    public void complete(Admission admission) {
        if (admission.release()) {
            admissionPermits.release();
        }
    }

    private static void requireSameLogicalDispatch(
            LocalDispatchStore.DispatchRecord record,
            ExecuteTaskCommand command,
            WorkerIdentityProvider.WorkerIdentity identity) {
        if (!record.taskInstanceId().equals(command.taskInstanceId())
                || !record.attemptId().equals(command.attemptId())
                || record.leaseVersion() != command.leaseVersion()
                || !record.workerId().equals(identity.workerId().toString())
                || !record.workerEpoch().equals(identity.workerEpoch().toString())) {
            throw new IllegalStateException(
                    "dispatchId is already bound to another attempt: "
                            + command.dispatchId());
        }
    }

    public enum AdmissionStatus {
        ACCEPTED,
        DUPLICATE,
        REJECTED_CAPACITY
    }

    public static final class Admission {

        private final AdmissionStatus status;
        private final AtomicBoolean permitOwned;

        private Admission(AdmissionStatus status, boolean permitOwned) {
            this.status = status;
            this.permitOwned = new AtomicBoolean(permitOwned);
        }

        static Admission accepted() {
            return new Admission(AdmissionStatus.ACCEPTED, true);
        }

        static Admission duplicate() {
            return new Admission(AdmissionStatus.DUPLICATE, false);
        }

        static Admission capacityRejected() {
            return new Admission(AdmissionStatus.REJECTED_CAPACITY, false);
        }

        public AdmissionStatus status() {
            return status;
        }

        private boolean release() {
            return permitOwned.compareAndSet(true, false);
        }
    }
}
