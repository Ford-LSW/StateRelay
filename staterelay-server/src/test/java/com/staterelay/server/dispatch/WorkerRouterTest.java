package com.staterelay.server.dispatch;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerRouterTest {

    private static final Instant NOW = Instant.parse("2026-08-13T12:00:00Z");
    private final WorkerRouter router = new PowerOfTwoChoicesRouter(new Random(7));
    private final WorkerRouter.Requirements requirements =
            new WorkerRouter.Requirements("archiveOrders", NOW);

    @Test
    void routerNeverChoosesDrainingExpiredIncompatibleOrFullWorker() {
        List<WorkerRouter.WorkerCandidate> candidates = List.of(
                worker("a", "READY", NOW.plusSeconds(30), 3, 0, 16, compatible()),
                worker("b", "DRAINING", NOW.plusSeconds(30), 0, 0, 16, compatible()),
                worker("c", "READY", NOW.plusSeconds(30), 0, 0, 16, Set.of("other")),
                worker("d", "READY", NOW.plusSeconds(30), 16, 0, 16, compatible()),
                worker("e", "READY", NOW, 0, 0, 16, compatible()));

        assertThat(router.choose(candidates, requirements)).get()
                .extracting(WorkerRouter.WorkerCandidate::workerId).isEqualTo("a");
    }

    @Test
    void effectiveLoadUsesTheGreaterOfReservedAndReportedActiveCount() {
        var reservedBehindReport = worker(
                "high", "READY", NOW.plusSeconds(30), 1, 9, 16, compatible());
        var reservedAheadOfReport = worker(
                "low", "READY", NOW.plusSeconds(30), 4, 0, 16, compatible());

        assertThat(reservedBehindReport.effectiveLoad()).isEqualTo(9);
        assertThat(router.choose(List.of(reservedBehindReport, reservedAheadOfReport), requirements))
                .contains(reservedAheadOfReport);
    }

    @Test
    void seededPowerOfTwoChoiceIsDeterministicAndChoosesLowerEffectiveLoad() {
        List<WorkerRouter.WorkerCandidate> candidates = List.of(
                worker("a", "READY", NOW.plusSeconds(30), 8, 0, 16, compatible()),
                worker("b", "READY", NOW.plusSeconds(30), 2, 0, 16, compatible()),
                worker("c", "READY", NOW.plusSeconds(30), 5, 0, 16, compatible()));

        WorkerRouter first = new PowerOfTwoChoicesRouter(new Random(91));
        WorkerRouter second = new PowerOfTwoChoicesRouter(new Random(91));

        assertThat(first.choose(candidates, requirements))
                .isEqualTo(second.choose(candidates, requirements));
    }

    private WorkerRouter.WorkerCandidate worker(
            String workerId,
            String status,
            Instant leaseExpiresAt,
            int reserved,
            int reported,
            int maximum,
            Set<String> handlers) {
        return new WorkerRouter.WorkerCandidate(
                workerId, status, leaseExpiresAt, reserved, reported, maximum, handlers);
    }

    private Set<String> compatible() {
        return Set.of("archiveOrders");
    }
}
