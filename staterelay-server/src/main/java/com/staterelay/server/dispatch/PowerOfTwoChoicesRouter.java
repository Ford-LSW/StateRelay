package com.staterelay.server.dispatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

public final class PowerOfTwoChoicesRouter implements WorkerRouter {

    private static final Comparator<WorkerCandidate> PREFERENCE =
            Comparator.comparingInt(WorkerCandidate::effectiveLoad)
                    .thenComparing(WorkerCandidate::workerId);

    private final Random random;

    public PowerOfTwoChoicesRouter(Random random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public Optional<WorkerCandidate> choose(
            List<WorkerCandidate> candidates, Requirements requirements) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(requirements, "requirements");
        List<WorkerCandidate> eligible = new ArrayList<>();
        for (WorkerCandidate candidate : candidates) {
            if (candidate.isEligible(requirements)) {
                eligible.add(candidate);
            }
        }
        if (eligible.isEmpty()) {
            return Optional.empty();
        }
        if (eligible.size() == 1) {
            return Optional.of(eligible.get(0));
        }
        int firstIndex = random.nextInt(eligible.size());
        int secondIndex = random.nextInt(eligible.size() - 1);
        if (secondIndex >= firstIndex) {
            secondIndex++;
        }
        return Optional.of(PREFERENCE.compare(
                eligible.get(firstIndex), eligible.get(secondIndex)) <= 0
                ? eligible.get(firstIndex) : eligible.get(secondIndex));
    }
}
