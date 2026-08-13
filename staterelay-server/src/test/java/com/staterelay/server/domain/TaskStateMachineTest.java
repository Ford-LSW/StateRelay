package com.staterelay.server.domain;

import org.junit.jupiter.api.Test;

import static com.staterelay.server.domain.TaskAttemptStatus.ACCEPTED;
import static com.staterelay.server.domain.TaskAttemptStatus.RUNNING;
import static com.staterelay.server.domain.TaskAttemptStatus.SUCCESS;
import static com.staterelay.server.domain.TaskAttemptStatus.TIMED_OUT;
import static com.staterelay.server.domain.TaskInstanceStatus.FAILED;
import static com.staterelay.server.domain.TaskInstanceStatus.RETRY_WAIT;
import static com.staterelay.server.domain.TaskInstanceStatus.WAITING;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskStateMachineTest {

    private final TaskInstanceStateMachine instanceStateMachine = new TaskInstanceStateMachine();
    private final TaskAttemptStateMachine attemptStateMachine = new TaskAttemptStateMachine();

    @Test
    void retryableAttemptFailureMovesInstanceToRetryWaitWithoutIntermediateFailedState() {
        assertThatNoException().isThrownBy(() ->
                instanceStateMachine.requireTransition(TaskInstanceStatus.RUNNING, RETRY_WAIT));
        assertThatThrownBy(() ->
                instanceStateMachine.requireTransition(FAILED, RETRY_WAIT))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void terminalAttemptCannotReturnToRunning() {
        assertThatThrownBy(() ->
                attemptStateMachine.requireTransition(SUCCESS, RUNNING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void onlyAcceptedAndRunningAttemptsCanTimeOutAndTimedOutAttemptIsTerminal() {
        assertThatNoException().isThrownBy(() ->
                attemptStateMachine.requireTransition(ACCEPTED, TIMED_OUT));
        assertThatNoException().isThrownBy(() ->
                attemptStateMachine.requireTransition(RUNNING, TIMED_OUT));
        assertThatThrownBy(() ->
                attemptStateMachine.requireTransition(TIMED_OUT, RUNNING))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() ->
                instanceStateMachine.requireTransition(WAITING, RETRY_WAIT))
                .isInstanceOf(IllegalStateException.class);
    }
}
