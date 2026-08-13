package com.staterelay.contract.handler;

/**
 * Executes one typed distributed task invocation.
 *
 * <p>Implementations should make their business side effects idempotent with
 * {@link TaskContext#idempotencyKey()} because a dispatch may be retransmitted.</p>
 *
 * @param <P> task parameter type
 * @param <R> successful task result type
 */
public interface TaskHandler<P, R> {

    TaskResult<R> execute(TaskContext context, P parameter) throws Exception;
}
