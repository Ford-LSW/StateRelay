package com.staterelay.contract.handler;

public interface TaskHandler<P, R> {

    TaskResult<R> execute(TaskContext context, P parameter) throws Exception;
}
