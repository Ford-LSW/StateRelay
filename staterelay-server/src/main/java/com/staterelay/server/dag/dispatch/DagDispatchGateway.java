package com.staterelay.server.dag.dispatch;

import java.time.Instant;

/** 面向应用层的持久化 DAG 分配与投递边界。 */
public interface DagDispatchGateway {

    /** 预留 Worker，并在预留事务提交后投递命令。 */
    DagDispatchCoordinator.Outcome dispatch(
            DagDispatchStore.ReservationRequest request, Instant now);

    /** 使用原有逻辑请求标识重发到期的不确定命令。 */
    int retryUncertain(Instant now, int batchSize);
}
