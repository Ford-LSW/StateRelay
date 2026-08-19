package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 节点实例状态。
 *
 * <p>与 {@code sr_dag_node_instance.status} 列对应（存数值）。
 * 编码预留间隔，方便后续插入中间态。
 *
 * <pre>
 * 0   WAITING       等待前驱完成
 * 10  READY         前驱全部 SUCCESS，可被 Scheduler 领取
 * 20  DISPATCHING   Scheduler CAS 抢占中
 * 30  DISPATCHED    NodeAttempt 已创建，远程调用已发出
 * 40  RUNNING       Worker 已 ACK，正在执行
 * 50  SUCCESS       执行成功（终态）
 * 60  FAILED        执行失败且超过重试上限（终态）
 * 70  CANCELLED     用户取消（终态）
 * 80  SKIPPED       前驱失败后链式跳过（终态）
 * 90  TIMEOUT       Scheduler 超时扫描命中（非终态，立即推进到 READY 或 FAILED）
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum NodeInstanceStatus implements CodedEnum {
    WAITING(0),
    READY(10),
    DISPATCHING(20),
    DISPATCHED(30),
    RUNNING(40),
    SUCCESS(50),
    FAILED(60),
    CANCELLED(70),
    SKIPPED(80),
    TIMEOUT(90);

    private final int code;
}
