package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 节点执行尝试状态。
 *
 * <p>与 {@code sr_dag_node_attempt.status} 列对应（存数值）。
 * 编码预留间隔。
 *
 * <pre>
 * 0   CREATED       Attempt 已创建，尚未派发
 * 10  DISPATCHING   正在派发中
 * 20  ACCEPTED      Worker 已确认接收
 * 30  RUNNING       Worker 正在执行
 * 40  SUCCESS       执行成功（终态）
 * 50  FAILED        执行失败（终态）
 * 60  CANCELLED     被取消（终态）
 * 70  TIMEOUT       Scheduler 超时扫描命中（终态）
 * 80  UNKNOWN       网络超时无法确认结果（终态，避免误重试）
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum NodeAttemptStatus implements CodedEnum {
    CREATED(0),
    DISPATCHING(10),
    ACCEPTED(20),
    RUNNING(30),
    SUCCESS(40),
    FAILED(50),
    CANCELLED(60),
    TIMEOUT(70),
    UNKNOWN(80);

    private final int code;
}
