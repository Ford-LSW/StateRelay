package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 实例状态。
 *
 * <p>与 {@code sr_dag_instance.status} 列对应（存数值）。
 * 编码预留间隔，方便后续插入中间态。
 *
 * <pre>
 * 0   INIT       实例已建，NodeInstance 尚未全部创建
 * 10  RUNNING    NodeInstance 全部创建完，CAS 推进后正式运行
 * 20  SUCCESS    所有节点正常结束
 * 30  FAILED     存在不可恢复失败节点
 * 40  CANCELLED  用户取消
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum DagInstanceStatus implements CodedEnum {
    INIT(0),
    RUNNING(10),
    SUCCESS(20),
    FAILED(30),
    CANCELLED(40);

    private final int code;
}
