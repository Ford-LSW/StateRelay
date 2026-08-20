package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DAG 实例状态。
 *
 * <p>与 {@code sr_dag_instance.status} 列对应（存数值）。
 * 编码预留间隔，方便后续插入中间态。
 *
 * <p>对齐文档 §33.1 / §37.2：
 * <pre>
 * 0   INIT        实例已建，NodeInstance 尚未全部创建
 * 10  RUNNING     NodeInstance 全部创建完，CAS 推进后正式运行
 * 15  CANCELLING  用户取消中间态：已 CAS RUNNING→CANCELLING，等待 RUNNING 节点收敛
 * 18  FAILING     失败中间态：发现不可恢复失败节点，等待清理后 → FAILED
 * 20  SUCCESS     所有节点正常结束
 * 30  FAILED      存在不可恢复失败节点（终态）
 * 40  CANCELLED   用户取消完成（终态）
 * </pre>
 *
 * <p>终态：{@link #SUCCESS}、{@link #FAILED}、{@link #CANCELLED}。
 * 中间态：{@link #CANCELLING}、{@link #FAILING}。
 * 非终态非中间态：{@link #INIT}、{@link #RUNNING}。
 */
@Getter
@AllArgsConstructor
public enum DagInstanceStatus implements CodedEnum {
    INIT(0),
    RUNNING(10),
    CANCELLING(15),
    FAILING(18),
    SUCCESS(20),
    FAILED(30),
    CANCELLED(40);

    private final int code;

    /**
     * 是否终态（SUCCESS / FAILED / CANCELLED）。
     */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }

    /**
     * 是否活跃态（scanner 需要持续扫描推进：RUNNING / CANCELLING / FAILING）。
     */
    public boolean isActive() {
        return this == RUNNING || this == CANCELLING || this == FAILING;
    }
}
