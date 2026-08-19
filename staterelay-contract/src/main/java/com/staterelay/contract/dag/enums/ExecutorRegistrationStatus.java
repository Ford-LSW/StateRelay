package com.staterelay.contract.dag.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Worker 注册状态。
 *
 * <p>与 {@code sr_executor_registration.status} 列对应（存数值）。
 *
 * <pre>
 * 0   OFFLINE   离线
 * 10  ONLINE    在线，可接任务
 * 20  DRAINING   排水中（不再接新任务，等存量任务完成）
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum ExecutorRegistrationStatus implements CodedEnum {
    OFFLINE(0),
    ONLINE(10),
    DRAINING(20);

    private final int code;
}
