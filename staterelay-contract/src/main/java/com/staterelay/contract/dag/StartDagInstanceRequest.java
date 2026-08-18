package com.staterelay.contract.dag;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 启动 DAG 实例请求。
 */
@Data
@NoArgsConstructor
public class StartDagInstanceRequest {

    /** DAG 业务编码 */
    private String dagCode;
    /** DAG 定义版本号，可空，为空时取最新 PUBLISHED 版本 */
    private Integer version;
    /** DAG 级输入参数，会注入到 {@code ${dag.inputs.xxx}} 表达式 */
    private Map<String, Object> inputs;
    /** 业务 ID，便于业务侧反查 */
    private String businessId;
    /** 幂等键，相同 key 多次提交只创建一个 DAG 实例 */
    private String idempotencyKey;

    public StartDagInstanceRequest(String dagCode, Integer version, Map<String, Object> inputs,
                                   String businessId, String idempotencyKey) {
        java.util.Objects.requireNonNull(dagCode, "dagCode");
        java.util.Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        this.dagCode = dagCode;
        this.version = version;
        this.inputs = inputs == null ? Map.of() : inputs;
        this.businessId = businessId;
        this.idempotencyKey = idempotencyKey;
    }
}
