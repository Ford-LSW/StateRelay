package com.staterelay.contract.dag.enums;

/**
 * 通用"带数值编码"枚举接口，供 JPA AttributeConverter 与 MyBatis TypeHandler 共用。
 *
 * <p>所有 DAG 状态/类型枚举实现该接口，使数据库存储统一使用 {@code int} 编码。
 */
public interface CodedEnum {
    int getCode();
}
