package com.staterelay.server.dag.converter;

import com.staterelay.contract.dag.enums.CodedEnum;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JPA AttributeConverter 抽象基类：枚举 ↔ 数据库 Integer 互转。
 *
 * <p>所有 DAG 状态/类型字段统一存 int 编码（性能与存储友好），业务代码全用枚举。
 * 子类只需在构造中传入枚举 Class，无需重复实现任何逻辑。
 *
 * @param <E> 实现 {@link CodedEnum} 的枚举类型
 */
public abstract class AbstractCodedEnumConverter<E extends Enum<E> & CodedEnum>
        implements jakarta.persistence.AttributeConverter<E, Integer> {

    private final Map<Integer, E> codeToEnum;

    protected AbstractCodedEnumConverter(Class<E> enumClass) {
        this.codeToEnum = Arrays.stream(enumClass.getEnumConstants())
                .collect(Collectors.toUnmodifiableMap(CodedEnum::getCode, Function.identity()));
    }

    @Override
    public Integer convertToDatabaseColumn(E attribute) {
        return attribute == null ? null : attribute.getCode();
    }

    @Override
    public E convertToEntityAttribute(Integer dbData) {
        if (dbData == null) {
            return null;
        }
        E result = codeToEnum.get(dbData);
        if (result == null) {
            throw new IllegalArgumentException("Unknown code " + dbData
                    + " for enum " + codeToEnum.values().getClass().getComponentType().getSimpleName());
        }
        return result;
    }
}
