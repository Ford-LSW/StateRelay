package com.staterelay.server.dag.orchestration;

/**
 * 参数解析失败异常（对齐文档 §17）。
 *
 * <p>ParameterResolver 在 DISPATCHING 阶段解析 inputBindings 时，若遇到：
 * <ul>
 *   <li>引用的上游 Artifact 缺失</li>
 *   <li>引用的 dag.input 字段不存在</li>
 *   <li>引用语法错误</li>
 * </ul>
 * 抛出本异常，由调度器捕获后：
 * <ol>
 *   <li>CAS NodeInstance DISPATCHING → FAILED（不重试，不计入 retry_count）</li>
 *   <li>同事务 +1 finished_node_count</li>
 *   <li>不调用 Worker</li>
 * </ol>
 *
 * <p>视为配置错误：重试也不会成功。
 */
public class InputResolveException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InputResolveException(String message) {
        super(message);
    }

    public InputResolveException(String message, Throwable cause) {
        super(message, cause);
    }
}
