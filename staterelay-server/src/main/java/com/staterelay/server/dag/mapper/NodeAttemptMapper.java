package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.enums.NodeAttemptStatus;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * DAG 节点执行尝试 MyBatis Mapper。
 *
 * <p>对应 XML：{@code resources/mapper/NodeAttemptMapper.xml}
 *
 * <p>状态机对齐文档：
 * <ul>
 *   <li>CREATED(0) → DISPATCHING(10) → ACCEPTED(20) → RUNNING(30) → SUCCESS(40)/FAILED(50)/TIMEOUT(70)/UNKNOWN(80)</li>
 * </ul>
 */
@Mapper
public interface NodeAttemptMapper {

    /**
     * 插入新的 NodeAttempt 记录。
     */
    int insert(@Param("attempt") NodeAttemptInsert attempt, @Param("now") Instant now);

    /**
     * CAS: CREATED(0) → DISPATCHING(10)。
     */
    int markDispatching(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: DISPATCHING(10) → ACCEPTED(20)（Worker 确认接收）。
     */
    int markAccepted(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: ACCEPTED(20) → RUNNING(30)（Worker 开始执行）。
     */
    int markRunning(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → SUCCESS(40)。
     */
    int markSuccess(@Param("attemptId") Long attemptId,
                    @Param("resultJson") String resultJson,
                    @Param("resultRef") String resultRef,
                    @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → FAILED(50)。
     */
    int markFailed(@Param("attemptId") Long attemptId,
                   @Param("errorCode") String errorCode,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    /**
     * CAS: RUNNING(30) → TIMEOUT(70)（Scheduler 超时扫描）。
     */
    int markTimeout(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * CAS: → UNKNOWN(80)（网络超时无法确认结果）。
     */
    int markUnknown(@Param("attemptId") Long attemptId, @Param("now") Instant now);

    /**
     * 扫描 RUNNING(30) 且超时的 Attempt（Scheduler 超时检测）。
     */
    List<Long> scanTimeoutAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 扫描终态但未处理的 Attempt（DAG Engine 推进 NodeInstance）。
     */
    List<Long> scanTerminalAttempts(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * 用于插入的参数。
     */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class NodeAttemptInsert {
        private Long dagInstanceId;
        private Long nodeInstanceId;
        private Integer attemptNo;
        private String requestId;
        private String algorithmCode;
        private String workerId;
        private String workerAddress;
        private String requestJson;
    }
}
