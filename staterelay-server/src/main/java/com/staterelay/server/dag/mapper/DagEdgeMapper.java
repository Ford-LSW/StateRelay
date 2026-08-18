package com.staterelay.server.dag.mapper;

import com.staterelay.contract.dag.enums.DagEdgeType;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * DAG 边 MyBatis Mapper，处理依赖检查等聚合查询。
 *
 * <p>对应 XML：{@code resources/mapper/DagEdgeMapper.xml}
 */
@Mapper
public interface DagEdgeMapper {

    /**
     * 检查指定节点的所有前置节点是否都 SUCCESS。
     *
     * @return 0 表示所有前置已 SUCCESS（可运行）；>0 表示仍有未完成的前置
     */
    int countUnsatisfiedPredecessors(@Param("dagInstanceId") Long dagInstanceId,
                                     @Param("dagDefinitionVersionId") Long dagDefinitionVersionId,
                                     @Param("toNodeId") String toNodeId);

    /**
     * 查询指定节点的所有直接后继 nodeId（用于推进后唤醒）。
     */
    List<String> findSuccessorNodeIds(@Param("dagDefinitionVersionId") Long dagDefinitionVersionId,
                                      @Param("fromNodeId") String fromNodeId);

    /**
     * 查询指定节点的所有直接前驱 nodeId。
     */
    List<String> findPredecessorNodeIds(@Param("dagDefinitionVersionId") Long dagDefinitionVersionId,
                                        @Param("toNodeId") String toNodeId);

    /**
     * 查询入度为 0 的节点（无前驱节点）。
     */
    List<String> findRootNodeIds(@Param("dagDefinitionVersionId") Long dagDefinitionVersionId);

    /**
     * 批量插入边。
     */
    int batchInsertEdges(@Param("dagDefinitionVersionId") Long dagDefinitionVersionId,
                         @Param("edges") List<EdgeInsert> edges);

    /** 边插入参数。edgeType 为 {@link DagEdgeType}，写入 DB 时存其 code。 */
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    static class EdgeInsert {
        private String from;
        private String to;
        private DagEdgeType edgeType;
        private String condition;
    }
}
