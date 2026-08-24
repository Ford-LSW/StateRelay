package com.staterelay.contract.dag.artifact;

import com.staterelay.contract.dag.enums.CodedEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 空间 Artifact 状态机（对齐文档 §16.1）。
 *
 * <p>定义空间成果的生命周期，与调度侧 Attempt 围栏解耦：
 * <pre>
 *   CREATING  ← Worker 正在生成或上传
 *      ↓
 *   STAGED    ← 文件已生成并登记，但当前 Attempt 结果尚未被 Scheduler 确认
 *      ↓
 *   AVAILABLE ← 当前权威 Attempt 已被接受，可被下游读取
 *      ↓
 *   ORPHANED  ← 旧 Attempt / 晚到 Attempt / 失败 Attempt 产生的非权威成果
 *      ↓
 *   DELETING  ← 清理程序标记
 *      ↓
 *   DELETED   ← 物理删除
 * </pre>
 *
 * <p><b>关键不变式（文档 §16.2、§16.3）：</b>
 * <ul>
 *   <li>Worker 不能直接把 STAGED 提升为 AVAILABLE</li>
 *   <li>只有通过当前 Attempt 完整围栏的结果才能使 Artifact → AVAILABLE</li>
 *   <li>后继节点只能读取 {@link #AVAILABLE} 状态的 Artifact</li>
 *   <li>ORPHANED Artifact 由 Artifact Cleanup Scanner 定期回收</li>
 * </ul>
 *
 * <p><b>典型场景（文档 §16.3）：</b>
 * <pre>
 *   B Attempt 1 TIMEOUT
 *       ↓
 *   B Attempt 2 已经创建
 *       ↓
 *   Attempt 1 晚到 SUCCESS
 *       ↓
 *   Attempt 1 生成的文件物理存在，但已无权覆盖当前 NodeInstance 输出
 *       → Artifact 进入 ORPHANED，由清理程序回收
 * </pre>
 */
@Getter
@AllArgsConstructor
public enum SpatialArtifactState implements CodedEnum {
    /** Worker 正在生成或上传 */
    CREATING(10),
    /** 文件已生成并登记，但当前 Attempt 结果尚未被 Scheduler 确认 */
    STAGED(20),
    /** 当前权威 Attempt 已被接受，可被下游读取 */
    AVAILABLE(30),
    /** 旧 Attempt / 晚到 Attempt / 失败 Attempt 产生的非权威成果 */
    ORPHANED(40),
    /** 清理程序标记待删除 */
    DELETING(50),
    /** 物理删除完成 */
    DELETED(60);

    /** 数值编码，用于存储 */
    private final int code;

    /**
     * 是否可被下游节点读取（仅 {@link #AVAILABLE} 满足）。
     */
    public boolean isReadable() {
        return this == AVAILABLE;
    }

    /**
     * 是否为终态（不会再回到活跃态）。
     */
    public boolean isTerminal() {
        return this == DELETED;
    }
}
