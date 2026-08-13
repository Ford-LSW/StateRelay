# StateRelay 分布式任务调度平台设计说明

## 1. 文档信息

- 日期：2026-08-12
- 状态：设计已分段确认，待整体审核
- 首批用户：公司内部普通 Java 业务系统
- 部署环境：同一 Kubernetes 集群，兼容 Docker 内网部署
- 容量基线：约 50 个应用、日任务实例 100 万、峰值触发 100 次/秒

## 2. 产品定位

StateRelay 定位为面向 Java 业务系统的可靠分布式任务调度平台。它提供接近主流轻量调度中间件的接入体验和常用调度能力，同时将外部异步长任务的一致性、状态对账和宕机恢复作为核心差异化能力。

StateRelay 不定位为面向数据开发人员的完整数据工作流平台。首版不追求 Spark、Flink、Hive、DataX 等数据生态，也不建设复杂低代码工作流编辑器。

### 2.1 首版目标

1. Spring Boot 应用通过 Starter 快速接入，普通任务只需实现一个业务方法。
2. 支持 Cron、固定频率、固定延迟、API 和延迟触发。
3. 支持单机、广播和静态分片执行，以及简单 DAG 编排。
4. 支持执行器注册、实例级路由、容量控制、超时、重试、取消、故障转移、日志和告警。
5. 支持外部异步任务的幂等提交、UNKNOWN 对账、回调与轮询补偿、取消和结果导入。
6. 调度节点、执行器 Pod 或 Redis 故障后，系统可从 PostgreSQL 状态恢复。
7. 控制台能够准确回答任务卡在哪里、当前执行权属于谁、是否可以安全重试。

### 2.2 非目标

首版不包含：

- Spark、Flink、Hive、DataX 等数据任务插件；
- 跨工作流依赖、历史补数、动态节点生成和循环工作流；
- 复杂拖拽式 DAG 编辑器；
- MapReduce 动态计算框架；
- Agent 执行模型；
- 多语言 SDK；
- 跨集群主动连接网关；
- Kafka、ZooKeeper 等强制依赖；
- Kubernetes Job 资源调度。

## 3. 设计原则

### 3.1 控制面与执行面分离

调度中心负责定义、触发、选址、状态推进、恢复和运维，不执行任何业务代码。业务代码运行在接入 Starter 的 Java 应用 Pod 中。

### 3.2 PostgreSQL 是最终事实来源

调度状态、执行权、租约和审计信息以 PostgreSQL 为准。HTTP 请求、回调、Redis 通知均允许重复或暂时丢失，任何状态推进必须经过数据库状态校验。

### 3.3 业务数据归业务系统

平台只保存调度元数据、小型参数快照、小型结果和业务数据引用。大型结果、领域对象及敏感业务数据仍由业务系统自己的数据库或对象存储管理。

### 3.4 至少一次执行与幂等协作

平台不承诺任意 Java 业务逻辑恰好执行一次。平台保证投递幂等、执行权围栏和可恢复的至少一次执行，并向业务处理器提供幂等键。业务副作用通过唯一约束、事务表、Outbox 或外部请求标识实现幂等。

### 3.5 定义版本化，运行快照不可变

任务或工作流配置修改后发布新版本，只影响未来实例。运行实例保存定义版本和不可变配置快照，避免运行过程被在线配置变更污染。

## 4. 总体架构

```text
┌──────────────────────── StateRelay Control Plane ────────────────────────┐
│ Trigger │ Workflow │ Dispatcher │ Recovery │ Callback │ Console │ Alert │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ HTTP：实例级 Pod 地址
                                │ 注册、心跳、ACK、进度、日志、结果
┌───────────────────────────────▼──────────────────────────────────────────┐
│ Spring Boot Application + StateRelay Starter                            │
│ Executor HTTP Server │ Worker Pool │ TaskHandler │ Async Adapter        │
│ Reporter │ Local Dispatch Dedup │ Lifecycle READY/DRAINING              │
└──────────────────────────────────────────────────────────────────────────┘
              │                                      │
              ▼                                      ▼
       Business Database                    External Async System

PostgreSQL：最终事实、并发协调、Outbox
Redis（可选）：注册缓存、热点读取、SSE 和实时通知
对象存储（可选）：大日志、结果文件和 Artifact
```

### 4.1 控制面组件

- Definition Service：任务、工作流、触发器及版本发布；
- Trigger Engine：计算到期触发，创建任务或工作流实例；
- Workflow Engine：判断 DAG 节点就绪条件和工作流终态；
- Dispatcher：筛选 Worker、预占容量、创建 Attempt 并可靠下发；
- Recovery Scanner：恢复到期租约、重试任务、UNKNOWN 请求和 Outbox；
- Callback Service：接收执行结果、外部任务回调及状态推进；
- Event Relay：将事务内 Outbox 发布到 Redis、SSE 和告警渠道；
- Console API：实例检索、日志、人工重试、取消、暂停和审计。

### 4.2 Starter 组件

- Handler Scanner：扫描注解和处理器接口，注册 Handler 元数据；
- Executor HTTP Server：接收调度、取消和状态确认命令；
- Dispatch Deduplicator：基于 dispatchId 防止重复入队；
- Worker Pool：有界线程池、Handler 并发隔离和阻塞策略；
- Lifecycle Manager：注册、心跳、READY、DRAINING 和注销；
- Reporter：批量上报进度、日志和最终结果；
- External Async Runtime：托管提交、查询、对账、回调和结果获取。

## 5. 编程模型

### 5.1 普通任务

普通任务只实现业务执行逻辑：

```java
public interface TaskHandler<P, R> {
    TaskResult<R> execute(TaskContext context, P parameter);
}
```

`TaskContext`提供任务实例标识、执行尝试标识、幂等键、取消检查、进度上报、参数和日志能力。注册、心跳、ACK、超时、异常捕获、结果回调和重试均由 Starter 管理。

### 5.2 外部异步任务

外部异步处理器只负责适配远程系统能力：

```java
public interface ExternalAsyncTaskHandler<P, R> {
    SubmitReceipt submit(ExternalTaskContext context, P parameter);
    RemoteTaskSnapshot query(ExternalTaskContext context, SubmitReceipt receipt);
    RemoteTaskSnapshot reconcile(ExternalTaskContext context);
    default void cancel(ExternalTaskContext context, SubmitReceipt receipt) {}
    default R fetchResult(ExternalTaskContext context, SubmitReceipt receipt) { return null; }
}
```

其中 `submit`、`query` 和 `reconcile` 为核心能力。平台负责持久化 requestId、状态机、轮询、回调去重、重试调度和恢复扫描。

### 5.3 处理器元数据

Starter 注册的元数据至少包括：

- appName、handlerName 和 handlerType；
- 参数与结果 Schema；
- 处理器版本及兼容范围；
- 是否支持进度、取消、分片和外部异步协议；
- 默认超时和建议并发上限。

## 6. 核心数据模型

### 6.1 五层对象

1. Task Definition：描述运行什么以及采用什么策略。
2. Task Instance：描述某次具体触发及最终业务可见状态。
3. Task Attempt：描述某一代执行权和一次执行尝试。
4. Dispatch Record：描述一次逻辑 HTTP 投递及其重传和 ACK。
5. Remote Attempt：描述一次外部系统请求及其不确定状态。

普通任务使用前四层；外部异步任务额外使用 Remote Attempt。

### 6.2 关键关系

```text
TaskDefinition 1 ── N TaskInstance
TaskInstance   1 ── N TaskAttempt
TaskAttempt    1 ── 1 DispatchRecord（一个逻辑dispatch可多次HTTP传输）
TaskAttempt    1 ── 0..1 RemoteAttempt

WorkflowDefinition 1 ── N WorkflowInstance
WorkflowInstance   1 ── N WorkflowNodeInstance
WorkflowNodeInstance 1 ── 1 TaskInstance
```

### 6.3 状态职责

#### Task Instance

```text
WAITING → READY → RUNNING → SUCCESS
                    ├────→ RETRY_WAIT → READY
                    ├────→ FAILED
                    └────→ CANCELLED
```

该状态回答用户看到的这次任务最终怎样。FAILED 仅表示重试策略已经耗尽或错误不可重试；单次 Attempt 失败但仍可重试时，Task Instance 直接进入 RETRY_WAIT，不先进入 FAILED。

#### Task Attempt

```text
CREATED → ASSIGNED → ACCEPTED → RUNNING → SUCCESS/FAILED/CANCELLED
                         └──────────────→ LOST
```

该状态回答当前执行权属于哪个 Worker，以及这一代执行是否仍有效。

#### Dispatch Record

```text
PENDING → SENT → ACKED
             └→ UNCERTAIN → ACKED/EXPIRED
```

该状态只回答调度命令是否被执行器确认，不代表业务执行成功。

#### Remote Attempt

```text
CREATED → SUBMITTING → SUBMITTED → RUNNING → SUCCESS/FAILED
              └────→ UNKNOWN → SUBMITTED/ABSENT/MANUAL_CHECK
```

UNKNOWN 表示外部副作用是否发生尚不确定，禁止盲目创建新的远程请求。

### 6.4 关键标识

| 标识 | 含义 |
| --- | --- |
| taskInstanceId | 一次具体触发 |
| attemptId / attemptNo | 一次执行尝试及其序号 |
| dispatchId | 一个逻辑投递；HTTP 重传复用该值 |
| leaseVersion | 执行权代数，用于拒绝旧 Worker 写入 |
| idempotencyKey | 业务副作用幂等协作标识 |
| requestId | 外部系统提交幂等与状态对账标识 |
| remoteTaskId | 外部系统返回的任务标识 |

## 7. 可靠投递协议

### 7.1 正常流程

1. Dispatcher 批量领取 READY 实例。
2. 根据应用、Handler、标签、环境和容量筛选 Worker。
3. 数据库事务内原子预占容量，创建 Attempt、租约和 Dispatch Record。
4. 调度中心向具体 Pod 地址发送 HTTP 执行命令。
5. Starter 根据 dispatchId 幂等接收，并快速返回 ACCEPTED。
6. Starter 在线程池中执行处理器，上报 RUNNING、进度和日志。
7. Starter 回调最终结果，控制面通过 attemptId 与 leaseVersion 做 CAS 更新。
8. 状态变化与 Outbox 在同一事务提交，异步产生实时通知和告警。

### 7.2 ACK 丢失

HTTP 响应丢失时，调度中心使用相同 dispatchId 重传。执行器若已接收该 dispatchId，只返回原 ACK，不再次入队。传输重试不创建新 Attempt。

### 7.3 Worker 宕机

Worker 心跳或 Attempt 租约过期后，Recovery Scanner 将原 Attempt 标记为 LOST，释放容量，并按策略创建新 Attempt，同时提升 leaseVersion。旧 Worker 的迟到进度或结果因版本过期被拒绝。

### 7.4 业务副作用

围栏只能保护平台状态，无法回滚已经提交到业务数据库或第三方系统的副作用。Starter 向业务提供 idempotencyKey；业务使用唯一键、执行记录表、事务 Outbox 或远程幂等键保证重试安全。

## 8. 外部异步任务协议

1. 平台创建 Remote Attempt，并在调用外部系统前持久化唯一 requestId。
2. 调用适配器 `submit`。
3. 成功返回时保存 remoteTaskId，状态进入 SUBMITTED/RUNNING。
4. 网络超时或连接中断时进入 UNKNOWN，不直接重新提交。
5. Recovery Scanner 调用 `reconcile(requestId)`：
   - 查到远程任务：绑定已有 remoteTaskId；
   - 外部明确返回不存在：允许使用原 requestId 再次提交；
   - 无法确认：继续对账或进入 MANUAL_CHECK。
6. 回调负责实时推进，轮询负责补偿；两者使用状态 CAS 和事件标识去重。
7. 远程成功后按需调用 `fetchResult`，大型结果只在平台保存引用。

外部异步等待不长期占用 Java 工作线程。Starter 完成提交后释放线程，由平台定时触发查询或处理远程回调。

## 9. 任务定义、触发器和工作流

### 9.1 任务定义

任务定义包含：

- Handler 引用和参数 Schema；
- 超时、重试次数、退避和错误分类策略；
- Worker Group、标签、路由和版本兼容要求；
- 应用级与 Handler 级并发限制；
- 阻塞策略和告警策略。

任务定义通过草稿、发布版本和停用进行管理。创建实例时保存 definitionVersion 和配置快照。

### 9.2 触发器

首版支持：

- Cron；
- fixed-rate；
- fixed-delay；
- API 手工或业务事件触发；
- 指定时间延迟触发。

唯一约束 `UNIQUE(trigger_id, scheduled_time)` 防止多个调度节点为同一计划时间重复创建实例。

### 9.3 简单 DAG

首版支持串行、并行、汇聚、条件分支和失败继续。发布时验证无环。

节点触发规则：

- ALL_SUCCESS：全部上游成功；
- ALL_DONE：全部上游终结；
- ANY_SUCCESS：至少一个上游成功；
- EXPRESSION：根据小型 JSON 输出或状态表达式判断。

节点间只传递小型 JSON 或 Artifact 引用。失败策略支持终止工作流、继续执行和跳过下游。

## 10. Kubernetes 执行器治理

### 10.1 实例级注册

Starter 通过 Downward API 获取 Pod 名称和 Pod IP，注册：

```text
appName, workerId, workerEpoch, podName, podIP, executorPort,
handlers, handlerVersions, labels, maxConcurrency, usedCapacity, status
```

调度中心直接访问具体 Pod IP 和执行器端口，不通过会负载均衡到任意 Pod 的普通 Service。

### 10.2 两阶段选址

1. 按 appName、Worker Group、环境、标签和 Handler 能力筛选候选组。
2. 过滤非 READY、心跳过期、版本不兼容和无容量实例。
3. 在数据库中原子预占容量令牌后创建 Attempt 和租约。

默认路由采用随机二选一后选择负载较低的实例，避免多个调度节点同时争用一个全局最低负载实例。

### 10.3 并发与阻塞策略

平台同时限制：

- 应用总并发；
- 单 Handler 并发；
- 单任务定义并发；
- Starter 本地有界线程池和队列。

阻塞策略包括 PARALLEL、SERIAL、DISCARD 和 REPLACE。

### 10.4 优雅下线

Pod 生命周期为 STARTING → READY → DRAINING → OFFLINE。`preStop` 触发 DRAINING，停止接收新任务并等待短任务完成。Java 任务采用协作式取消，不强杀线程。长任务未结束时，后续由租约和恢复协议处理。

## 11. 调度中心高可用与性能

### 11.1 无状态集群

调度节点不设置固定 Leader。多个节点可以同时扫描到期触发、待派发实例和过期租约，通过唯一约束、`FOR UPDATE SKIP LOCKED` 和状态 CAS 收敛并发结果。

典型批量领取：

```sql
SELECT id
FROM task_instance
WHERE status IN ('READY', 'RETRY_WAIT')
  AND next_run_at <= now()
ORDER BY priority DESC, next_run_at, id
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

批次必须小、事务必须短，HTTP 调用不放在数据库事务中。

### 11.2 数据库优化

- Task Instance 热表只保留调度必需字段；参数、错误详情和大日志拆分；
- 为 READY、RETRY_WAIT、RUNNING 租约到期等活跃状态建立部分索引；
- 实例、Attempt、Dispatch、事件按月分区；
- 在线保留近期数据，终态旧分区归档；
- 列表采用游标分页，不使用深 Offset；
- 使用 `EXPLAIN ANALYZE` 验证扫描计划和索引命中。

### 11.3 背压

调度领取批次、HTTP 下发并发、回调处理、日志上传和事件发布均使用有界队列。无 Worker 容量时更新 next_run_at 延后重试，不在调度节点内存堆积。

### 11.4 Outbox 和 Redis 降级

状态变更与 Outbox 事件在同一 PostgreSQL 事务提交。Event Relay 将事件发布到 Redis、SSE 和告警渠道。Redis 不可用时任务仍正常调度，控制台实时性下降；恢复后通过 Outbox 补发。首版不要求 Kafka。

## 12. 控制台与运维治理

首版提供六类页面：

1. 应用与执行器：Pod、版本、Handler、容量、生命周期和心跳；
2. 任务定义：版本、触发器、路由、超时、重试、并发及启停；
3. 工作流：节点关系、发布版本和运行拓扑；
4. 实例中心：按应用、任务、状态、时间、业务键和错误类型查询；
5. 实例详情：统一时间线展示 Instance、Attempt、Dispatch、租约、Remote Attempt、日志和结果；
6. 告警与审计：规则、通知渠道、静默、确认和人工操作记录。

人工重试、取消、暂停和强制终结必须通过平台命令执行，使用状态 CAS，并写入操作者、原因、前后状态及时间。禁止直接修改数据库状态。

告警至少覆盖：

- 连续失败和失败率突增；
- READY、RETRY_WAIT、UNKNOWN 和 Outbox 积压；
- 调度延迟 P95/P99 超标；
- Worker 离线和容量耗尽；
- 外部 UNKNOWN 长时间未对账；
- 回调或事件补偿积压。

## 13. 安全设计

- 调度中心与执行器使用服务身份、短期凭证或双向 TLS；
- Kubernetes NetworkPolicy 只允许调度中心访问执行器内部端口；
- 控制台和 API 采用应用或项目级 RBAC；
- 参数和日志默认脱敏，敏感业务数据禁止进入平台；
- 外部回调校验签名、时间戳和重放标识；
- 任务定义发布、人工恢复和权限变更全部记录审计事件；
- Starter 不默认开放脚本执行、任意反射调用或上传代码能力。

## 14. 可观测性

### 14.1 指标

- 调度延迟：scheduledAt 到 dispatchAt；
- READY、RETRY_WAIT、UNKNOWN 积压数量和最老年龄；
- HTTP 下发成功率、ACK 延迟和重传率；
- Attempt 成功率、错误分类和重试次数；
- Worker 容量利用率、队列等待时间和离线数量；
- PostgreSQL 扫描耗时、锁等待和事务失败；
- Outbox 发布延迟、回调补偿延迟和外部轮询次数。

### 14.2 关联标识

日志和追踪统一携带 taskInstanceId、attemptId、dispatchId、workerId、leaseVersion、requestId、remoteTaskId 和 traceId。业务幂等键只记录脱敏摘要。

## 15. 测试与验收

### 15.1 测试层次

- 单元与契约测试：触发计算、退避、状态迁移、DAG 判断和 SDK 契约；
- 数据库并发测试：唯一约束、CAS、SKIP LOCKED、租约和分区；
- 端到端测试：调度、HTTP、Handler、回调、重试和恢复；
- 故障注入测试：调度 Pod、执行器 Pod、网络、Redis、数据库和外部系统故障。

### 15.2 必测故障

1. HTTP ACK 丢失：相同 dispatchId 重传，执行器只入队一次；
2. Worker 执行中崩溃：租约到期后产生新 Attempt，旧结果被拒绝；
3. 调度节点事务提交后崩溃：其他节点继续处理，计划实例不重复；
4. Redis 完全不可用：任务正确运行，实时通知降级并可补发；
5. 外部 submit 成功但响应丢失：UNKNOWN 对账找到原任务，不重复提交；
6. 回调重复、乱序或丢失：状态不回退，轮询最终补偿。

### 15.3 首版量化目标

- 50 个接入应用；
- 日任务实例 100 万；
- 峰值触发 100 次/秒；
- 有可用 Worker 时，调度延迟 P95 不超过 1 秒，P99 不超过 3 秒；
- 调度节点故障后 30 秒内恢复处理；
- Worker 故障后不超过两个租约周期进入恢复；
- 同一 triggerId 和 scheduledTime 的重复实例数为零；
- 旧 leaseVersion 成功写入数为零；
- 外部系统重复创建任务数为零。

上述性能目标需在真实 Kubernetes、PostgreSQL 规格和网络条件下压测校准，并保留基准脚本与容量报告。

## 16. 分阶段建设路线

### 16.1 第一阶段：可靠单任务闭环

- Task Definition、Trigger、Instance、Attempt 和 Dispatch Record；
- Spring Boot Starter、普通 TaskHandler、实例级注册和 HTTP 下发；
- 容量、租约、ACK 重传、超时、重试、取消和优雅下线；
- 实例列表、实例时间线、日志和人工恢复；
- PostgreSQL 协调、Outbox、指标和核心故障测试。

### 16.2 第二阶段：外部异步长任务

- ExternalAsyncTaskHandler 和 Remote Attempt；
- requestId、UNKNOWN 对账、回调与轮询补偿；
- 远程取消、结果引用和人工对账；
- 将空间审查 GIS 任务作为首个生产适配器验证协议。

### 16.3 第三阶段：简单工作流与治理

- 版本化 Workflow Definition 和不可变运行快照；
- 串并行、汇聚、条件、失败继续和参数引用；
- 应用级 RBAC、告警规则、审计、归档和容量治理；
- 广播、静态分片和更多路由策略。

### 16.4 后续演进触发条件

只有满足明确需求或测量到瓶颈时才演进：

- 跨集群任务达到稳定规模时，引入主动连接网关；
- PostgreSQL 协调成为实测瓶颈时，引入时间分片、分区调度或消息队列；
- 多个非 Java 场景出现时，再建设多语言协议；
- 数据开发团队成为主要用户时，再评估数据源、补数和复杂可视化 DAG。

## 17. 竞争边界与优势

StateRelay 首版不以任务插件数量和数据生态与 DolphinScheduler 竞争，也不以 MapReduce 动态计算能力与 PowerJob 竞争。它以以下组合建立价值：

1. 普通 Spring Boot 系统低成本接入；
2. 调度定义、实例、尝试、HTTP 投递和外部请求五层模型；
3. dispatchId、leaseVersion、idempotencyKey 和 requestId 分别解决不同重复问题；
4. 明确建模外部提交 UNKNOWN，而不是把网络超时等同失败；
5. 回调、轮询、恢复扫描和人工对账形成完整长任务生命周期；
6. 数据库最终事实与 Redis 可降级，便于在公司内部稳定部署；
7. 以实例时间线和证据链为中心的运维体验。

这一定位使平台不必复制成熟数据工作流产品的全部功能，也能在 Java 业务系统和外部异步长任务场景中形成明确优势。

## 18. 已确认决策汇总

- 首批用户为公司内部普通 Java 业务系统；
- 采用 Spring Boot Starter，业务只实现处理器能力；
- 调度中心和执行器位于同一 Kubernetes 集群；
- 执行器主动注册，调度中心通过 HTTP 直调具体 Pod；
- 使用普通任务与外部异步任务两套处理器；
- 采用至少一次执行、投递幂等和执行权围栏；
- 采用五层核心数据模型；
- 定义版本化，运行实例使用不可变快照；
- 首版支持简单 DAG，不建设数据开发生态；
- 采用容量预占、实例级路由和 DRAINING 优雅下线；
- 调度中心无状态，使用 PostgreSQL 协调和 Outbox；
- 控制台以实例时间线为核心；
- 故障注入和量化指标作为发布门槛。
