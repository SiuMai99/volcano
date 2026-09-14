# Volcano 通用 xPU 拓扑感知调度设计（收敛版 V2）

> 状态：结构化设计提案，不代表 Volcano 已实现或社区已接受。
>
> 本文以 [99-generic-xpu-topology-aware-design-zh.md](./99-generic-xpu-topology-aware-design-zh.md) 为基线，参考
> [Network Topology Aware Scheduling.md](../Network%20Topology%20Aware%20Scheduling.md) 的设计和当前实现，对组语义、
> Statement 事务边界和拓扑状态保存方式进行收敛。
>
> 关联议题：[volcano-sh/volcano#5751](https://github.com/volcano-sh/volcano/issues/5751)

## 1. 设计结论

### 1.1 核心收敛

本文采用现有 Network Topology 的分层，不为 xPU 再复制一套平行的 Job、Group 和事务模型：

~~~text
PodGroup / SubGroup 的 workload intent
  -> JobInfo / SubJobInfo 保存作用范围和策略
  -> 现有 HyperNode gradient/order 产生 Node 候选
  -> xPU 插件在候选 Node 内做 DeviceDomain filter/score
  -> Statement.Allocate / Discard / Commit 复用现有推演事务
  -> Job/SubJob 或插件私有状态保存已选择的 Domain/Fabric
  -> exact-ID、Adapter、持久恢复按需要增量引入
~~~

四条边界必须保持：

1. HyperNode 只描述 Node/网络层次，不增加 Device、Domain、Health、Reservation 等 xPU 热状态；
2. JobInfo、SubJobInfo 是现有的 Job/SubGroup 调度上下文，也是 Group 级拓扑策略的主要作用范围；
3. Statement 是本轮 Task Allocate 的推演、提交和回退载体；
4. Device/Domain/Fabric 的观察状态属于 xPU 插件自己的 topology cache/session overlay，不能混入 HyperNode。

### 1.2 三个原有概念的处理结果

| 原稿概念 | V2 处理 | 原因 |
| --- | --- | --- |
| TopologyGroup | 不再作为独立调度对象；保留 GroupRef/稳定 ID 的语义 | applyTo=Group 当前可以直接映射到 JobInfo 或 SubJobInfo |
| AdmissionSet | 不再作为平行事务对象；定义为 Statement 中新增 Allocate operations 的逻辑视图 | 当前 Statement 已经能保存、提交和回退本轮 operations |
| GroupPlacementAnchor | 保留其“已选择 Domain/Fabric”的语义，改名为 AllocatedDomain/Fabric placement state | 它是后续调度使用的 placement state，不应只是内存快照，也不应替代 per-Pod allocation evidence |

### 1.3 MVP 与增强阶段

MVP 先复用现有能力：

- JobInfo/SubJobInfo 作为拓扑策略和生命周期上下文；
- HyperNode gradient/order 作为 Node 候选和网络层次排序；
- xPU 插件私有 cache 维护 Device/Domain 的拓扑和可用状态；
- Statement 维护本轮 Allocate、Discard、Commit；
- Node-local DeviceDomain 做 hard filter 和 soft score；
- Mock/Annotation Provider 验证规范化模型和调度语义。

只有在下列要求出现时，才增加更强的 group handoff：

~~~text
跨多个 admission 波次保持同一 Fabric/Domain
  或必须执行 exact Device ID
  或需要 Adapter/Claim 的外部分配协议
  或 scheduler 重启后要求 hard fail-closed 恢复
~~~

此时增加的是 placement evidence、Adapter 协议和 Statement 提交前 hook，
不是把大量字段扩散到 HyperNode，也不是再创建一套独立调度框架。

## 2. 问题、场景与保证边界

### 2.1 总量满足不等于 Domain 满足

一台 Node 上有两个互不等价的 Device Domain：

~~~text
Domain A: 6 个 free Device
Domain B: 2 个 free Device
Node 总 free: 8 个 Device
~~~

一个要求 8 个 Device 且必须位于同一个 Domain 的 Pod，不能因为 Node 总量为 8 就被判定为可调度。
因此 xPU 插件需要在已经通过普通 Node Predicate 的候选内，继续验证 DeviceDomain 的容量和 membership。

### 2.2 三类场景

| 场景 | 主要约束 | V2 处理 |
| --- | --- | --- |
| 单 Node 多 Device Domain | 一个 Pod 的 Device 必须来自同一 Node Domain | xPU 插件在每个候选 Node 内做 Domain filter |
| 普通多 Node 集群 | xPU scale-up 只在 Node 内，Node 间是 IB/RoCE | HyperNode 负责 Node 范围，xPU Domain 只负责本地设备 |
| 跨 Node Fabric | 多个 Node 的设备属于同一显式 Fabric | MVP 只保留 Provider/Fabric 模型；跨波次粘性和 exact handoff 属于增强阶段 |

### 2.3 Group 与 Pod 的作用范围

本文沿用 Volcano 的实际层次：

~~~text
Job       = PodGroup 在 scheduler 内的 JobInfo
SubJob    = SubGroup 在 scheduler 内的 SubJobInfo
Task      = Pod 在 scheduler 内的 TaskInfo
~~~

DeviceTopologyPolicy.applyTo 只有两个作用范围：

| applyTo | 作用范围 | 对应的现有对象 |
| --- | --- | --- |
| Pod | 每个匹配 Pod 独立满足 | TaskInfo |
| Group | 整个 PodGroup 或一个 SubGroup 共同满足 | JobInfo 或 SubJobInfo |

不再额外定义 target: Partition。Partition、TP、PP、EP 等分组继续使用现有
SubGroupPolicy、label selector 和 SubJobInfo。

### 2.4 V2 明确保证什么

在 MVP 调度语义层，V2 保证：

- hard Node-local policy 不会把不同 Domain 的 free count 拼接成一个假方案；
- xPU 只缩小已有 Node/HyperNode 候选，不重新引入已被其他 Predicate 排除的 Node；
- Statement 中的失败 Allocate 可以按现有机制回退；
- Job/SubJob 的后续 Task 可以参考已选择的 Domain/Fabric placement state；
- 没有 DeviceTopology 时，行为与当前 Volcano 一致。

V2 不把下面的能力误写成当前实现或 MVP 默认保证：

- Device Plugin 数量接口不能自动证明 Volcano 选择的 UUID 被运行时执行；
- Kubernetes 多个 Pod Binding 不是 API 级原子事务；
- 内存 Session Snapshot 不能单独提供 scheduler 重启后的 hard exact 恢复；
- 当前 Statement.Commit() 和 AddBindTask() 尚未提供整组 Adapter Prepare/PreBind barrier。

## 3. 现有 Network Topology 设计与实现基线

### 3.1 HyperNode 的职责

现有设计把 HyperNode 定义为由 Node 或子性能域组成的网络性能域，并通过 tier/tierName
表达不同层次的网络边界。HyperNode 是 Node placement plane，不是设备分配器。

当前 HyperNodeInfo 只保存结构拓扑信息：

~~~text
Name
HyperNode
Parent
Children
tier
tierName
isDeleting
~~~

对应实现：pkg/scheduler/api/hyper_node_info.go 的 HyperNodeInfo。

V2 保持这个边界。xPU 的 Device、Domain、Fabric、Health 和 Reservation
不进入 HyperNode CRD 或 HyperNodeInfo。

### 3.2 JobInfo/SubJobInfo 的职责

现有 Network Topology 实现把调度语义放在 Job/SubJob 上：

~~~text
JobInfo:
    NetworkTopology
    AllocatedHyperNode
    SubJobs
    TaskToSubJob

SubJobInfo:
    NetworkTopology
    AllocatedHyperNode
    Tasks
    MinAvailable
~~~

对应实现：

- pkg/scheduler/api/job_info.go 的 JobInfo；
- pkg/scheduler/api/sub_job_info.go 的 SubJobInfo。

Job/SubJob 负责：

- 保存从 PodGroup/SubGroupPolicy 转换来的拓扑约束；
- 区分 Job 级和 SubJob 级作用范围；
- 参与 JobReady/SubJobReady；
- 保存当前已经形成的 placement context；
- 作为后续 Task 调度的输入。

因此 xPU 设计不需要再创造一个独立 TopologyGroup 来表示同一层语义。

### 3.3 Session Open 与 placement state 恢复

Session 打开时，现有实现会：

~~~text
SchedulerCache.Snapshot
  -> 构造 Session 内的 HyperNodes/Jobs/Nodes
  -> 解析 NetworkTopology tier
  -> 清理失效 AllocatedHyperNode
  -> 根据已分配 Task 的 Node 恢复 AllocatedHyperNode
~~~

对应函数：

- framework.OpenSession；
- Session.adjustNetworkTopologySpec；
- Session.removeInvalidAllocatedHyperNode；
- Session.recoverAllocatedHyperNode。

网络拓扑可以这样恢复，是因为 Node membership 足以推断候选 HyperNode。
当没有已分配 Task 时，现有实现会清除 AllocatedHyperNode，允许重新选择。

V2 借鉴这个生命周期，但不假设 xPU 可以永远照搬它：

~~~text
Node -> HyperNode
~~~

通常可以推导，而：

~~~text
Node -> Node-local DeviceDomain -> exact Device ID
~~~

不一定可以推导。一个 Node 内有多个 Device Domain 时，只知道 Pod 在哪个 Node，
不能知道它使用了哪个 Domain 或哪些 Device ID。

### 3.4 network-topology-aware 插件调用链

现有插件注册：

~~~text
AddHyperNodeGradientForJobFn
AddHyperNodeGradientForSubJobFn
AddHyperNodeOrderFn
AddBatchNodeOrderFn
AllocateFunc
DeallocateFunc
~~~

对应实现：pkg/scheduler/plugins/network-topology-aware/network_topology_aware.go。

其核心行为是：

1. hard topology 根据允许 tier 和已有 AllocatedHyperNode 生成 HyperNode gradients；
2. HyperNodeOrder 做资源 binpack；
3. BatchNodeOrder 根据候选 Node 与已选择 HyperNode 的 LCA 进行排序；
4. Allocate/Deallocate callback 更新插件私有的 HyperNode 资源使用缓存；
5. gang 插件通过 JobReady/SubJobReady 判断 Job/SubJob 是否达到最小可运行集合。

注意：这里的资源缓存是 HyperNode 级的聚合 score 状态，不是 HyperNodeInfo
新增的公共 Device 字段。

### 3.5 allocate action 与 Statement

现有 allocate action 的主路径是：

~~~text
allocateForJob
  -> HyperNodeGradientForJobFn
  -> allocateForSubJob
  -> allocateResourcesForTasks
  -> Statement.Allocate(Task, Node)
  -> JobReady/SubJobReady
  -> SaveOperations/RecoverOperations
  -> Statement.Commit 或 Statement.Discard
~~~

对应函数：

- pkg/scheduler/actions/allocate/allocate.go:allocateForJob；
- pkg/scheduler/actions/allocate/allocate.go:allocateForSubJob；
- pkg/scheduler/actions/allocate/allocate.go:allocateResourcesForTasks；
- pkg/scheduler/framework/statement.go。

Statement 当前保存的是 Evict、Pipeline、Allocate operation。
Allocate 期间更新 Session 中的 Task/Node 状态，失败时执行回退；
Discard 逆序撤销 operation；Commit 逐个调用 AddBindTask。

这已经覆盖了“本轮新增 Allocate operations 及其回退”的基本需求，
但当前提交仍是 per-Task 的：

~~~text
Statement.Commit
  -> AddBindTask(Task 0)
  -> AddBindTask(Task 1)
  -> ...
~~~

它不是整组 Adapter Prepare、全组 PreBind 或 Kubernetes Binding 的原子保证。

## 4. V2 的分层架构

### 4.1 组件关系

~~~mermaid
flowchart TB
    intent["PodGroup / SubGroup policy"] --> context["JobInfo / SubJobInfo"]
    context --> hn["Existing HyperNode gradients and order"]
    hn --> nodes["Candidate Nodes"]
    nodes --> xpu["xPU plugin: DeviceDomain filter and score"]
    xpu --> stmt["Statement.Allocate / Discard / Commit"]
    stmt --> state["AllocatedDomain / Fabric placement state"]
    xpu --> cache["xPU topology cache and Session overlay"]
    state -. optional exact evidence .-> adapter["Adapter / Claim / durable record"]
    adapter -. optional group handoff .-> stmt
~~~

### 4.2 所有权表

| 组件 | 拥有 | 不拥有 |
| --- | --- | --- |
| Public API | resource、applyTo、mode、scope、tier/tierName 等用户意图 | Device ID、Domain ID、Health、Reservation、Adapter 名称 |
| JobInfo/SubJobInfo | Job/SubGroup policy、readiness、Task membership、placement context | Provider 原始 payload、全局 Device owner |
| HyperNode/HyperNodeInfo | Node/网络层次、Parent/Children、tier | Device/Domain/Health/Reservation |
| xPU Provider | 物理拓扑观察、版本、来源、新鲜度 | workload policy、最终调度选择 |
| xPU topology cache | canonical Device/Domain/Fabric、membership、Health、AllocationState | Public API 和 Job readiness |
| Session overlay | 当前 Session 的 speculative Device/Domain 状态 | 跨 Session 的已提交 owner |
| Statement | 本轮 operations 的 Allocate、Discard、Commit | Provider 解析和长期拓扑事实 |
| Adapter | 将已选择的 Device ID 落实到外部机制 | Domain/Fabric 规划 |

### 4.3 不修改 HyperNode 的理由

Network Topology 已经证明，Node/网络拓扑和 workload placement state 可以分离：

~~~text
HyperNodeInfo
  = 静态或准静态的 Node topology

JobInfo/SubJobInfo.AllocatedHyperNode
  = Job/SubJob 当前的 placement context

network-topology-aware private cache
  = HyperNode 级聚合资源 score
~~~

xPU 采用同一原则：

~~~text
HyperNodeInfo
  = Node/网络候选

JobInfo/SubJobInfo 或插件私有 placement state
  = 当前 Job/SubJob 选择的 Domain/Fabric

xPU cache
  = Device/Domain/Fabric/Health/Reservation
~~~

## 5. 组语义：复用 Job/SubJob，不复制三套对象

### 5.1 GroupRef 替代独立 TopologyGroup

当 applyTo=Group 时，作用范围直接使用现有对象：

~~~text
PodGroup level:
    GroupRef = JobInfo.UID

SubGroup level:
    GroupRef = SubJobInfo.UID
~~~

如需稳定字符串 ID，可以定义：

~~~go
type GroupRef struct {
    JobID    api.JobID
    SubJobID api.SubJobID
}
~~~

没有 SubJob 时只使用 JobID；有 SubGroup 时使用 JobID + SubJobID。
这已经能区分 PodGroup、SubGroup 和 Task，不需要额外创建 TopologyGroup 生命周期。

TopologyGroup 这个词可以在设计讨论中作为“Group 级拓扑作用范围”的概念名，
但不应被实现成独立 CRD、独立 scheduler object 或独立 readiness 体系。

### 5.2 AdmissionSet 是 Statement 的逻辑视图

对某个 GroupRef 和一个 winning Statement，定义：

~~~text
AdmissionSet(statement, group)
  = statement.operations 中属于该 group 的新增 Allocate operations
~~~

它的边界是：

- Group 尚未 Ready 时，包含本次使 JobReady/SubJobReady 成立的新增 operations；
- Group 已经 Ready 时，包含本轮真正新增的 operations；
- 已经 Binding/Running 且拥有可恢复 assignment 的成员不重复进入；
- Statement 中其他 Job、其他 SubJob 或 Evict/Pipeline operation 不自动属于它。

因此 AdmissionSet 是一个有用的解释和校验视图，但不是新事务类型。

实现上优先使用：

~~~text
Statement
  + GroupRef
  + xPU topology context
  + selected assignment / placement state
~~~

而不是：

~~~text
Statement transaction
  + another AdmissionSet transaction
  + another participant framework
~~~

### 5.3 Statement 已有能力与仍需补充的契约

当前 Statement 已经能做：

- 收集 Allocate operations；
- Allocate callback 失败时回退；
- Discard 时逆序回退；
- Save/Recover speculative operations；
- Commit 时把 Task 加入 bind path。

只有以下增强语义不能从当前 Statement 自动推出：

1. 一个 Group 的所有新增 operations 必须使用同一个不可变 topology plan；
2. 每个 Task 必须有完整的 DeviceDomain assignment；
3. 多个 Device ID 的最终 CAS 必须作为一个 group-level 操作；
4. Adapter Prepare/Abort 必须覆盖整个集合；
5. 所有成员 PreBind 成功前不能开始任何 Kubernetes Bind；
6. 外部副作用的 fencing、重启恢复和不确定结果需要记录。

因此第一阶段可以增加一个插件私有的 GroupTopologyContext，
后续 exact 阶段再增加 BeforeStatementCommit 一类提交前 hook。
不应在 MVP 中把 AdmissionSet 设计成与 Statement 并行的事务框架。

## 6. Placement state：从 AllocatedHyperNode 推导 xPU 设计

### 6.1 语义

Network Topology 的：

~~~text
JobInfo/SubJobInfo.AllocatedHyperNode
~~~

对应到 xPU 可以是：

~~~text
AllocatedDomain
AllocatedFabric
~~~

它表达：

~~~text
当前 Job/SubJob 已经形成了一个可供后续 Task 参考的 Domain/Fabric placement
~~~

它不表达：

- 每个 Pod 的全部 Device ID；
- Device 的 owner 和 reservation 全部状态；
- Device Plugin 一定会执行 Volcano 选择的 ID；
- Kubernetes Binding 已经成功。

### 6.2 推荐的内部状态

MVP 可以使用插件私有状态或 Job/SubJob 的 scheduler-internal 字段：

~~~go
type DevicePlacementState struct {
    GroupRef              GroupRef
    ResourceName          corev1.ResourceName
    DomainKeys            []DomainKey
    FabricKeys            []DomainKey
    PolicyFingerprint     string
    EvidenceRef           string
    Phase                 PlacementPhase
}
~~~

DomainKeys 和 FabricKeys 保存组级 placement；每个 Task 的具体 assignment
仍然由 Task/BindContext/Adapter allocation record 保存。

第一阶段可以只保存：

~~~text
GroupRef + selected Domain/Fabric key
~~~

不要为了预留后续 exact 能力，把 fencing token、Adapter participant、Claim 状态等
字段全部放入 JobInfo 或 HyperNode。

### 6.3 内存状态与恢复证据

内存 Session Snapshot 是一次调度周期的派生视图：

~~~text
Cache Snapshot
  -> Session Job/SubJob
  -> plugin placement state
~~~

它适合完成当前周期的过滤、打分和推演，但不是 hard exact 语义的唯一持久来源。

恢复能力按拓扑粒度区分：

| 状态 | 仅凭已绑定 Pod 的 Node 能否恢复 | 需要什么 |
| --- | --- | --- |
| Job/SubJob 的 HyperNode | 通常可以 | Node membership 和现有恢复逻辑 |
| Node-local DeviceDomain | 不一定 | Domain membership 或 allocation evidence |
| exact Device ID | 不能可靠推导 | Pod annotation、HAMi/vGPU record、DRA Claim 或 Adapter record |
| 未绑定但已 Prepare 的外部 allocation | 不能凭 Pod Node 推导 | fencing token 和 Adapter reconciliation |

因此 AllocatedDomain/Fabric 可以像 AllocatedHyperNode 一样作为内存 placement state，
但只要设计声称 scheduler 重启后仍有 hard exact 保证，就必须同时定义可恢复 evidence。

### 6.4 生命周期

placement state 遵循现有 Job/SubJob 生命周期：

~~~text
无 active allocated member
  -> placement state 为空

本轮 Statement speculative Allocate
  -> 只写 Session overlay，失败则 Discard

Statement 成功 Commit
  -> 保存 selected Domain/Fabric placement

后续新增 Task
  -> 参考已有 placement，不能无理由漂移

所有 active member 结束并释放
  -> 清理 placement state

Session 重建
  -> 从现有 assignment/evidence 恢复；无法恢复时按 hard/soft 语义处理
~~~

与现有 AllocatedHyperNode 一样，如果 Job/SubJob 已没有 active allocated task，
可以清除 placement state 并允许下一次全新选择。但如果还有 active exact allocation，
不能只因内存字段为空就选择新的 Domain/Fabric。

## 7. Canonical xPU 模型与缓存

### 7.1 保留必要的 vendor-neutral 模型

V2 仍然需要独立于 HyperNode 的 xPU canonical model：

~~~text
Device
  = 可被分配和互斥占用的稳定设备身份

DeviceDomain
  = 在同一性能边界内的 Device 集合

FabricDomain
  = 允许跨 Node 的显式 Device/local-Domain 集合

Membership
  = Domain 到 Device 或子 Domain 的显式关系

Link
  = 可选的 pair capability，仅用于 soft score
~~~

Node 或 HyperNode 关联只表示 placement association，不自动表示某个 Node 的所有
Device 都属于某个 Fabric。Fabric membership 必须显式列出 Device 或 local Domain。

### 7.2 稳定 ID 范围

建议的内部 identity：

~~~text
DeviceKey
  = ResourceName + NodeUID + ProviderDeviceID

NodeDomainKey
  = ResourceName + NodeUID + ProviderDomainID

FabricDomainKey
  = ResourceName + ProviderFabricID
~~~

其中：

- NodeUID 用于避免 Node 重建后同名对象误认；
- Node-local ProviderDomainID 只需在同一 ResourceName + NodeUID 内唯一；
- Fabric ID 必须在同一 ResourceName 的 Fabric scope 内唯一；
- tierName 是非唯一的分类/合同名称，不是实例身份；
- fabricEpoch 或 source generation 是版本信息，不是主键。

### 7.3 Health 与 AllocationState 分离

Device cache 中至少区分：

~~~text
Health:
    Healthy / Unhealthy / Unknown

AllocationState:
    Free / TentativeReserved / Allocated / ReconcilePending
~~~

只有：

~~~text
Health == Healthy && AllocationState == Free
~~~

才能进入 hard policy 的 SchedulableFree。

这些状态属于 xPU cache 和 Session overlay，不属于 HyperNodeInfo。

### 7.4 Provider、Normalizer、Cache

Provider 负责从 Mock、Node Annotation、厂商 API、Device Plugin companion 或 DRA
观察输入；Normalizer 负责：

- 校验稳定 ID；
- 检查重复、悬空和跨 Node membership；
- 解析 Parent/Child closure；
- 去重 EffectiveDeviceKeys；
- 绑定 source epoch、generation、fingerprint 和 freshness；
- 生成 canonical Device/Domain/Fabric snapshot。

Cache 负责：

~~~text
Topology snapshot
Device -> Domains index
Node -> local Domains index
Fabric -> participating local Domains/Devices index
Health
AllocationState
placement evidence
~~~

Provider 不负责 workload policy、Filter/Score 或 owner 提交。

## 8. MVP 调度算法

### 8.1 复用现有调用链

MVP 的建议调用链：

~~~text
Scheduler.runOnce
  -> framework.OpenSession
  -> SchedulerCache.Snapshot
  -> Session.adjustNetworkTopologySpec
  -> network-topology-aware HyperNode gradient/order
  -> xPU plugin narrows candidate Node
  -> xPU plugin filters local DeviceDomain
  -> Statement.Allocate(Task, Node)
  -> AllocateFunc updates xPU Session overlay
  -> JobReady/SubJobReady
  -> Statement.Commit or Statement.Discard
  -> AddBindTask per Task
~~~

xPU 插件不能绕过普通 Predicate，也不能在 NodeOrder 阶段把一个已被普通 Predicate
排除的 Node 重新带回来。

### 8.2 Node-local hard filter

对一个请求 k 个整卡 Device：

1. 接受现有 HyperNode/Predicate 产生的 Node 候选；
2. 查找该 Node 上匹配 resource、scope、tier 的 local Domain；
3. 计算 Healthy 与 Free 的 effective Device 集合；
4. 只有某一个 Domain 的可用数量不少于 k 时才保留该 Node；
5. 6 + 2 不能拼成 8；
6. 没有完整方案时返回结构化 fit reason。

MVP 的 hard filter 证明的是 scheduler-side Domain 语义。
如果实际 Adapter 不能落实具体 Device ID，不得把这个结果升级为 hard exact runtime 保证。

### 8.3 Soft score

soft topology 继续沿用现有 Node/HyperNode score 组合方式：

~~~text
优先满足更多 preferred policy
  -> 优先使用更少的 Domain
  -> 优先使用更少的 Node
  -> 优先减少剩余碎片
  -> 有可信 Link 时再比较 Link score
~~~

没有 Link 或 Device exact enforcement 时，soft 只提供 advisory score，
不应因此改变普通 Node resource 的基本可调度性。

### 8.4 Domain placement 的首次选择与后续选择

首次为 Job/SubJob 调度时：

~~~text
候选 Node
  -> 候选 local Domain/Fabric
  -> 选择 Domain/Fabric
  -> 写入 Session placement state
~~~

后续新增 Task 时：

~~~text
已有 placement state
  -> 先检查现有 Domain/Fabric 是否仍然有效
  -> 在其范围内选择可用 Device/Node
  -> 不因每轮重新打开 Session 就随机重选
~~~

如果 placement state 不存在，允许从已绑定成员和 evidence 恢复；恢复失败时：

- soft policy 可以退化为普通候选和可解释 score；
- hard policy 不能假设满足，必须 fail closed 或等待明确恢复。

## 9. Exact-ID 与增强 handoff

### 9.1 什么时候需要增强机制

以下场景不属于简单的 HyperNode candidate filter：

~~~text
Node 内多个 Domain，且运行时必须使用 scheduler 选定的 Device ID
跨多个 admission 波次保持同一 Fabric
多个 Pod/Claim 需要一起 Prepare
Adapter Commit 可能部分成功
scheduler 崩溃后不能静默重新选 Domain/Device
~~~

这些场景需要 xPU 插件在 Statement 外围增加少量 group topology context，
而不是建立新的完整调度主循环。

### 9.2 GroupTopologyContext

建议的增强上下文：

~~~go
type GroupTopologyContext struct {
    GroupRef              GroupRef
    Placement             DevicePlacementState
    NewAllocateTaskIDs    []api.TaskID
    TaskAssignments       map[api.TaskID]TaskResourceAssignment
    ReferencedFingerprints map[ObjectKey]string
    ReservationToken      string
}
~~~

它附着在 winning Statement 或由 Statement 唯一拥有，不另建一份会重复提交的
AdmissionSet participant。

TaskResourceAssignment 只在 exact 路径要求时包含具体 DeviceKeys：

~~~go
type TaskResourceAssignment struct {
    ResourceName corev1.ResourceName
    NodeName     string
    DomainKeys   []DomainKey
    DeviceKeys   []DeviceKey
    AdapterName  string
}
~~~

BestEffort 路径不应伪造 DeviceKeys。

### 9.3 提交前 hook 的最小边界

只有 exact group handoff 开启时，才需要类似：

~~~go
type BeforeStatementCommitFn func(
    ctx context.Context,
    statement *framework.Statement,
    topology *GroupTopologyContext,
) error
~~~

概念顺序：

~~~text
Statement speculative Allocate
  -> Session xPU overlay
  -> JobReady/SubJobReady
  -> final cache reservation/CAS
  -> Adapter Prepare/Commit
  -> persist placement evidence
  -> existing Statement Commit / group pre-bind gate
  -> per-Pod Kubernetes Bind
~~~

在这个阶段，AdmissionSet 只是 GroupTopologyContext.NewAllocateTaskIDs
的来源和校验集合。它不取代 Statement，也不取代 JobReady/SubJobReady。

### 9.4 Guarantee boundary

即使启用了 exact group handoff，第一阶段最多承诺：

~~~text
任何 Pod 开始 Kubernetes Bind 前：
  所有 exact assignment 已完成必要 reservation；
  Adapter Prepare/Commit 已成功；
  placement evidence 已成功写入或进入可恢复 reconciliation；
  所有要求的 PreBind 已完成。
~~~

Kubernetes Binding 仍然按 Pod 逐个提交。第一个 Pod 成功、第二个 Pod 失败时，
不能声称 Kubernetes API 提供整组回滚。

## 10. 失败、恢复与可观测性

### 10.1 MVP 失败处理

MVP 复用现有 Statement：

~~~text
Predicate/Domain filter 失败
  -> 当前候选失败，继续其他候选或返回 fit reason

Allocate callback 失败
  -> Statement.Allocate 回退当前 Task

winning solution 不满足 JobReady/SubJobReady
  -> Statement.Discard

已提交 placement 发生健康或 membership 变化
  -> 重新验证 placement；不能使用过期 Domain summary
~~~

### 10.2 Exact 阶段失败处理

当 Adapter 或外部记录参与后：

| 事件 | 处理 |
| --- | --- |
| Reserve 前 fingerprint 改变 | 丢弃旧 plan，重新规划 |
| 任一 Task 没有完整 assignment | 整个 Group context 失败 |
| Adapter Prepare 失败 | Abort 已准备部分，撤销 Session/Cache reservation |
| Abort/Release 结果不明确 | 保留 ReconcilePending，不能直接标记 Free |
| scheduler 重启 | 从 evidence/Adapter observation 恢复；否则 hard fail closed |
| active member 仍存在但 placement 不可恢复 | 不为后续成员静默选择第二个 Domain/Fabric |

ReconcilePending 是增强阶段的 cache 状态，不应提前扩展为 JobInfo、SubJobInfo
或 HyperNode 的公共字段。

### 10.3 观测信息

MVP 至少记录：

~~~text
Job/SubJob/Task
candidate HyperNode/Node
selected Domain/Fabric
hard/soft mode
fit reason
placement state phase
provider epoch/fingerprint
~~~

Exact 阶段再增加：

~~~text
PlanID
ReservationToken
AdapterName
DeviceKeys
EvidenceRef
reconciliation phase
~~~

这些信息优先通过 Volcano 已有 Job/PodGroup condition、reason/message、recorder
和插件 metrics 暴露，不把高频热状态写入 workload spec。

## 11. 验证计划

### 11.1 对照现有 Network Topology

先验证不改变已有行为：

- 未配置 xPU policy 时现有 Network Topology 测试结果不变；
- HyperNodeInfo 没有新增 Device/Reservation 字段；
- Job/SubJob 仍由现有 JobInfo/SubJobInfo 创建和维护；
- HyperNode gradient 仍由 network-topology-aware 负责；
- Statement 的 Save/Recover/Discard 路径仍然有效。

### 11.2 xPU MVP 测试

| 测试 | 期望 |
| --- | --- |
| 单 Node 两 Domain free=6+2，请求 8，hard | 失败，不能跨 Domain 拼接 |
| 单 Node 一个 Domain free=8，请求 8 | 成功，所有 Device 来自同一 Domain |
| 两个等价 Domain | 稳定 tie-break，重复运行结果一致 |
| soft + Domain 不满足 | 保留普通候选，记录退化原因 |
| Statement Allocate callback 失败 | 当前 Task 和 xPU Session overlay 回退 |
| winning Statement Discard | 所有本轮 tentative state 回退 |
| JobReady 后仍有 pending Task | Job 重新入队，后续 Task 参考 placement state |
| 没有 active allocated member | placement state 清理 |

### 11.3 Exact/增强测试

只有进入增强阶段后才测试：

- exact Device ID 是否被 Adapter 严格执行；
- 后续 admission 是否继续使用相同 Domain/Fabric；
- fingerprint 变化后的整组 replan；
- Prepare/Abort/Release 失败后的 reconciliation；
- scheduler restart 后 evidence 恢复；
- 两个 Session/leader 是否会同时占用同一 Device ID；
- PreBind 失败时是否尚未开始 Kubernetes Bind。

Mock Adapter 只能证明调度语义和事务顺序，不能替代真实 Device Plugin、DRA driver
或厂商运行时的 exact-ID 执行证据。

## 12. 分阶段交付

### 12.1 Phase 1：复用现有框架

- 只增加 xPU canonical model、Provider、Normalizer 和插件私有 cache；
- 复用 JobInfo/SubJobInfo 的 Group 作用范围；
- 复用 HyperNode candidate gradient/order；
- 复用 Statement Allocate/Discard/Commit；
- 不引入独立 TopologyGroup、AdmissionSet transaction 或 Anchor resource；
- 用 Mock/Annotation 验证 Node-local Domain filter/score。

### 12.2 Phase 2：DeviceDomain placement state

- 增加 Job/SubJob 或插件私有的 AllocatedDomain/Fabric state；
- 让后续 Task 参考已有 placement；
- 复用现有 Session Open 的恢复/清理模式；
- 对 Node 可推导的 placement，允许从已绑定 Pod Node 恢复；
- 对 Node 内 Domain，明确需要 membership/evidence 才能恢复。

### 12.3 Phase 3：exact-ID Adapter

- 增加 TaskResourceAssignment 和 Adapter capability；
- 明确 Exact 与 BestEffort；
- Exact 不可执行时 hard fail closed；
- 仅在需要时增加 Statement 提交前 hook；
- 保持 Kubernetes Binding 非原子边界。

### 12.4 Phase 4：跨波次 Fabric 与 durable evidence

- 跨 Node Fabric 的显式 membership；
- 跨波次 admission 继续满足已选择的 Fabric；
- Adapter/Claim/evidence 的 fencing 和 reconciliation；
- scheduler restart 后的 hard recovery；
- 必要时增加 group pre-bind gate。

## 13. 兼容性与开放问题

### 13.1 兼容原则

1. 未配置 xPU topology 时零行为变化；
2. 不修改 HyperNode 的公共语义来承载 Device state；
3. 不增加第二套 Job/SubGroup readiness 语义；
4. 不把 tierName、Device ID、Domain ID 和 reservation 暴露成 workload desired state；
5. 不把 Mock/Annotation 或 Device Plugin 数量接口写成真实 exact-ID 能力；
6. 不把 Kubernetes Binding 写成整组原子提交。

### 13.2 需要确认的问题

以下问题影响增强阶段，但不阻塞 MVP：

- Node-local DeviceDomain placement state 放在 JobInfo/SubJobInfo，还是插件私有 map；
- 第一条真实 Exact Adapter 选择 HAMi/vGPU、DRA 还是其他厂商机制；
- Anchor/evidence 的持久载体是 PodGroup status、受控 annotation、Claim 还是 Adapter record；
- 多 Container、共享/分数 Device 的 assignment 模型是否进入后续范围；
- 是否需要跨多个 scheduler 实例共享 durable reservation。

如果这些问题尚未决定，MVP 不应假设跨进程的强 CAS、exact crash recovery 或
Kubernetes group binding atomicity。

## 附录 A. 最小内部 API 草案

以下类型只表达 V2 的边界，不代表最终 Public API：

~~~go
type GroupRef struct {
    JobID    api.JobID
    SubJobID api.SubJobID
}

type DeviceKey struct {
    ResourceName corev1.ResourceName
    NodeUID      types.UID
    ProviderID   string
}

type DomainKey struct {
    ResourceName corev1.ResourceName
    Scope        DeviceDomainScope
    NodeUID      types.UID
    ProviderID   string
}

type DevicePlacementState struct {
    GroupRef          GroupRef
    ResourceName      corev1.ResourceName
    DomainKeys        []DomainKey
    FabricKeys        []DomainKey
    PolicyFingerprint string
    EvidenceRef       string
    Phase             PlacementPhase
}
~~~

建议的 Public API 只表达：

~~~text
resourceName
applyTo: Pod | Group
mode: hard | soft
domain.scope: Node | Fabric
domain.tier 或 tierName
podSelector（仅适用于 Pod 作用范围）
~~~

Public API 不表达：

~~~text
selected Node/Domain/Fabric
Device ID
Health/Free/Reserved/Allocated
Snapshot revision
Reservation token
Adapter name
~~~

## 附录 B. 当前源码证据索引

| 能力 | 当前源码 |
| --- | --- |
| HyperNode 结构 | pkg/scheduler/api/hyper_node_info.go:86 |
| Job/SubJob topology state | pkg/scheduler/api/job_info.go、pkg/scheduler/api/sub_job_info.go |
| Session snapshot 和 placement recovery | pkg/scheduler/framework/session.go:250、:328、:372 |
| Job/SubJob readiness | pkg/scheduler/framework/session_plugins.go:424、:482 |
| Statement operations | pkg/scheduler/framework/statement.go:53、:256、:375、:402 |
| Save/Recover/Merge | pkg/scheduler/framework/statement.go:427 |
| Job-level allocation | pkg/scheduler/actions/allocate/allocate.go:357、:426 |
| SubJob/task allocation | pkg/scheduler/actions/allocate/allocate.go:521、:775 |
| HyperNode gradient/order/callback | pkg/scheduler/plugins/network-topology-aware/network_topology_aware.go:285 |
| Per-Task bind queue | pkg/scheduler/cache/cache.go:1364 |

## 附录 C. 设计关系与状态边界

~~~text
当前实现
  HyperNode CRD/HyperNodeInfo
  JobInfo/SubJobInfo NetworkTopology + AllocatedHyperNode
  network-topology-aware gradient/order/callback
  gang JobReady/SubJobReady
  Statement Allocate/Discard/Commit
  AddBindTask per Task

V2 可复用设计
  Job/SubJob 作为 xPU Group 作用范围
  HyperNode 只做 Node/network candidate
  xPU plugin private Device/Domain cache
  Statement 作为本轮 operations carrier
  AllocatedDomain/Fabric placement state

后续研究/增强
  exact Device ID Adapter
  durable evidence/fencing
  cross-wave Fabric anchor
  group Prepare/PreBind gate
  scheduler crash reconciliation
~~~

本文不把“V2 设计存在”写成“上述增强已经在 Volcano 中实现”。
