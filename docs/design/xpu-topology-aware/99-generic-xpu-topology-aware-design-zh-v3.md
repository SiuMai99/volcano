# Volcano 通用 xPU 拓扑感知调度设计（收敛版 V3）

> 状态：结构化设计提案，不代表 Volcano 已实现或社区已接受。
>
> 本文以 [V2](./99-generic-xpu-topology-aware-design-zh-v2.md) 为基线，吸收社区方案和评审中已经明确的方向，
> 收敛 Node 身份、Session Snapshot、Workload API、Annotation、策略更新、抢占释放、Alpha 请求边界、
> Fabric 发布以及组事务语义。
>
> 关联议题：[volcano-sh/volcano#5751](https://github.com/volcano-sh/volcano/issues/5751)
>
> 关联社区提案：[volcano-sh/volcano#5965](https://github.com/volcano-sh/volcano/pull/5965)

## 1. V3 结论与保证边界

### 1.1 相对 V2 的九项收敛决定

| # | V3 决定 | 对 V2 的变化 |
| --- | --- | --- |
| 1 | 所有 Node-scope canonical key 都纳入 `NodeUID`；分别定义 source ID、canonical ID 和 scheduler key | 不再把 `NodeName` 当稳定 owner，也不再用含义模糊的 `ProviderID` 代替设备值 |
| 2 | `*DeviceTopologySnapshot` 直接进入 `api.ClusterInfo`，由一次 `SchedulerCache.Snapshot()` 捕获 | 删除“Cluster snapshot 与 topology snapshot 分开读取再重试”的方向 |
| 3 | Public API 保留 `hard/soft`，不暴露 `allocationStrategy` | 内部固定使用 deterministic Compact，不把尚无替代值的策略冻结成 API |
| 4 | 增加 Pod Annotation authoring 入口，但 controller 必须把它转换成 `PodGroup.spec.deviceTopology` | scheduler 只消费一个 canonical spec；VCJob、直接 PodGroup 与 Pod Annotation 不形成三套语义 |
| 5 | 允许策略在无活动状态时更新；活动 reservation、anchor 或 allocation 出现后语义更新 fail closed | 补充 policy fingerprint、更新闸门和 PodGroup Condition/Reason 写回路径 |
| 6 | 仅把 topology-aware victim selection 延后 | eviction、release、取消 eviction 和不确定结果的 ledger 正确性属于 Alpha 必做项 |
| 7 | Alpha 仅接受一个普通 Container 对目标扩展资源提出正整数整卡请求 | Exact assignment 必须记录 `PodUID + ContainerName + ResourceName + DeviceKeys`，每个 key 携带 canonical `DeviceID` |
| 8 | Annotation Fabric 由一个 owner Node 发布完整成员集合 | 删除多个成员 Node 分片发布同一 Fabric 后做 version-vector merge 的 Alpha 路径 |
| 9 | framework API 名称保持开放，但事务语义不可删减 | checkpoint、全组 PreBind、零提前 Bind、幂等补偿和显式 reconciliation 都是 Exact Alpha 验收项 |

### 1.2 一条主路径，而不是平行调度器

~~~text
VCJob typed field / direct PodGroup spec / Pod annotations
  -> controller canonicalization
  -> PodGroup.spec.deviceTopology（scheduler 唯一策略真源）
  -> SchedulerCache.Snapshot（ClusterInfo + immutable DeviceTopologySnapshot）
  -> existing HyperNode / Predicate / Queue / Gang candidate path
  -> xPU Domain/Fabric filter and deterministic Compact score
  -> one complete AdmissionSet plan
  -> Statement tentative Allocate
  -> final ledger reservation + exact Adapter preparation
  -> durable GroupPlacementAnchor/evidence
  -> complete-batch PreBind gate
  -> per-Pod Kubernetes Bind
  -> authoritative Allocated / Released / Unknown reconciliation
~~~

V3 不建立第二套 Job、SubGroup、readiness 或调度循环。`TopologyGroup`、`AdmissionSet` 和
`GroupPlacementAnchor` 是附着于现有 Job/SubJob/Statement 的内部语义，不是新的 CRD 或平行事务框架。

### 1.3 两级能力，禁止混写保证

| 能力级别 | 可以承诺 | 不能承诺 |
| --- | --- | --- |
| Advisory MVP | Annotation/Mock topology ingestion、immutable snapshot、Node-local Domain filter 数据、`soft` score、deterministic Compact、结构化 reason | 运行时一定使用 scheduler 选中的 Device ID；`hard` 自动降级；多 Pod 原子绑定 |
| Exact Alpha | compatible Adapter 能执行选中 ID；`hard` fail closed；整组 reserve/prepare/prebind barrier；显式 reconciliation；重启恢复前阻塞新 `hard` 调度 | Kubernetes API 级多 Pod 原子绑定；未验证的 native Device Plugin exact-ID；topology-aware victim selection |

没有 compatible exact Adapter 时：

- `soft` 可以作为 advisory score，且不创建伪造的 per-device owner；
- `hard` 必须返回 `XPUAssignmentNotEnforceable`；
- 管理员配置不得把 `hard` 静默改为 `soft`。

Mock Adapter 可以验证规划和事务顺序，但不能作为真实硬件 exact-ID 执行证据。若发布“可用的 Exact Alpha”，
必须至少有一个真实 Adapter 完成选中 ID、恢复、释放和失败注入验收。

### 1.4 文档中的四种状态

后文使用下列标签，避免把提议写成当前实现：

- **当前实现**：仓库中已经存在且本设计核对过的行为；
- **可复用能力**：已有组件可以继续承担的所有权；
- **本文提议**：V3 要新增或修改的合同；
- **后续研究**：不进入首个 Exact Alpha 验收路径的能力。

## 2. 当前实现、可复用能力与缺口

### 2.1 当前实现

当前 Volcano 已经具有：

- `HyperNodeInfo` 的 Node/网络层次，以及 `network-topology-aware` 的 gradient/order；
- `JobInfo`、`SubJobInfo` 对 PodGroup/SubGroup policy、readiness 和 `AllocatedHyperNode` 的保存；
- `framework.OpenSession()` 调用 `SchedulerCache.Snapshot()` 构造一次 Session；
- `Statement.Allocate()`、`Discard()`、`SaveOperations()`、`RecoverOperations()` 和 `Commit()`；
- `SchedulerCache.AddBindTask()`、`executePreBinds()` 和逐 Pod `Bind()`；
- `Session.UpdatePodGroupCondition()`，以及 Session close 时 `JobUpdater.UpdateAll()` 到
  `SchedulerCache.UpdateJobStatus()` 的状态回写；
- PodGroup controller 从 Pod Annotation 构造或更新自动 PodGroup，以及 Deployment、ReplicaSet、StatefulSet、
  bare Pod 的现有 controller 路径。

当前源码边界必须如实记录：

1. `api.ClusterInfo` 尚无 `DeviceTopologySnapshot`；
2. `SchedulerCache.Snapshot()` 在 `SchedulerCache.Mutex` 下克隆 Nodes/Jobs/Queues 和 HyperNode 视图；
3. `Statement.Commit()` 无错误返回，按 operation 逐个调用 `AddBindTask()`；
4. `AddBindTask()` 是单 Task 接口；
5. `executePreBinds()` 对一个 BindContext 失败后仍可能绑定同 batch 中其他成功项；
6. 当前 Pod Annotation 转换只覆盖 NetworkTopology，并且无效 mode 会回落到默认值，不能直接照搬为 xPU hard policy；
7. 当前 `UpdatePodGroupCondition()` 按 `Condition.Type` 覆盖，不支持同时保留多个 `Unschedulable` writer 的独立记录。

因此，现有路径不是 Exact Alpha 所需的全组提交屏障。

### 2.2 可复用能力

| 现有组件 | 继续拥有 | xPU 不应接管 |
| --- | --- | --- |
| Queue/DRF/capacity/gang | 队列、资源公平性、Job/SubJob readiness | Device identity 和 exact allocation |
| HyperNode/network-topology-aware | Node/网络层次、network gradient 和 score | Device、Domain、Health、Reservation |
| Predicate/NodeInfo | Kubernetes Node 级资源、taint、volume、port 等可行性 | 单个 Domain 内的 Device membership |
| JobInfo/SubJobInfo | workload/group 上下文和 Task membership | Provider payload 与全局 Device owner |
| Statement | 本轮 Task/Node speculative mutation 的 owner | Provider 解析与硬件事实 |
| SchedulerCache | 集群 live state、一次 Session snapshot、bind handoff | 厂商 runtime 分配协议 |
| Adapter | 选中 ID 的真实 reservation/assignment/release observation | Domain/Fabric 规划 |

### 2.3 本文提议的新增所有权

~~~mermaid
flowchart LR
    Sources["Trusted topology publisher"] --> Provider["Provider parse and normalize"]
    Provider --> Live["Scheduler-owned topology live cache"]
    Live --> Snapshot["ClusterInfo.DeviceTopology<br/>immutable session view"]
    Snapshot --> Plugin["xPU filter, score and plan"]
    Plugin --> Statement["Existing Statement + one group context"]
    Statement --> Ledger["Reservation ledger"]
    Ledger --> Adapter["Exact allocation adapter"]
    Statement --> Batch["Complete batch PreBind gate"]
    Batch --> Bind["Per-Pod Kubernetes Bind"]
    Adapter --> Observe["Allocated / Released / Unknown"]
    Observe --> Live
~~~

关键所有权如下：

- Provider 只拥有 source ingestion 和 validation；
- topology live cache 拥有 canonical facts、readiness、indexes 和 allocation ledger；
- `ClusterInfo.DeviceTopology` 是 Session 的不可变只读视图；
- xPU plugin 只做 policy compile、filter、score、plan；
- Statement 仍拥有普通 Task/Node mutation，并额外携带唯一的 group topology context；
- Adapter 是真实分配系统唯一的 exact owner；topology ledger 只是 scheduler-side concurrency guard。

### 2.4 后续研究

下列能力不进入首个 Exact Alpha：

- DRA `claimName` Public API 和 ResourceSlice Provider；
- MIG、vGPU、共享/分数设备、多 Container、Init Container 的 allocation lifecycle；
- topology-aware victim selection 和 device-level pipeline 持久化；
- 自动推导 Fabric、NCCL ring、厂商 link-bandwidth 规划；
- 无共享持久 reservation record 时的 active-active 多 scheduler exact reservation；

## 3. Workload API 与唯一 canonicalization 路径

### 3.1 Public API

Public API 只表达 workload intent：

~~~go
type DeviceTopologyMode string

const (
    HardDeviceTopologyMode DeviceTopologyMode = "hard"
    SoftDeviceTopologyMode DeviceTopologyMode = "soft"
)

type DeviceTopologyApplyTo string

const (
    DeviceTopologyApplyToPod   DeviceTopologyApplyTo = "Pod"
    DeviceTopologyApplyToGroup DeviceTopologyApplyTo = "Group"
)

type DeviceTopologyDomainScope string

const (
    DeviceTopologyDomainScopeNode   DeviceTopologyDomainScope = "Node"
    DeviceTopologyDomainScopeFabric DeviceTopologyDomainScope = "Fabric"
)

type DeviceTopologyDomainSelector struct {
    Scope    DeviceTopologyDomainScope `json:"scope"`
    Tier     *int32                    `json:"tier,omitempty"`
    TierName string                    `json:"tierName,omitempty"`
}

type DeviceTopologyPolicy struct {
    ResourceName corev1.ResourceName          `json:"resourceName"`
    ApplyTo      DeviceTopologyApplyTo        `json:"applyTo,omitempty"`
    Mode         DeviceTopologyMode           `json:"mode,omitempty"`
    Domain       DeviceTopologyDomainSelector `json:"domain"`
    PodSelector  *metav1.LabelSelector        `json:"podSelector,omitempty"`
}

type DeviceTopologySpec struct {
    Policies []DeviceTopologyPolicy `json:"policies,omitempty"`
}
~~~

建议在 `PodGroupSpec` 和 `SubGroupPolicySpec` 增加 `DeviceTopology *DeviceTopologySpec`；VCJob 可增加 ergonomic
字段，但 controller 必须把它转换到生成的 PodGroup/SubGroup canonical spec。scheduler 不直接读取 VCJob 或 Pod Annotation。
`hard/soft` 刻意与现有 `NetworkTopologyMode` 术语保持一致，不另造 `Required/Preferred` 的同义 API。

默认和校验规则：

- `mode` 默认 `hard`；`applyTo` 默认 `Pod`；
- `mode` 只接受 `hard/soft`；无效值必须拒绝，不能默认成 `hard`；
- `tier` 与 `tierName` 互斥且至少设置一个；
- `scope=Fabric` 只匹配 Provider 明确发布的 Fabric；HyperNode 或 IB/RoCE 可达性不能隐式满足；
- `podSelector` 只允许在 PodGroup 级 `applyTo=Pod`；
- 同一作用单元的 hard policies 使用 AND；soft policies 只累计有证据的 preference；
- 同一作用单元不得对同一 `resourceName + applyTo + scope` 提交互相矛盾的 policy；
- 未配置 `DeviceTopology` 时，当前调度行为不变。

### 3.2 Public API 不暴露 allocationStrategy

V3 删除首版 Public `allocationStrategy`/`allocationPolicy`。内部唯一策略是 deterministic Compact：

1. 先按稳定 key 排序 Fabric、Node、Domain 和 Device；
2. 优先 exact fit；
3. 再选能容纳请求的最小 Domain；
4. Group plan 优先使用更少的 Domain 和 Node；
5. 再最小化剩余碎片；
6. 所有 score 相同则按 `FabricKey -> NodeUID -> DomainKey -> DeviceKey` 字典序决胜。

Compact 是确定性的内部选择/soft preference，不是绕过 hard constraint 的理由。未来只有在 Spread/Any 的语义、兼容性和
升级规则确定后，才单独进行 Public API review。

Public API 同样不暴露：Device/Domain/Fabric ID、selected Node、Health、AllocationState、snapshot revision、
reservation token、Adapter name 或 Provider payload。

### 3.3 Pod Annotation authoring contract

本文提议一个 workload annotation：

~~~text
scheduling.volcano.sh/device-topology
~~~

其值是 versioned JSON，字段与 `DeviceTopologySpec` 一一对应。它只是 authoring 入口，不是 scheduler API，也不是
运行时 assignment carrier。controller 解析后必须经过与 typed field 相同的 default、validation、stable sort 和
canonical serialization，再写入 `PodGroup.spec.deviceTopology`。

~~~json
{
  "apiVersion": "scheduling.volcano.sh/v1alpha1",
  "spec": {
    "policies": [
      {
        "resourceName": "nvidia.com/gpu",
        "applyTo": "Group",
        "mode": "hard",
        "domain": {"scope": "Fabric", "tierName": "scale-up-fabric"}
      }
    ]
  }
}
~~~

未知 `apiVersion`、未知字段、重复 JSON key、超过大小上限或非 canonical 类型必须拒绝，不能由宽松 JSON 解码静默丢弃。

~~~mermaid
flowchart TD
    Job["VCJob typed field"] --> JobCtl["Job controller"]
    Direct["Direct PodGroup typed spec"] --> Canonical["PodGroup.spec.deviceTopology"]
    Pod["Pod or Pod template annotation"] --> PGCtl["PodGroup controller"]
    JobCtl --> Canonical
    PGCtl --> Canonical
    Canonical --> Scheduler["Scheduler reads only canonical spec"]
    Pod -. "never read as parallel policy" .-> Scheduler
~~~

支持路径：

| 创建方式 | 转换责任 | canonical PodGroup |
| --- | --- | --- |
| VCJob | Job controller 复制、默认化并校验 typed field | VCJob 生成的 PodGroup |
| 直接 PodGroup + member Pods | 用户直接提交 typed spec；webhook 校验 | 用户提交的 PodGroup |
| Deployment/ReplicaSet | Pod template annotation 进入实际 Pod；PodGroup controller 解析 | owner 对应的自动 PodGroup |
| StatefulSet | Pod template annotation 进入实际 Pod；PodGroup controller 解析 | StatefulSet 对应的自动 PodGroup |
| bare Pod | PodGroup controller 解析；`minMember=1` | 该 Pod 的自动 PodGroup |

### 3.4 Authority、优先级与冲突

authoring source 的 authority 顺序是：

~~~text
已存在的 canonical PodGroup typed spec
  > owner VCJob typed spec
  > Pod / Pod-template annotation（只用于尚无 typed policy 的自动 PodGroup）
~~~

“优先级”只决定谁有权物化 canonical spec，不表示高优先级可以静默覆盖低优先级的不同语义。controller/webhook 对所有
同时存在的非空 source 做 canonical fingerprint 比较：

- 只有一个 source：接受并物化；
- 多个 source 规范化后相同：接受一次，不产生第二份策略；
- 多个 source 规范化后不同：fail closed，返回或写入 `XPUTopologyPolicyConflict`；
- 一个 PodGroup 中不同 member Pod 的 annotations 不同：整个 PodGroup fail closed；
- member Pod annotation 与直接 PodGroup typed spec 不同：不得覆盖 PodGroup；
- VCJob typed field 与生成 Pod/PodGroup 中的 annotation 不同：不得由子对象覆盖 owner intent；
- 未通过解析/校验的 annotation：不得创建一个默认 hard policy，也不得让 scheduler 忽略它继续普通调度。

冲突解除前，xPU 的 `JobValid/JobEnqueueable` gate 必须识别 controller 写入的
`PodGroupUnschedulable=True, Reason=XPUTopologyPolicyConflict`，即使 canonical spec 为空或仍是旧值也保持 Group Pending。
这个 Condition 只是 fail-closed validation gate，不是第二份 policy；scheduler 的 topology 语义仍只来自 canonical spec，
并且绝不读取 Pod workload annotation。Workload annotation 不能携带 Device ID、reservation token、Adapter handoff
或任何调度结果。

### 3.5 Policy 更新合同

每份 canonical spec 计算 `PolicyFingerprint`。规范化必须包括 default 后的字段、排序后的 policies 和 label selector，
但不能包括对象 `resourceVersion` 等存储元数据。

语义更新只在 Group 处于 quiescent 状态时允许：

~~~text
没有 TentativeReserved / Held / Binding / Allocated / ReconcilePending owner
AND 没有 active GroupPlacementAnchor 或未决 evidence
AND 没有已经进入 bind handoff 的成员
~~~

规则如下：

1. quiescent 时允许更新；新 fingerprint 使旧 plan、fit cache 和 placement hint 全部失效；
2. 活动状态出现后，只允许 canonical fingerprint 完全相同的 no-op 更新；
3. 活动状态下修改 `mode/resource/scope/tier/applyTo/selector` 必须由 webhook 或 controller 拒绝；
4. 删除 policy 也是语义更新，不能借删除绕过 drain；
5. VCJob 和生成 PodGroup 必须使用 UID/resourceVersion CAS 保持同一 fingerprint；
6. Pod template rollout 产生不同 annotations 时，旧 PodGroup 不原地切换策略；controller 应创建新的作用单元或要求用户先 drain；
7. 不能确认是否存在外部分配时按“活动”处理，而不是允许更新。

## 4. ID、Node 重建与 tombstone

### 4.1 三层名称不能混用

V3 明确区分：

| 层 | 示例 | 语义 |
| --- | --- | --- |
| Provider identity | `annotation-v1` | 哪个启用的 source contract 发布事实 |
| Source-local value | `GPU-4c2e`、`nvlink-0`、`fabric-0` | payload 中稳定但有明确作用域的值 |
| Canonical scheduler key | 下面定义的 `DeviceKey/LocalDomainKey/FabricKey` | cache、plan、ledger 和 fingerprint 使用的完整身份 |

`ProviderID` 绝不能被当成 `devices[].id`；`devices[].id` 也不能脱离 NodeUID、resource 和 identity namespace
直接成为 scheduler map key。

### 4.2 DeviceID、DomainID 与 FabricID

~~~go
// Source value types are opaque values from one validated provider contract.
type SourceDeviceID string
type SourceDomainID string
type SourceFabricID string

// DeviceID is the allocator-facing identity understood by a paired Adapter.
// Value comes from devices[].id; ProviderID and Namespace come from trusted config.
type DeviceID struct {
    ProviderID string
    Namespace  string
    Value      SourceDeviceID
}

// DeviceKey is the scheduler identity of a Node-owned allocatable device.
type DeviceKey struct {
    ResourceName corev1.ResourceName
    OwnerNodeUID types.UID
    ID           DeviceID
}

// DomainID is a provider-normalized Node-local domain identity.
// It is not a global key and is distinct from FabricID.
type DomainID struct {
    ProviderID string
    Namespace  string
    Value      SourceDomainID
}

type LocalDomainKey struct {
    ResourceName corev1.ResourceName
    OwnerNodeUID types.UID
    ID           DomainID
}

// FabricID is cluster-scoped only within one provider identity contract and resource.
type FabricID struct {
    ProviderID   string
    Namespace    string
    ResourceName corev1.ResourceName
    Value        SourceFabricID
}

type FabricKey = FabricID
~~~

唯一性和稳定性合同：

- `SourceDeviceID` 在一个 `ProviderID + Namespace + NodeUID + ResourceName` 内唯一；
- `DeviceID` 是 Adapter identity contract 下的 normalized value；它自身不是 scheduler map key，两个 Node 上可以相等；
- `DeviceKey` 因包含 `OwnerNodeUID`，不会把同名重建 Node 上的 `device0` 误认成旧设备；
- `SourceDomainID` 在一个 `ProviderID + Namespace + NodeUID + ResourceName` 内唯一；
- `DomainID` 表示 Node-local domain 的 provider-normalized identity；它必须与 `OwnerNodeUID + ResourceName`
  组合成 `LocalDomainKey` 才能作为 scheduler key；
- `LocalDomainKey` 是 Node-scope Domain 的唯一 canonical key；
- `SourceFabricID` 在一个 `ProviderID + Namespace + ResourceName` 内唯一；
- `FabricKey` 不包含 member NodeName/UID，但 canonical Fabric object 必须保存 owner UID 和全部已解析 member UID/generation；
- `tierName` 是非唯一分类合同，不是 Domain identity；
- source generation、fabric generation 和 snapshot revision 是版本，不是 ID；
- 数组下标、可漂移 GPU index、NodeName、HyperNodeName 都不能单独作为 Device/Domain/Fabric 身份。

NodeName 只用于查询、日志和 Kubernetes Bind；所有 owner、membership、plan 和 ledger 比较使用 NodeUID。

两个 Node 都可以发布 `devices[].id="device0"` 和 `localDomains[].id="domain0"`。只要它们的 NodeUID 不同，
`DeviceKey` 和 `LocalDomainKey` 就不同；这不是重复 ID 错误。相反，同一 identity contract 和 resource 下的
`fabrics[].id="fabric0"` 必须唯一，并由唯一 owner 发布。

### 4.3 Canonical topology objects

~~~go
type NodeIdentity struct {
    Name            string
    UID             types.UID
    ResourceVersion string
}

type TopologyDevice struct {
    Key                   DeviceKey
    NodeName              string // diagnostic/bind lookup only
    LocalDomainKeys       []LocalDomainKey
    Health                DeviceHealth
    SourceGeneration      uint64
    MembershipFingerprint string
}

type DeviceDomain struct {
    Key                   LocalDomainKey
    NodeName              string // diagnostic only
    Tier                  int32
    TierName              string
    MemberDeviceKeys      []DeviceKey
    MemberDomainKeys      []LocalDomainKey
    EffectiveDeviceKeys   []DeviceKey
    SourceGeneration      uint64
    MembershipFingerprint string
}

type FabricMember struct {
    NodeName         string // diagnostic only
    NodeUID          types.UID
    SourceGeneration uint64
    LocalDomainKeys  []LocalDomainKey
}

type FabricDomain struct {
    Key                   FabricKey
    OwnerNodeUID          types.UID
    SourceGeneration      uint64
    Members               []FabricMember
    MembershipFingerprint string
}
~~~

`MemberDeviceKeys/MemberDomainKeys` 是 source-normalized 的显式 membership；`EffectiveDeviceKeys` 是 Normalizer 校验
无环和去重后派生的 closure。一个 Device 可以属于多个不同 tier 的重叠 local Domains。Fabric membership 必须显式指向
member local Domains，不能从 Node/HyperNode 关联推断。

可选 `DeviceLink{Source, Target, Score, LinkClass}` 只用于有证据的 soft score/诊断。缺边、非对称边或厂商 link 名称不能
推导 hard Domain；首个 Alpha 的 hard 正确性必须在完全没有 Link 数据时仍成立。

### 4.4 Node 删除和同名重建

当 `node-a` 的 UID 从 `uid-old` 变为 `uid-new`：

1. 在 `SchedulerCache.Mutex` 下先把 `uid-old` 从 candidate indexes 移除；
2. 旧 UID 的 provider readiness 不得转移给新 UID；新 UID 从 `Pending` 开始；
3. 旧 UID 的 free facts 可以删除，但仍有 `Held/Binding/Allocated/ReconcilePending` owner 的 key 保留为 unavailable tombstone；
4. 旧 UID tombstone 只有收到 authoritative `Released` 后才能删除；
5. 新 UID 不继承旧 UID 的 source generation、Domain membership、Health 或 Free 状态；
6. 迟到的旧 UID `ReplaceFacts/ClearFacts` 即使 NodeName 相同也必须拒绝；
7. 新 Node 完成一次有效 `ReplaceFacts` 或 `ClearFacts` 后，readiness 才从 `Pending` 进入 `Synced`；
8. 包含旧 UID 的 Fabric 立即不可用，owner 必须以更新 generation 重新发布并解析完整成员集合；
9. 不能把旧 UID 的 allocation evidence 映射到新 UID。

~~~mermaid
stateDiagram-v2
    state replacement <<fork>>
    [*] --> OldActive: Node uid-old observed
    OldActive --> OldTombstone: Node deleted
    OldActive --> replacement: same name uid-new observed
    replacement --> OldTombstone: retain old active owners only
    replacement --> NewPending: create new identity
    OldTombstone --> [*]: authoritative Released for all old owners
    NewPending --> NewSynced: valid ReplaceFacts or ClearFacts
    NewPending --> NewPending: invalid or delayed old-UID update
~~~

## 5. Provider、Fabric 与 Cache

### 5.1 Provider update contract

~~~go
type ProviderNodeUpdate struct {
    ProviderID          string
    IdentityNamespace   string
    NodeName            string
    NodeUID             types.UID
    NodeResourceVersion string
    SourceGeneration    uint64
    ObservedAt          time.Time
    FreshUntil          time.Time
    Operation           ProviderUpdateOperation // ReplaceFacts | ClearFacts
    Facts               *NodeTopologyFacts
}
~~~

Provider 负责 source-specific parse 和 normalize，不提供 `FilterNode/ScoreNode/Allocate`。每个
`ProviderID + NodeUID` 的 readiness 是 `Pending | Synced`：

- `ReplaceFacts` 是该 source 对当前 Node 的完整事实替换；
- `ClearFacts` 表示 Provider 已检查该 Node 且没有 inventory，它可以完成初次同步；
- invalid payload 不能完成初次同步，也不能覆盖最后一次有效事实；
- 同一 NodeUID 的 source generation 对 topology content 单调；相同 generation 只允许内容完全相同的 heartbeat；
- Kubernetes `resourceVersion` 是 opaque 字符串，只做“是否仍等于当前观察对象”比较，不做数值排序；
- Provider 时间戳用于 freshness，不用于 Node update 排序。

Annotation payload 由受信 publisher 写入 Node；workload ServiceAccount 和 scheduler ServiceAccount 不得有该 key 的写权限。
由于 Node patch RBAC 不能限制单个 annotation key，部署必须使用 ValidatingAdmissionPolicy 或 webhook 对 publisher identity
做 fail-closed 限制。

### 5.2 Annotation Fabric：单 owner、完整声明

Alpha 的一个 `FabricKey` 只有一个 authoritative owner Node。owner Node 的 annotation 必须发布该 Fabric 的完整集合：

~~~json
{
  "id": "rack-1/fabric-0",
  "resourceName": "nvidia.com/gpu",
  "ownerNode": "node-a",
  "members": [
    {"node": "node-a", "localDomains": ["nvlink-0"]},
    {"node": "node-b", "localDomains": ["nvlink-0"]}
  ]
}
~~~

接受规则：

1. `ownerNode` 必须等于承载 annotation 的 NodeName，cache 用实际 NodeUID 覆盖任何 payload owner metadata；
2. owner 必须出现在 members 中；members 是完整集合，不是增量 patch；
3. member Node 只发布自己的 local devices/domains，不得重复声明同一 `FabricKey`；
4. cache 只有在每个 member Node 已 `Synced`，且每个 local Domain 能解析到当前 NodeUID 和 source generation 时才发布 Fabric；
5. canonical Fabric 保存 `OwnerNodeUID` 和每个 member 的 `NodeUID + SourceGeneration + LocalDomainKeys`；
6. owner 的新 generation 可以完整替换成员；被删除成员仍有 active owner 时保留 tombstone，不进入 candidate index；
7. owner 删除、换 UID、stale 或 ClearFacts 会立即使 Fabric 对新 hard plan 不可用；
8. owner identity 变更不得无条件接管同一个 Fabric ID。必须先显式清除旧声明、处理完旧 allocation/tombstone，再由新 owner 发布；
9. member Node 换 UID 后旧 Fabric 不自动复活，owner 必须发布更新 generation；
10. 普通 Ethernet、IB、RoCE 或同一 HyperNode 关系不构成 xPU Fabric membership。

这取代早期草案中“每个成员 Node 发布局部 `fabricMembership` 再合并 version vector”的 Annotation Alpha 路径。
未来 cluster-scoped Fabric CRD/Provider 可以采用不同 authority，但必须输出同一 canonical `FabricDomain`。

### 5.3 Canonical facts 与 allocation ledger 分离

~~~text
Topology facts
  Device/Domain/Fabric identity, membership, source generation, freshness, observed health

Allocation ledger
  Free, TentativeReserved, Held, Binding, Allocated, ReconcilePending, owner/evidence

Session view
  ClusterInfo 中的一份 immutable DeviceTopologySnapshot + Session tentative overlay
~~~

`Health` 与 `AllocationState` 正交。只有：

~~~text
Health == Healthy && AllocationState == Free && ProviderReadiness == Synced
~~~

才是 `SchedulableFree`。已分配设备变为 Unhealthy 仍保留 owner；source 中删除一个仍有 owner 的 Device 只会创建 tombstone，
不会把它变成 Free。

### 5.4 DeviceTopologySnapshot 进入 ClusterInfo

**本文提议**在 `api.ClusterInfo` 增加：

~~~go
type ClusterInfo struct {
    // existing fields omitted
    DeviceTopology *DeviceTopologySnapshot
}
~~~

并在 `framework.Session` 保存同一指针或其只读引用。所有 map、slice、set 和对象在 publish 后不可变；下一次 Provider/ledger
更新必须构建新对象并原子交换 pointer，不能修改已被 Session 引用的数据。

`SchedulerCache.Snapshot()` 在一次主锁临界区中完成：

~~~go
func (sc *SchedulerCache) Snapshot() *api.ClusterInfo {
    sc.Mutex.Lock()
    defer sc.Mutex.Unlock()

    snapshot := cloneClusterStateLocked(sc)
    snapshot.DeviceTopology = sc.topologyCache.PublishedSnapshot()
    return snapshot
}
~~~

这里的 `PublishedSnapshot()` 只 atomic-load 已发布的 immutable pointer，不获取 `topologyCache.mu`，也不执行 parse、graph
closure、Adapter RPC 或全量 clone。
它保证 Nodes/Jobs/Queues 与 DeviceTopology 来自同一次 `SchedulerCache.Snapshot()` 的观察点，因此：

- 不需要第二次 topology snapshot 调用；
- 不需要比较后“最多重试两次”的 magic retry；
- plugin 不得在 `OnSessionOpen` 中 type-assert cache 再取一个较新的 topology view；
- planning 全程使用 `ClusterInfo.DeviceTopology.Revision`，最终 reserve 再对 live ledger 校验引用对象。

### 5.5 Provider 重活、publish 与锁序

“进入同一次 Snapshot”只有在 publish 与 Node identity 也被协调时才成立。V3 的固定路径是：

~~~text
Node informer event
  -> enqueue existing Node work
  -> under SchedulerCache.Mutex record current NodeName/UID/resourceVersion
     and mark a replacement UID as Pending while invalidating old candidate facts
  -> release all locks
  -> parse, normalize, build graph closure, fingerprint and run Adapter ValidateTopology
  -> acquire SchedulerCache.Mutex
  -> verify NodeName still resolves to the same UID/resourceVersion
  -> acquire topologyCache.mu
  -> verify provider generation / current validation token
  -> publish a new immutable snapshot pointer
  -> release topologyCache.mu
  -> release SchedulerCache.Mutex
~~~

固定锁序是：

~~~text
SchedulerCache.Mutex -> topologyCache.mu
~~~

禁止任何路径以 `topologyCache.mu -> SchedulerCache.Mutex` 获取锁。`SchedulerCache.Snapshot()` 已持有主锁时只 atomic-load
immutable pointer，不再获取 topology write lock。Provider parse、Normalizer 重活、Adapter RPC 和持久 I/O 都在两把锁外执行。
只修改 topology ledger 且无需读取 `sc.Nodes` 的路径可以单独获取 `topologyCache.mu`；一旦需要两把锁，仍必须遵守上述顺序，
并且绝不能在持锁期间调用 Adapter。

如果锁外工作结束后 UID/resourceVersion 或 source generation 已变化，结果直接丢弃并重新入队；不能把旧 parse 结果发布给新 Node。
Node identity 的 Pending/invalidation 和 `sc.Nodes` 可见性必须在同一主锁下更新，使任何 Session 至多看到：

- 旧 Node + 旧 UID 的有效 view；或
- 新 Node + 新 UID 的 `Pending` view；

不能看到新 Node 与旧 UID topology 的组合。

### 5.6 Snapshot 内容与最终 revalidation

`DeviceTopologySnapshot` 至少包含：

~~~go
type DeviceTopologySnapshot struct {
    Revision             uint64
    PublishedAt          time.Time
    NodeIdentities       map[string]NodeIdentity
    ProviderReadiness    map[ProviderNodeKey]ProviderNodeSyncState
    Devices              map[DeviceKey]TopologyDevice
    LocalDomains         map[LocalDomainKey]DeviceDomain
    Fabrics              map[FabricKey]FabricDomain
    DeviceToDomains      map[DeviceKey][]LocalDomainKey
    NodeToDomains        map[types.UID][]LocalDomainKey
    SchedulableByDomain  map[LocalDomainKey][]DeviceKey
}
~~~

Plan 记录全局 `Revision` 用于诊断，并 pin 实际引用对象的 `MembershipFingerprint`、source generation 和 NodeUID。
提交时不要求整个 Revision 完全相等；final reserve 只重新验证 plan 引用对象，避免无关 Node heartbeat 使所有计划失效。

## 6. Group、AdmissionSet 与调度算法

### 6.1 复用 Job/SubJob，但保留稳定 Group 语义

`TopologyGroup` 是逻辑概念，不是新 scheduler object：

~~~go
type TopologyGroupRef struct {
    PodGroupUID   types.UID
    SubGroupID    api.SubJobID // empty for PodGroup-level scope
}
~~~

- PodGroup 级 `applyTo=Group` 使用 PodGroup UID；
- SubGroup 级 `applyTo=Group` 使用 PodGroup UID + 已规范编码的 SubGroup ID；
- `JobInfo/SubJobInfo` 继续拥有 Task membership 和 readiness；
- xPU context 只保存该 Group 的 policy fingerprint、placement anchor 和 assignment evidence。

`AdmissionSet` 是 winning Statement 中本轮新增 Allocate operations 的完整逻辑视图：

~~~text
AdmissionSet(statement, group)
  = 该 statement 中属于 group 的全部新增 Allocate operations
~~~

其规则是：

1. Group 尚未 Ready 时，包含本轮使 `JobReady/SubJobReady` 成立的确定性集合；
2. Group 已 Ready 时，包含本轮实际新增的全部 Allocate operations，不能退化为空；
3. 已 Binding/Running 且有可恢复 assignment 的成员是固定约束，不重复 Reserve；
4. `AdmissionSet` 中任一 Task 缺少完整 plan，则整个集合不进入 handoff；
5. `SaveOperations/RecoverOperations/Merge` 必须转移 plan context 的唯一所有权，不能复制第二个可提交 participant。

### 6.2 GroupPlacementAnchor

`GroupPlacementAnchor` 防止 `minAvailable < replicas` 时不同 admission wave 漂移到另一个 hard Fabric/Domain：

~~~go
type GroupPlacementAnchor struct {
    GroupRef                TopologyGroupRef
    ResourceName            corev1.ResourceName
    PolicyFingerprint       string
    LocalDomainKeys         []LocalDomainKey
    FabricKey               *FabricKey
    MembershipFingerprints  map[ObjectKey]string
    ReservationEpoch        string
    EvidenceRef             string
    RecordRevision          string
    Phase                   PlacementPhase
}
~~~

生命周期：

- 第一次成功 Exact handoff 前建立 anchor；anchor 持久化成功后才能进入 batch bind；
- 后续 AdmissionSet 必须先恢复并满足同一 anchor；
- hard policy 的 anchor 无法无歧义恢复时 fail closed，不能静默选择第二个 Fabric；
- policy fingerprint 不同的 plan 不能复用旧 anchor；
- 只有 Group 终止，且不存在 active allocation、reservation、reconciliation 后才能清理；
- anchor 写结果不明确时进入 reconciliation，不能把“写入超时”当作“没有 anchor”。

持久载体仍需 API review，但“不持久化而声称跨 scheduler restart 的 hard Group 保证”不是可选方案。

### 6.3 与 Network Topology 的组合

当前 `HyperNodeGradientForJobFn` 和 `HyperNodeGradientForSubJobFn` 是 first-plugin-wins。xPU plugin 不注册第二个
gradient 并假设框架会自动求交。组合顺序是：

1. Queue、NodeShard、network-topology-aware 等现有组件产生合法候选；
2. 普通 Predicate 验证 Kubernetes Node 级约束；
3. xPU filter 只缩小候选；
4. xPU soft/Compact score 进入现有 score aggregation；
5. Group planner 在这些候选上形成完整 AdmissionSet plan，不能重新引入已淘汰 Node。

若要在 HyperNode gradient 之前剪掉明显无 Domain 的 Node，应新增 framework-reviewed read-only feasibility summary，
而不是依赖插件注册顺序或第二个 gradient。

### 6.4 Node-local hard filter

一个 Pod 在一个 Node 上请求 `k` 个整卡 Device 时：

1. 从 `ClusterInfo.DeviceTopology` 找到该 NodeUID、resource、scope、tier 匹配的 local Domains；
2. 使用 `SchedulableByDomain` 的显式 Device 集合；
3. 逐 Device 检查 Healthy、Free、fresh、provider synced 和 Adapter enforceable；
4. 仅当一个 Domain 至少有 `k` 个 Device 时接受；
5. 两个 Domain 的 `6 + 2` 不能满足同一 Domain 请求 `8`；
6. 缺少可执行 exact Adapter 的 hard policy 返回 `XPUAssignmentNotEnforceable`；
7. 没有完整方案时返回结构化 xPU reason，不伪装成普通 Node aggregate shortage。

### 6.5 Fabric Group plan

hard Fabric plan 先选一个满足 anchor 的 `FabricKey`，再只在其完整 member local Domains 内分配所有 AdmissionSet members。
每个 Pod 仍必须满足本地 Domain；Fabric 不能代替 local Device grouping。

候选集合是：

~~~text
normal Predicate Nodes
INTERSECT NodeShard Nodes
INTERSECT selected HyperNode/network scope（如果配置）
INTERSECT explicit Fabric member Nodes
INTERSECT per-Pod local Domain feasibility
~~~

### 6.6 Deterministic Compact planner

Planner 是 side-effect-free 的。它不能调用 Adapter 或 live ledger reserve。建议顺序：

1. Task 按候选 Domain 最少、请求 Device 数最多、现有 Task order、PodUID 排序；
2. Domain/Device 按 3.2 的 deterministic Compact 顺序枚举；
3. plan-owned `NodeInfo` clone 重放普通 `AddTask` 资源记账，防止多个 Task 分别可放但合计超量；
4. greedy 无法完成时可做 bounded backtracking；
5. `maxSearchStates/maxCandidateDomains/maxPlanningAttempts/deadline` 到达时返回
   `XPUTopologyPlanningBudgetExceeded`，不能假报 `NotEnoughResources`；
6. provisional plan 无 reservation、token 或外部副作用；只有 winning Statement 的 final plan 可以 reserve。

## 7. Alpha 请求形状、Exact Plan 与 Adapter

### 7.1 单普通 Container 整卡限制

对每条 Alpha `DeviceTopologyPolicy.resourceName`，每个被该 policy 选中的 Pod 必须满足：

- 目标资源只出现在一个 `spec.containers[]` 普通 Container；
- request 为正整数 whole-device quantity；
- 该 Container 的 request/limit 符合 Kubernetes 扩展资源规则且数量一致；
- `initContainers`、restartable init sidecar、ephemeral container 不得请求该目标资源；
- 另一个普通 Container 不得再次请求同一目标资源；
- 不接受 DRA claim、MIG、vGPU、memory/core share 或厂商 fractional geometry；
- `applyTo=Group` 的所有目标成员都必须满足同一可解释形状；
- `applyTo=Pod` 时，只有 selector 命中的 Pod 进入该 policy，但命中后不得以“请求为零”静默跳过。

不满足时，admission webhook 应尽量提前拒绝；对于 controller 创建后或运行时才可见的对象，scheduler 写入
`XPUTopologyUnsupportedPodRequest` 并保持 Pending。

普通 Kubernetes effective Pod request 仍由现有 Predicate 计算。上面的限制只用于建立无歧义的
`ContainerName -> selected DeviceKeys -> canonical DeviceID` handoff，不在 xPU plugin 内重写 CPU/内存/Init Container
的一般资源计算。

### 7.2 完整 assignment identity

~~~go
type TopologyContainerAssignment struct {
    ContainerName string
    ResourceName  corev1.ResourceName
    DeviceKeys    []DeviceKey
}

type TopologyTaskPlacement struct {
    TaskID          api.TaskID
    PodUID          types.UID
    NodeName        string
    NodeUID         types.UID
    LocalDomainKey  LocalDomainKey
    FabricKey       *FabricKey
    Assignments     []TopologyContainerAssignment
}

type TopologyPlacementPlan struct {
    PlanID                   string
    GroupRef                 TopologyGroupRef
    PolicyFingerprint        string
    SnapshotRevision         uint64
    ReferencedFingerprints   map[ObjectKey]string
    Placements               []TopologyTaskPlacement
}
~~~

Alpha 中每个目标 resource 的 `Assignments` 恰有一项。Adapter 必须校验 PodUID、ContainerName、ResourceName、
request quantity、NodeUID、Domain/Fabric 和 DeviceKeys；任何字段变化都使旧 handoff 失效。Filter/Score/Allocate 阶段不得
再次运行厂商 chooser 并替换 plan 中的 ID。

### 7.3 Adapter capability 与唯一分配 owner

~~~go
type AllocationAdapter interface {
    IdentityContract() DeviceIdentityContract
    ValidateTopology(ctx context.Context, facts NodeTopologyFacts) error
    Reserve(ctx context.Context, plan TopologyPlacementPlan) (AdapterReservation, error)
    PrepareBind(ctx context.Context, reservation AdapterReservation, placement TopologyTaskPlacement) (AllocationHandoff, error)
    Commit(ctx context.Context, reservation AdapterReservation) error
    Compensate(ctx context.Context, reservation AdapterReservation) (ReconcileResult, error)
    Release(ctx context.Context, reservation AdapterReservation) (ReconcileResult, error)
    Recover(ctx context.Context) ([]RecoveredReservation, error)
    Reconcile(ctx context.Context, requests []ReconcileRequest) ([]ReconcileResult, error)
}
~~~

~~~go
type DeviceIdentityContract struct {
    ProviderID string
    Namespace  string
    ResourceName corev1.ResourceName
}
~~~

一个 resource 在一次调度生命周期只有一个 exact-allocation owner。xPU Adapter 与现有 deviceshare/DRA/backend 不得分别
reserve 或扣减同一批 Device ID。Adapter identity contract 必须与
`DeviceIdentityContract{ProviderID, Namespace, ResourceName}`
完全匹配，并证明 backend 接受这些 ID。

Plain Device Plugin 数量和 kubelet `GetPreferredAllocation` 不是 scheduler-selected exact reservation 接口。
未经验证时它们只允许 `soft` advisory；不能启用 `hard`。

`Compensate/Release` 的 nil error 本身不是可复用 Device 的充分条件；返回值还必须携带对应 owner 的显式
`Released` 证据。RPC error、空结果或 `Unknown` 一律进入第 9 章的 reconciliation。

## 8. 事务语义：名称开放，行为固定

### 8.1 当前缺口

当前 `Statement` 可以回滚 Task/Node speculative operations，但不会自动回滚 allocate action 的所有旁路状态；当前 bind 路径也不是
全组闸门。因此 Exact Alpha 需要 framework change。

以下接口名都只是候选：

~~~text
BeforeStatementCommit / TransactionParticipant
AddBindGroup / AddBindBatch
GroupCommitHandle / XPUTopologyHandoff
~~~

maintainer 可以选择最终命名和抽象层次，但 8.2 至 8.8 的语义及失败注入测试不能因命名争论被删除。

### 8.2 Allocation-attempt checkpoint

每次 final plan/reserve 前，由 allocate action 建立 checkpoint，至少覆盖：

- `JobWorksheet`、`SubJobWorksheet` 及其 queue/iterator 状态；
- `NodesFitErrors` 和候选淘汰记录；
- SubJob allocation、nomination 和 HyperNode/Domain choice；
- recorder decision、plan context 和本轮派生 score；
- 尚未由 Statement operation 拥有的 Session overlay mutation。

`Statement.Discard()` 继续逆序恢复 Task/Node operation；checkpoint 恢复其余旁路状态。只有整个事务成功后才能 adopt checkpoint。
首次 reserve 失败后，下一次 replan 必须看到失败前的完整 worksheet，而不是丢失 Task、候选或 nomination。

### 8.3 正确顺序

~~~mermaid
sequenceDiagram
    participant A as Allocate action
    participant P as xPU Planner
    participant S as Statement
    participant L as Topology Ledger
    participant X as Exact Adapter
    participant Q as Group Bind Gate
    participant K as Kubernetes API

    A->>A: Create allocation-attempt checkpoint
    A->>P: Build side-effect-free AdmissionSet plan
    P-->>A: Complete immutable plan
    A->>S: Tentative Allocate all selected Tasks
    S->>L: Final revalidate and atomically hold every ID
    L-->>S: Held reservation or no change
    S->>X: Reserve and Prepare every assignment
    X-->>S: Immutable handoffs
    S->>S: Persist anchor/evidence
    S->>Q: Prepare complete bind batch
    Q-->>S: All PreBind succeeded
    S->>X: Commit idempotently
    S->>Q: Atomically accept complete batch
    loop Kubernetes binding is per Pod
        Q->>K: Bind one Pod
        K-->>Q: success or failure
    end
~~~

在 `Q-->>S: All PreBind succeeded` 之前，Kubernetes Bind 调用次数必须为零。

### 8.4 Final reserve 与 group completeness

final reserve 必须：

1. 覆盖 AdmissionSet 中全部新 Allocate operations；
2. 重新校验 PodUID、NodeUID、policy/anchor、请求数量和引用 fingerprint；
3. 对全部 DeviceKeys 做 all-or-nothing `Free -> Held`；任一冲突则零修改；
4. 使用 canonical Fabric/Domain/Device lock order；
5. reserve 成功后把唯一 owner token 与同一个 Statement/group context 绑定；
6. 不允许失败后只替换一个 Task 的 Device 并继续使用旧 Group plan。

### 8.5 全组 PreBind 与零提前 Bind

组级 bind gate 必须同时接收全部 BindContexts，并保证：

- 在任何 context 进入可被 bind worker 消费的队列前，完整校验 Task/Pod/Node/handoff；
- 对完整 batch 运行所有 PreBind/Prepare；
- 任一成员失败时逆序 rollback 已完成的 PreBind，提交零个 BindContext；
- 参与此路径的 PreBinder 必须提供 batch contract 或幂等 rollback；
- 成功后以一个不可拆分 queue item 接受 batch；
- prepared xPU batch 不再进入现有 per-context `executePreBinds()` 第二次执行；
- worker 只有看到 batch 的 `Prepared=true` 和完整 digest 后才能逐 Pod Bind。

当前 `executePreBinds()` 的“失败一个、继续绑定其他成功项”不能用于 Exact Alpha group path。

### 8.6 幂等 compensation

所有跨系统操作使用稳定 `{SchedulerEpoch, PlanID, ReservationID, PlanDigest}`：

- `Reserve/Prepare/Commit/Compensate/Release` 重试同一个 token 必须得到相同结果或显式 Unknown；
- 失败时按相反顺序补偿：batch preparation -> Adapter -> ledger -> Session/Statement -> checkpoint；
- 补偿“请求已发出但结果未知”不能视为成功；相关 ID 进入 `ReconcilePending`；
- 旧 SchedulerEpoch 的请求不能覆盖新 leader 的 owner；
- 一个 Pod 已绑定后不能用本地 rollback 假装其外部分配已撤销；只能 reconcile。

### 8.7 可接受的 framework 形状

形状 A 可以是最小 hook：

~~~go
type BeforeStatementCommitFn func(
    ctx context.Context,
    operations []OperationView,
) (*GroupCommitHandle, error)
~~~

形状 B 可以是通用 participant：

~~~go
type TransactionParticipant interface {
    Prepare(context.Context) error
    Commit(context.Context) error
    Rollback(context.Context) error
}
~~~

无论选择哪一种，都要求：

- `Statement` 的 exact 提交路径返回 `error`；
- 错误前不清空 operations；
- group context、participant、checkpoint 只有一个 owner；
- 非 xPU 的现有 `Statement.Commit()` 调用可以保持兼容；
- cache batch submission 自身有完整预校验和 error result。

### 8.8 Kubernetes Binding 边界

Exact Alpha 承诺：

> 在任何 Pod 开始 Kubernetes Bind 前，AdmissionSet 中所有 exact assignment 已完成 final reservation、Adapter
> preparation、anchor/evidence 持久化和全组 PreBind。

它不承诺 Kubernetes API 的多 Pod Bind 原子性。第一个 Pod 成功、第二个 Pod 失败时：

- 成功 Pod 的 allocation 进入 authoritative observation/reconciliation；
- 失败和未绑定成员执行幂等 compensation；
- 不确定 Device 保持不可用；
- Job controller 是否补偿已绑定 Pod 是更上层启动/容错策略。

## 9. Ledger、抢占、恢复与 Condition

### 9.1 Allocation state machine

~~~mermaid
stateDiagram-v2
    [*] --> Free
    Free --> TentativeReserved: Session dry-run
    TentativeReserved --> Free: Statement discard
    TentativeReserved --> Held: final all-or-nothing reserve
    Held --> Free: compensation and release confirmed before handoff
    Held --> Binding: complete batch accepted
    Binding --> Allocated: authoritative Allocated
    Binding --> ReconcilePending: timeout or Unknown
    Allocated --> ReconcilePending: release requested
    ReconcilePending --> Allocated: authoritative Allocated
    ReconcilePending --> Free: authoritative Released
~~~

`TentativeReserved` 只在 Session overlay；`Held/Binding/Allocated/ReconcilePending` 属于 live ledger。
Health 变化不自动改变 allocation owner。

### 9.2 仅延期 topology-aware victim selection

**后续研究**可以延期：用 Domain/Fabric 碎片、link 或 replacement plan 优化 victim 选择。

**Exact Alpha 必做**：任何现有 preemption/reclaim/eviction 导致的设备释放都经过同一个 ledger/Adapter path。

- `Statement.Evict()` 或 Deallocate callback 只能记录 `ReleaseRequested`/`ReconcilePending`，不能立即 `Free`；
- `Releasing` Pod、Node `FutureIdle` 和 nomination 不是 authoritative Device release；
- victim Pod 仍在 Node 或 Adapter 仍报告 allocation 时，其 Device 不能被 final reserve；
- eviction 被取消/回滚时保留或恢复同一个 owner，不能创建第二份 owner；
- 只有 Adapter/Claim/可信 runtime source 明确返回 `Released` 后才能 `Free`；
- release 后 fingerprint/availability 变化时，新 workload 重做完整 plan；
- Pipeline 只保存可重建 hint，不提前 reserve 等待 victim 的 Device。

### 9.3 显式 reconciliation result

~~~go
type ReconcileState string

const (
    ReconcileAllocated ReconcileState = "Allocated"
    ReconcileReleased  ReconcileState = "Released"
    ReconcileUnknown   ReconcileState = "Unknown"
)

type ReconcileResult struct {
    ReservationID string
    PlanDigest    string
    PodUID        types.UID
    DeviceKeys    []DeviceKey
    State         ReconcileState
    ObservedAt    time.Time
    EvidenceRef   string
}
~~~

规则：

1. `Allocated` 保留 owner 并进入/保持 `Allocated`；
2. `Released` 是唯一允许对应 owner 返回 `Free` 的结果；
3. `Unknown` 进入/保持 `ReconcilePending`；
4. RPC error、timeout、空列表、缺少某个 token、Pod NotFound 或 scheduler 内存里没有记录，都不是 release 证据；
5. Adapter 必须逐个回答请求中的 reservation/token。结果缺席按 `Unknown` 处理；
6. 如果未来 Adapter 支持 authoritative complete snapshot，也必须为 scheduler 已知未决 token 给出可审计的 negative/release proof，
   不能仅以“列表里没出现”推断 Released。

### 9.4 重启和 leader 切换

在没有共享持久 reservation record 之前，保证范围是：单 active leader + process-local ledger + backend durable Adapter lease。
Follower 可以 warm informer/provider read-only state，但不能 reserve、commit 或 handoff。

新 leader 必须：

1. 等待 Node/Provider 初次同步；
2. 调用 Adapter `Recover()`；
3. 用 plan digest、NodeUID、DeviceKey 和 anchor/evidence 重建 ledger；
4. 把无法匹配的项置为 `ReconcilePending`；
5. 只有该 resource 恢复完成后才允许新的 hard plan。

不得声称仅靠内存 CAS 支持 active-active 多 scheduler exact reservation。

### 9.5 PodGroup Condition/Reason 写入路径

建议复用现有 `PodGroupCondition`，不新增一套 xPU status API。调度期路径是：

~~~text
xPU filter/plan/reserve/reconcile produces structured result
  -> one scheduling-outcome aggregator chooses the most specific reason
  -> Session.UpdatePodGroupCondition(job, condition)
  -> JobUpdater compares PodGroupOldState
  -> SchedulerCache.UpdateJobStatus
  -> PodGroup status persisted
~~~

由于当前 `Session.UpdatePodGroupCondition()` 按 `Type` 覆盖，xPU plugin 和 gang plugin 不能各自追加一个互相覆盖的
`Unschedulable` condition。应由统一 outcome aggregator 在 Session close 前选择：xPU 的具体原因优先于泛化的
`NotEnoughResources`，同时保留普通 fit errors 作为 message 细节。

`XPUTopologyPolicyConflict` 和 `XPUTopologyPolicyMutationForbidden` 是 controller/admission 产生的 authoring blocker。
只要 controller 尚未确认冲突已经消失，Session outcome aggregator 不得用动态资源 reason 覆盖它们；解除后由 controller
使用 status-subresource conflict retry 将 blocker 置为 `False`，scheduler 才能重新评估动态 topology。

失败 Condition：

~~~go
&scheduling.PodGroupCondition{
    Type:               scheduling.PodGroupUnschedulableType,
    Status:             corev1.ConditionTrue,
    TransitionID:       string(ssn.UID),
    LastTransitionTime: metav1.Now(),
    Reason:             reason.Code,
    Message:            reason.BoundedMessage(),
}
~~~

本轮 xPU constraint 已解决时，将 `Unschedulable` 更新为 `False`、Reason=`XPUTopologyResolved`，再由现有路径写
`Scheduled` outcome，避免用户看到陈旧 True。Condition message 可以包含 resource、scope、required count、最大可用 Domain
count 和 provider/adapter 状态，但不能把 DeviceID/PodUID/token 用作 metrics label。

建议 reason：

~~~text
XPUTopologyPolicyConflict
XPUTopologyPolicyMutationForbidden
XPUTopologyDataNotReady
XPUTopologyStale
XPUTopologyUnsupportedPodRequest
XPUAssignmentNotEnforceable
XPUDeviceDomainFragmented
XPUFabricDomainUnavailable
XPUDeviceUnhealthy
XPUReservationConflict
XPUTopologyPlanningBudgetExceeded
XPUPreBindFailed
XPUReconcilePending
~~~

策略 authoring 冲突出现在 scheduler Session 之前时：

- admission webhook 可直接拒绝请求；
- auto-PodGroup controller 遇到成员 annotation 冲突时，不覆盖 canonical spec，并通过 status-subresource retry helper 写入
  `XPUTopologyPolicyConflict`，同时记录 Event；
- controller 写回与 scheduler 写回都使用 resourceVersion conflict retry，不能覆盖对方的无关 status 字段。

## 10. 失败矩阵

| 失败点 | hard 行为 | ledger/事务结果 | Condition/Reason |
| --- | --- | --- | --- |
| 新 Node provider 仍 Pending | 当前候选 fail closed | 不 reserve；旧 UID 不复用 | `XPUTopologyDataNotReady` |
| annotation/typed policy 冲突 | Group Pending | 不产生 plan | `XPUTopologyPolicyConflict` |
| 活动期修改 policy | 拒绝更新；旧 fingerprint 继续有效 | 不迁移 owner | `XPUTopologyPolicyMutationForbidden` |
| Provider stale/invalid | 不使用新 hard plan | 保留已有 owner；需要时 reconcile | `XPUTopologyStale` |
| Node 同名重建 | 新 UID Pending；旧 key 不候选 | 旧 active key tombstone | `XPUTopologyDataNotReady` |
| Fabric owner/member UID 变化 | Fabric 不可用，等待 owner 重发完整集合 | active key 保留 | `XPUFabricDomainUnavailable` |
| 6+2 请求同 Domain 8 | Node 不可行 | 无 reservation | `XPUDeviceDomainFragmented` |
| 请求来自多 Container/init/shared unit | Group Pending 或 admission reject | 无 reservation | `XPUTopologyUnsupportedPodRequest` |
| 缺 compatible exact Adapter | hard 不调度 | 无 per-ID owner | `XPUAssignmentNotEnforceable` |
| bounded planner 耗尽 | retryable Pending | 无 reservation | `XPUTopologyPlanningBudgetExceeded` |
| final fingerprint/NodeUID 改变 | 丢弃并完整 replan | CAS 零修改 | `XPUReservationConflict` |
| 任一 ID reserve 冲突 | 完整 replan，不局部换卡 | all-or-nothing CAS | `XPUReservationConflict` |
| Adapter Reserve/Prepare 失败 | 零 Bind | 逆序补偿；不明则 ReconcilePending | adapter reason / `XPUReconcilePending` |
| anchor/evidence 写入失败 | 零 Bind | 明确失败则补偿；结果不明则 reconcile | `XPUReconcilePending` |
| 任一 PreBind 失败 | 整 batch 提交零个 BindContext | rollback checkpoint/Statement/Adapter/ledger | `XPUPreBindFailed` |
| 单 Pod Bind 在 batch dispatch 后失败 | 不声称组原子回滚 | 已绑定项 reconcile；未绑定项补偿 | bind reason / `XPUReconcilePending` |
| eviction 请求已发出但未确认 release | Device 不进入新 plan | 保持 owner/ReconcilePending | `XPUReconcilePending` |
| Reconcile 返回空/缺项/Unknown | 不复用 ID | 保持 ReconcilePending | `XPUReconcilePending` |
| scheduler restart/recovery failure | 对该 resource 暂停 hard | recovered owner 或 quarantine | `XPUTopologyDataNotReady` |

soft policy 的 topology data/adapter 不可用时只失去相应 preference，并记录低基数退化原因；它不能让一个普通 Predicate
失败的 Node 重新可调度，也不创建具体 ID reservation。

## 11. 验证与验收计划

### 11.1 API 和 canonicalization

- `hard/soft` default、invalid value reject、tier/tierName 互斥；
- Public schema、Pod annotation 和 VCJob field 规范化后 fingerprint 一致；
- annotation 无 `allocationStrategy`，Public schema 也拒绝该字段；
- direct PodGroup 与 Pod annotation 相同可接受，不同 fail closed；
- VCJob 与 child annotation 不同 fail closed；
- canonical spec 为空但 `XPUTopologyPolicyConflict=True` 时，`JobValid/JobEnqueueable` 仍阻止调度；
- Deployment/ReplicaSet、StatefulSet、bare Pod 均只生成 canonical PodGroup policy；
- scheduler 测试证明它从不直接读取 workload annotation；
- quiescent 更新成功并废弃旧 plan；活动期 mutation 被拒绝；
- invalid annotation 不回落成默认 hard，也不按无 policy 调度。

### 11.2 Identity、Provider 和 Snapshot

- 两个 Node 都有 `device0/domain0` 时 canonical keys 不冲突；
- 同名 Node 换 UID 后旧 facts 退出 candidate index，新 UID 为 Pending；
- 迟到的旧 UID update/clear 不影响新 UID；
- old UID active allocation 保留 tombstone，直至显式 Released；
- ProviderID、identity namespace 和 source device value 各自参与正确字段，不能互相代替；
- 重复 source generation 只允许 content-identical heartbeat；
- Fabric 只有 owner annotation 可声明，member 重复声明被拒绝；
- owner 必须发布完整集合，member UID/generation 变化后必须重发；
- `ClusterInfo.Nodes` 与 `DeviceTopology.NodeIdentities` 在并发 Node replacement 下不存在新 Node/旧 UID 混合；
- Snapshot 指针 publish 后不可变；Provider update 不改变旧 Session view；
- provider parse/Adapter RPC 在锁外；锁序测试和 race test 不出现反向加锁；
- 不存在 dual-snapshot retry count 或 plugin 二次捕获。

### 11.3 Filter、Compact 和 Group

| 场景 | 期望 |
| --- | --- |
| 单 Node 两 Domain free=6+2，请求 8，hard | 失败，不能跨 Domain 拼接 |
| 单 Domain free=8，请求 8 | 选择该 Domain 的 8 个稳定 DeviceKeys |
| 两个等价 Domain | 多次运行选择完全一致 |
| Fabric 未显式声明，只共享 HyperNode/IB | 不能满足 hard Fabric |
| `minAvailable=4, replicas=10` | 首 wave 建立 anchor；后续 6 个 Task 继续满足同一 anchor |
| winning Statement 含已 Ready Group 的新增 Task | 全部新增 Allocate operation 都进入 AdmissionSet |
| normal Predicate 已排除 Node | xPU score/plan 不得重新引入 |
| planner budget 耗尽 | 返回专用 retryable reason，不假报资源不足 |

### 11.4 Exact transaction 和 reconciliation

- 多 Container/init/fractional 请求被拒绝；
- assignment 精确包含 PodUID、ContainerName、ResourceName 和 DeviceKeys；
- final CAS 任一冲突时全部 ID 保持原状态；
- reserve 失败后 checkpoint 恢复 worksheet、fit error、nomination 和 recorder decision；
- Adapter Prepare 第 N 项失败时前 N-1 项逆序幂等补偿；
- anchor 持久化失败时调用 Kubernetes Bind 零次；
- batch 第 N 个 PreBind 失败时提交 BindContext 零个、Kubernetes Bind 零次；
- prepared batch 不重复进入 per-context `executePreBinds()`；
- batch dispatch 后单 Pod Bind 失败不伪造对成功 Pod 的原子回滚；
- eviction 到 authoritative Released 之前 Device 一直不可 reserve；
- `Reconcile()` 返回 `Allocated/Released/Unknown` 三态；空结果和缺项均按 Unknown；
- restart/leader promotion 在 Recover 完成前拒绝 hard；
- 同一 token 的 Reserve/Prepare/Commit/Compensate/Release 重试满足幂等性。

### 11.5 回归、E2E 与性能

- 未配置 policy 时 network-topology-aware、gang、Statement 和现有 device paths 行为不变；
- HyperNode API 不增加 Device/Reservation 字段；
- Mock/KWOK 覆盖 fragmented/fitting Domain、explicit Fabric、Node replacement、health update 和失败回滚；
- 至少一个真实 Exact Adapter 覆盖选中 ID、进程重启恢复、release 和故障注入，才可称为可用 Exact Alpha；
- benchmark 对比 plugin disabled、enabled-no-policy、soft 和 hard；
- metrics 覆盖 provider age/error、snapshot publish latency、plan duration/budget、reservation lifecycle、
  unreconciled duration、recovery result 和结构化 reason；
- Event/metrics label 禁止使用 raw DeviceID、PodUID 或 ReservationID。

## 12. PR 拆分与交付阶段

### 12.1 PR 0：API 与 framework contract review

- 冻结 `DeviceTopologySpec` 的 hard/soft、applyTo、scope、tier/tierName；
- 明确 Public API 无 allocationStrategy；
- 评审 Pod annotation key、canonicalization 和 mutation webhook；
- 在 `BeforeStatementCommit` 与 participant、`AddBindGroup` 与 batch 接口之间选择最终名称；
- 接受 checkpoint、全组 PreBind、零提前 Bind、compensation 和 reconciliation 验收合同。

### 12.2 PR 1：canonical model、Provider 和 identity

- 实现 Device/LocalDomain/Fabric IDs 和 keys；
- Node UID/RV ordering、Pending/Synced、old UID tombstone；
- Annotation/Mock Provider、schema/security 和 freshness；
- single-owner complete Fabric declaration；
- immutable topology objects、indexes 和 unit tests。

### 12.3 PR 2：ClusterInfo snapshot 与 Advisory MVP

- `ClusterInfo.DeviceTopology` 和同一次 `SchedulerCache.Snapshot()` 捕获；
- 按固定锁序 publish immutable pointer；
- Pod/VCJob/PodGroup canonicalization 和 Condition reason；
- Node-local filter 数据、soft score、internal deterministic Compact；
- plugin-disabled/no-policy 回归和并发 snapshot 测试。

本阶段可称 Advisory MVP。若没有 exact Adapter 和第 8 章事务合同，hard 必须保持 fail closed。

### 12.4 PR 3：Exact Alpha transaction

- 单普通 Container 整卡 validation 和完整 assignment identity；
- side-effect-free group planner、AdmissionSet、allocation-attempt checkpoint；
- final all-or-nothing ledger reserve；
- Adapter durable reservation、prepare/commit/compensate/recover；
- GroupPlacementAnchor/evidence；
- complete-batch PreBind 和 atomic batch acceptance；
- Allocated/Released/Unknown reconciliation；
- eviction/release ledger integration；
- failure injection、restart 和 leader-handoff tests。

### 12.5 PR 4：Fabric/Group E2E 与发布

- explicit Fabric + HyperNode intersection；
- 跨 wave anchor E2E；
- KWOK scale、provider churn、Node replacement、Fabric owner replacement；
- metrics、runbook、feature gate/plugin config 和 safe drain；
- 真实 Adapter 验收后发布 Exact Alpha 能力边界。

## 13. 兼容性、明确不做与开放问题

### 13.1 兼容原则

1. 未配置 `DeviceTopology` 时零行为变化；
2. HyperNode 仍只拥有 Node/网络层次；
3. Job/SubJob readiness 不复制；
4. normal resource fit 不被 topology ledger 二次扣减；
5. existing deviceshare/DRA 未成为 compatible Adapter 前不共享 exact owner；
6. soft 不制造 hard filter，hard 不静默降级；
7. Mock/Annotation 不升级成真实硬件 exact-ID 证据；
8. Kubernetes Binding 不描述为整组原子提交；
9. feature/plugin/Adapter identity 配置不一致时 fail closed；
10. 禁用 exact feature 前必须 drain 非终态 policy、reservation、allocation 和 reconciliation。

### 13.2 已决定，不再作为开放问题

- Node-scope identity 必须包含 NodeUID；
- DeviceTopologySnapshot 放入 ClusterInfo 同次捕获；
- Public mode 使用 hard/soft；
- Alpha Public API 不含 allocationStrategy；
- scheduler 只读取 PodGroup canonical spec；
- activity 后 policy semantic mutation 被拒绝；
- Exact Alpha 为单普通 Container 整卡；
- Annotation Fabric 为 single owner complete declaration；
- topology-aware victim choice 可延期，但 release ledger correctness 不可延期；
- exact transaction 必须有 checkpoint、全组 PreBind、零提前 Bind、幂等 compensation 和显式三态 reconciliation。

### 13.3 仍需社区评审的 P0/P1 问题

| 优先级 | 问题 | 不确定的只是 | 已固定的底线 |
| --- | --- | --- | --- |
| P0 | transaction framework 采用最小 hook 还是 generic participant | Go API 名称、注册和兼容形状 | 第 8 章语义与测试不可缩减 |
| P0 | GroupPlacementAnchor/evidence 的持久载体 | PodGroup status、受控 annotation、Claim 或 Adapter record | 必须 durable、CAS、可恢复、可 reconcile |
| P0 | 第一个真实 Exact Adapter | HAMi/vGPU companion、厂商 API 或其他实现 | 必须接受 scheduler-selected IDs 并实现 Recover/Released evidence |
| P1 | early domain feasibility 如何与 HyperNode candidate path 组合 | 新 hook 的位置和数据结构 | 不注册第二个 first-plugin-wins gradient |
| P1 | `tierName` 是否更名为 `domainClass` | API 命名 | 它是非唯一分类，不是 Domain ID |
| P1 | Fabric mock/KWOK 与真实 Fabric 验收的发布节奏 | milestone 顺序 | 普通网络不可自动推导 Fabric |

### 13.4 后续研究清单

- topology-aware victim selection；
- DRA claim selector 和 ResourceSlice Provider；
- multi-container/init-container lifecycle；
- MIG/vGPU/shared geometry；
- cluster-scoped Fabric authority；
- link-aware ring/collective communication planning；
- shared durable reservation service 与 active-active fencing；
- workload start barrier 或 controller-level whole-group compensation。

## 附录 A. 实现映射

| 区域 | 当前源码 / 建议文件 | 责任 |
| --- | --- | --- |
| 当前 ClusterInfo | `pkg/scheduler/api/cluster_info.go` | 增加 immutable `DeviceTopology` 字段 |
| 当前 snapshot | `pkg/scheduler/cache/cache.go:SchedulerCache.Snapshot` | 同主锁观察点捕获 cluster + topology pointer |
| Session 初始化 | `pkg/scheduler/framework/session.go:openSession` | 从 ClusterInfo 取得同一 topology view |
| Provider/normalizer | `pkg/scheduler/topology/provider/`（建议） | Annotation/Mock parse、validate、NodeUID/RV/generation |
| live topology | `pkg/scheduler/cache/topology_cache.go`（建议） | facts、readiness、indexes、ledger、immutable publish |
| Pod canonicalization | `pkg/controllers/podgroup/pg_controller_handler.go`、Job controller、webhook | authoring source 转换、冲突和 mutation validation |
| Public types | `staging/src/volcano.sh/apis/pkg/apis/scheduling/`、batch API | `DeviceTopologySpec` typed schema 与生成代码 |
| 当前 Statement | `pkg/scheduler/framework/statement.go` | 保留普通 transaction；新增 exact error/group contract |
| 当前 bind path | `pkg/scheduler/cache/cache.go:AddBindTask/executePreBinds/BindTask` | 新增 complete-batch gate，不复用部分成功循环 |
| Condition | `pkg/scheduler/framework/session.go:UpdatePodGroupCondition`、`job_updater.go` | 聚合具体 xPU reason 并回写 PodGroup status |
| Plugin | `pkg/scheduler/plugins/xputopology/`（建议） | policy compile、filter、score、group plan |
| Adapter | `pkg/scheduler/topology/adapter/`（建议） | exact reserve/handoff/recover/reconcile |

## 附录 B. 关键不变量

1. NodeName 相同不代表 Node identity 相同；所有 Node-owned key 包含 NodeUID。
2. Source ID、Provider identity 和 canonical scheduler key 是不同层次。
3. 一次 Session 只使用 `ClusterInfo` 携带的一份 immutable topology view。
4. publish 与 Node identity 在 `SchedulerCache.Mutex` 下协调；重活和 RPC 在锁外。
5. scheduler 只读取 `PodGroup.spec.deviceTopology`。
6. Public hard/soft 不包含 allocationStrategy；internal Compact 必须确定性。
7. 活动 policy 不原地切换 fingerprint。
8. Annotation Fabric 只有一个 owner，且 owner 发布完整成员集合。
9. AdmissionSet 覆盖 winning Statement 中该 Group 的全部新 Allocate operations。
10. Exact assignment 总是绑定 PodUID、ContainerName、ResourceName 和具体 DeviceKeys。
11. 任一 final reserve/prepare/prebind 失败，在 Kubernetes Bind 前保持零 dispatch。
12. 所有外部 compensation 幂等；不确定结果进入 ReconcilePending。
13. observation 缺席不是 Released；只有显式 release evidence 才能 Free。
14. victim release 未确认前，其 Device 仍不可用于新 reservation。
15. Kubernetes per-Pod Bind 非原子，设计不声称可以原子 unbind。

## 附录 C. V3 状态边界

~~~text
当前实现
  HyperNode + network-topology-aware
  JobInfo/SubJobInfo readiness and AllocatedHyperNode
  SchedulerCache.Snapshot without DeviceTopology
  Statement Allocate/Discard/Commit per operation
  AddBindTask and per-context PreBind
  PodGroup Condition update path

可复用能力
  existing Queue/Gang/Predicate/HyperNode candidate path
  Job/SubJob as TopologyGroup context
  Statement as owner of normal Task/Node speculative mutation
  SchedulerCache as owner of one ClusterInfo snapshot

本文提议
  NodeUID-safe canonical IDs and tombstones
  ClusterInfo.DeviceTopology immutable paired view
  hard/soft Public API and internal deterministic Compact
  one PodGroup canonicalization path
  quiescent-only policy mutation
  single-container whole-device Exact Alpha
  single-owner complete Annotation Fabric
  checkpoint + group PreBind gate + idempotent compensation
  Allocated/Released/Unknown reconciliation
  eviction/release ledger correctness

后续研究
  topology-aware victim selection
  DRA/MIG/vGPU/multi-container
  cluster-scoped Fabric authority
  active-active durable reservation
  Kubernetes group atomic binding/start barrier
~~~

本文不把 V3 设计、Mock 验证、社区开放 PR 或文档中的 proposed API 写成 Volcano 当前已实现能力。
