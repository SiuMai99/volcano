# Volcano 通用 xPU 拓扑感知调度设计（收敛版 V4）

> 状态：结构化设计提案，不代表 Volcano 已实现或社区已接受。
>
> 本文以 [V3](./99-generic-xpu-topology-aware-design-zh-v3.md) 为基线，保留其 Node 身份、Session Snapshot、
> Annotation canonicalization 与现有 Statement/Pod Bind 路径，并把 workload topology taxonomy 收敛为
> `scope + domainClass`：删除 Public `tier/tierName`，同时补齐 Node-local Domain 与 Fabric Domain 的统一类别合同。
> Alpha 不引入 V3 中完整的 durable reservation、跨系统事务、全组 Bind barrier 或 active-active fencing；这些是后续 Exact 设计。
>
> 关联议题：[volcano-sh/volcano#5751](https://github.com/volcano-sh/volcano/issues/5751)
>
> 关联社区提案：[volcano-sh/volcano#5965](https://github.com/volcano-sh/volcano/pull/5965)

## 1. V4 结论与保证边界

### 1.1 相对 V3 的八项收敛决定

| # | V4 决定 | 对 V3 的变化 |
| --- | --- | --- |
| 1 | workload selector 固定为 `DeviceTopologyDomainSelector{Scope, DomainClass}` | 删除 Public `tier/tierName`；`domainClass` 是必填的稳定类别名 |
| 2 | `scope` 只表示 Domain 的身份、ownership 与 membership 边界 | `Node/Fabric` 不再兼任类别或层级；它不能回答是 NVLink island、HCCS domain 还是 PCIe root |
| 3 | `DomainClassKey = ResourceName + Scope + Name` | class 名只在一个 resource 和 scope 内解释；同名 class 不跨 resource/scope 比较 |
| 4 | `DeviceDomain` 与 `FabricDomain` 都携带 `Class DomainClassKey` | 补齐 V3 中 `scope=Fabric + tierName` 无 canonical Fabric 匹配字段的断链 |
| 5 | 管理员发布 `ResourceTopologyDescriptor` class catalog，Provider 只把厂商事实规范化到已声明 class | class 是 workload 合同，不是 Provider 临时造出的 ID，也不是 workload 指定的具体 Domain |
| 6 | hard policy 的未知、不支持或无可执行 class 一律 fail closed | 不按数字层级隐式扩大；内部 rank 不进入 workload API，也不产生 fallback |
| 7 | `applyTo` 继续与 `scope/domainClass` 正交 | 保留 `∀p∃d_p` 与 `∃d∀p`、SubGroup 作用域以及跨 admission wave anchor 语义 |
| 8 | `XPUTopologyAwareScheduling` Alpha feature gate 与 `xpu-topology-aware` scheduler plugin 双重显式启用 | 默认关闭；gate/plugin 任一缺失都不允许接受或静默忽略 xPU policy |

V3 的 NodeUID-safe identity、同一次 Session snapshot、single-owner Fabric 和 deterministic Compact 继续继承；
V4 Alpha 不承诺 V3 草案中的完整 reservation、全组 PreBind、三态外部 reconciliation 或 release correctness，
而是先用已绑定 Pod 的 NodeName 与 assignment annotation 恢复 anchor。后续 Exact 可在此基础上补回这些能力。

### 1.2 一条主路径，而不是平行调度器

~~~text
VCJob typed field / direct PodGroup spec / Pod annotations
  -> controller canonicalization
  -> PodGroup.spec.deviceTopology（scheduler 唯一策略真源）
  -> feature gate + xpu-topology-aware plugin activation validation
  -> SchedulerCache.Snapshot（ClusterInfo + immutable DeviceTopologySnapshot）
  -> existing HyperNode / Predicate / Queue / Gang candidate path
  -> xPU Domain/Fabric filter and deterministic Compact score
  -> Group/Pod plan in the current Session
  -> Statement tentative Allocate
  -> select Node + canonical DeviceKeys
  -> attach scheduler-owned xpu-assignment to each Pod BindContext
  -> existing per-Pod PreBind/Bind
  -> next Session derives Group anchor from bound Pods
~~~

V4 不建立第二套 Job、SubGroup、readiness 或调度循环。`TopologyGroup`、`AdmissionSet` 和
Pod-derived group anchor 是附着于现有 Job/SubJob/Statement 的 Session-local 内部语义，不是新的 CRD；PodGroup 摘要也不是唯一事实源。

### 1.3 两级能力，禁止混写保证

| 能力级别 | 可以承诺 | 不能承诺 |
| --- | --- | --- |
| Advisory MVP | Annotation/Mock topology ingestion、immutable snapshot、Node-local Domain filter 数据、`soft` score、deterministic Compact、结构化 reason | 运行时一定使用 scheduler 选中的 Device ID；`hard` 自动降级；多 Pod 原子绑定 |
| Pod-derived Topology Alpha | compatible Provider/Device Plugin 能消费或确认选中 ID；`hard` fail closed；已绑定 Pod 保存 NodeName 与 assignment；后续 wave/Session 从 Pod 恢复 anchor | 外部 durable reservation；跨系统 crash recovery/release；Kubernetes API 级多 Pod 原子绑定；active-active fencing |
| 后续 Exact | 在 Alpha 上增加 backend reservation、完整 batch、跨系统恢复/释放和真实 exact-ID enforcement | Kubernetes API 级多 Pod 原子绑定仍不能声称；未验证的 native Device Plugin exact-ID；topology-aware victim selection |

没有能够消费或确认 scheduler-selected DeviceKey 的 Provider/Device Plugin 时：

- `soft` 可以作为 advisory score，且不创建伪造的 per-device owner；
- `hard` 对相关 workload 保持 Pending，并返回 `XPUAssignmentNotEnforceable`；
- 管理员配置不得把 `hard` 静默改为 `soft`。

Mock Provider 可以验证规划、DeviceKey 和 annotation 恢复，但不能作为真实硬件 exact-ID 执行证据。真实 reservation、release
和失败注入验收属于后续 Exact 的发布条件。

Advisory MVP 与 Pod-derived Topology Alpha 都只有在 feature gate 和 plugin 同时启用后才存在；后续 Exact 不是 Alpha 的隐式升级，
必须单独满足 backend 和事务合同。

### 1.4 文档中的四种状态

后文使用下列标签，避免把提议写成当前实现：

- **当前实现**：仓库中已经存在且本设计核对过的行为；
- **可复用能力**：已有组件可以继续承担的所有权；
- **本文提议**：V4 要新增或修改的合同；
- **后续研究**：不进入首个 Pod-derived Topology Alpha 验收路径的能力；其中部分可在后续 Exact 实现。

## 2. 当前实现、可复用能力与缺口

### 2.1 当前实现

当前 Volcano 已经具有：

- `HyperNodeInfo` 的 Node/网络层次，以及 `network-topology-aware` 的 gradient/order；
- `JobInfo`、`SubJobInfo` 对 PodGroup/SubGroup policy、readiness 和 `AllocatedHyperNode` 的保存；
- `framework.OpenSession()` 调用 `SchedulerCache.Snapshot()` 构造一次 Session；
- `Statement.Allocate()`、`Discard()`、`SaveOperations()`、`RecoverOperations()` 和 `Commit()`；
- `SchedulerCache.AddBindTask()`、`executePreBinds()` 和逐 Pod `Bind()`；
- `framework.Session` 在新 Session 中可从已分配 Task 的非空 `NodeName` 恢复 `AllocatedHyperNode`；
- `DefaultBinder` 会把 `Task.PodAnnotations` 带入 Kubernetes Bind 的 Pod metadata，因此 scheduler-owned assignment annotation
  可以沿现有 Bind 路径持久化到 Pod；
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
8. 当前没有 `XPUTopologyAwareScheduling` feature gate、`xpu-topology-aware` plugin builder 或二者的启动组合校验；
9. 当前 `framework.Plugin` 只要求 `Name/OnSessionOpen/OnSessionClose`，现有 Session 也没有组级 topology plan、
   error-returning Statement participant 或 complete-batch PreBind 注册点。

因此，现有路径不是后续 Exact 所需的全组提交屏障；但它足以作为 Pod-derived Alpha 的逐 Pod Bind 基线。

### 2.2 可复用能力

| 现有组件 | 继续拥有 | xPU 不应接管 |
| --- | --- | --- |
| Queue/DRF/capacity/gang | 队列、资源公平性、Job/SubJob readiness | Device identity 和 exact allocation |
| HyperNode/network-topology-aware | Node/网络层次、network gradient 和 score | Device、Domain、Health、Reservation |
| Predicate/NodeInfo | Kubernetes Node 级资源、taint、volume、port 等可行性 | 单个 Domain 内的 Device membership |
| JobInfo/SubJobInfo | workload/group 上下文和 Task membership | Provider payload 与全局 Device owner |
| Statement | 本轮 Task/Node speculative mutation 的 owner | Provider 解析与硬件事实 |
| SchedulerCache | 集群 live state、一次 Session snapshot、bind handoff | 厂商 runtime 分配协议 |
| Provider/Device Plugin | 选中 ID 的消费/确认与 Pod assignment annotation | Domain/Fabric 规划和跨系统 durable reservation |

### 2.3 本文提议的新增所有权

~~~mermaid
flowchart LR
    Sources["Trusted topology publisher"] --> Provider["Provider parse and normalize"]
    Provider --> Live["Scheduler-owned topology cache"]
    Live --> Snapshot["ClusterInfo.DeviceTopology<br/>immutable Session view"]
    Snapshot --> Plugin["xPU filter, score and plan"]
    Plugin --> Statement["Existing Statement + Session-local group plan"]
    Statement --> Assignment["Pod BindContext + xpu-assignment"]
    Assignment --> Bind["Existing per-Pod Kubernetes Bind"]
    Bind --> Pods["Bound Pods: NodeName + assignment"]
    Pods --> Recover["Next Session derives Group anchor"]
    Recover --> Plugin
    Future["Post-Alpha durable Exact"] -.-> Assignment
~~~

关键所有权如下：

- Provider 只拥有 source ingestion 和 validation；
- 管理员 `ResourceTopologyDescriptor` 拥有 workload 可见的 DomainClass catalog；Provider 只能引用并规范化到该 catalog；
- topology live cache 拥有 canonical facts、descriptor view、readiness 和 indexes；本 Alpha 不拥有 allocation ledger；
- `ClusterInfo.DeviceTopology` 是 Session 的不可变只读视图；
- xPU plugin 做 policy compile、filter、score 和 side-effect-free plan；Session-local anchor/plan 只服务本轮调度；
- Statement 仍拥有普通 Task/Node mutation，并把最终 DeviceKeys 转换为每个 Pod 的 assignment annotation；
- 已绑定 Pod 是跨 Session anchor 的可恢复输入；可选 PodGroup 摘要只是快速索引，不是 allocation ledger；
- Provider/Device Plugin 负责消费或确认 assignment。跨系统 reservation、release 和 owner reconciliation 属于后续 Exact。

### 2.4 后续研究

下列能力不进入首个 Pod-derived Topology Alpha：

- DRA `claimName` Public API 和 ResourceSlice Provider；
- MIG、vGPU、共享/分数设备、多 Container、Init Container 的 allocation lifecycle；
- topology-aware victim selection 和 device-level pipeline 持久化；
- durable reservation/evidence、完整 batch PreBind、crash-safe release/reconcile 和 active-active fencing；
- 自动推导 Fabric、NCCL ring、厂商 link-bandwidth 规划；
- 无共享持久 reservation record 时的 active-active 多 scheduler exact reservation；

### 2.5 Feature gate、plugin 配置与实现合同

本节全部是**本文提议**。当前仓库已有 Volcano feature-gate plumbing、scheduler ConfigMap、plugin factory、
`framework.Plugin` 生命周期和 Predicate/NodeOrder 注册点，但尚未包含本节命名的 gate、plugin 或组事务扩展。

#### 2.5.1 双重启用与 fail-closed 规则

新增 Volcano Alpha feature gate：

~~~go
const XPUTopologyAwareScheduling featuregate.Feature = "XPUTopologyAwareScheduling"

var defaultVolcanoFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
    XPUTopologyAwareScheduling: {Default: false, PreRelease: featuregate.Alpha},
}
~~~

实现位置为 `pkg/features/volcano_features.go`。scheduler 与 admission webhook 都必须接收同一个
`--feature-gates=XPUTopologyAwareScheduling=true`；该 gate 不加入默认开启集合。scheduler 配置还必须在 `tiers[].plugins`
中显式加入 `xpu-topology-aware`。两个开关各自只拥有一类责任：

- feature gate 控制实验 API、webhook authoring、长生命周期 Provider/topology manager 和 framework 新路径是否可用；
- plugin 配置控制一次 Session 是否注册 xPU filter、score、group plan 与 exact transaction participant；
- gate 不能替代 plugin，plugin 也不能自行绕过 gate；未声明 `deviceTopology` 的 workload 不产生 xPU filter、score、plan 或 reservation。

启动/配置重载矩阵如下：

| Scheduler gate | `xpu-topology-aware` plugin | 结果 |
| --- | --- | --- |
| 关闭 | 未配置 | 功能关闭；不启动 Provider/topology manager，不改变普通 workload 调度行为 |
| 关闭 | 已配置 | 配置非法；首次启动失败，热更新拒绝并保留上一份已接受配置 |
| 开启 | 未配置 | 配置非法；scheduler 不进入 Ready，不能让 xPU policy 落入普通调度路径 |
| 开启 | 已配置 | 进入 xPU activation validation；Provider、catalog、assignment contract 和参数校验通过后才 Ready |

admission gate 关闭时，webhook 拒绝新建非空 policy，也拒绝给已有对象新增或修改 policy；只允许删除 policy。admission gate
开启也不是单独的 authoring 开关：webhook 必须读取与目标 schedulerName 对应的同一 scheduler ConfigMap（或由它原子派生的
read-only activation config），并复用相同的静态 validator。plugin 缺失、参数非法、config 不可读或 activation digest 不一致时，
非空 policy 仍被拒绝；webhook 不检查动态 Device 可用量、单 Node freshness 或 Adapter 实时健康。
scheduler core 还必须有一个不依赖 xPU plugin callback 的 typed-policy guard：gate/plugin 未形成有效组合时，任何已持久化的非空
`PodGroup.spec.deviceTopology` 都不可按普通 Job 调度。这是升级、回滚和误配置的最后 fail-closed 边界，不是第二个调度实现。
controller canonicalization 必须始终保留已持久化 intent 以支持 drain/删除，不能因为本进程 gate 关闭就把 policy 清空后生成一个
“无 topology 要求”的 PodGroup。

~~~mermaid
flowchart LR
    FG[Feature gate] --> V[Activation validator]
    PC[Scheduler tiers plugin entry] --> V
    AR[Plugin arguments] --> V
    V -->|invalid| NR[Startup not Ready or reject reload]
    V -->|valid| TM[Process-scoped topology manager]
    TM --> SV[Immutable Session topology view]
    SV --> PL[xpu-topology-aware callbacks]
    WG[Admission gate] --> WH[Webhook policy validation]
    PC -->|shared static activation config| WH
    WH --> PG[Canonical PodGroup policy]
    PG --> PL
~~~

feature gate 是进程启动参数，不能通过 scheduler ConfigMap 热更新。scheduler plugin 配置虽然可随配置文件热更新，
但移除 plugin、切换 Provider 或改变 identity namespace 都是 drain 操作：仍有已绑定 topology Pod 或正在进行的 semantic policy
mutation 时必须拒绝新配置并继续使用上一份配置。Alpha 不检查或恢复独立 reservation、allocation ledger 或 `ReconcilePending`。
禁用 gate 前必须先处理受保护的 topology Pod，再统一重启 scheduler 与 admission；各 scheduler replica 必须使用相同 gate、plugin
参数和配置 digest。规范化后的 activation digest 至少覆盖 gate 状态、plugin 参数、固定 catalog 是否可读和 Provider identity，
并进入 immutable topology view；后续 Exact 如增加 resource-to-Adapter owner，另行扩展 digest 和 drain 合同。

#### 2.5.2 scheduler 与 Helm 配置

`xpu-topology-aware` 不进入默认 `volcano-scheduler.conf`。下例是 Advisory 配置；完整文件必须保留集群原有 actions、tiers 和其他
plugin，不能把下面片段当成对默认配置的 merge：

~~~yaml
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
    enablePreemptable: false
  - name: conformance
- plugins:
  - name: overcommit
  - name: drf
    enablePreemptable: false
  - name: predicates
  - name: proportion
  - name: nodeorder
  - name: binpack
  - name: xpu-topology-aware
    enablePredicate: true
    enableNodeOrder: true
    arguments:
      xpu-topology.provider: annotation
      xpu-topology.provider-max-age: 2m
      xpu-topology.resource-owners: ""
      xpu-topology.max-search-states: 10000
      xpu-topology.max-candidate-domains: 64
      xpu-topology.max-planning-attempts: 3
      xpu-topology.planning-timeout: 50ms
~~~

Alpha 的 `resource-owners` 固定为空，表示 observation/assignment-confirmation only：`soft` 可打分，hard 只有在 Provider 能消费或
确认 scheduler-selected DeviceKey 且能保留 Pod assignment 时才可执行。跨系统 exact owner 配置不属于 Alpha；后续 Exact 可以另行
显式绑定 Adapter，例如：

~~~yaml
xpu-topology.resource-owners: "nvidia.com/gpu=hami-exact,huawei.com/Ascend910=ascend-exact"
~~~

上例中的 Adapter 名只是后续合同示例，不表示仓库当前已有 `hami-exact` 或 `ascend-exact`。未知 Adapter、一个 resource 多 owner、
Adapter 与 Provider identity contract 不一致或未声明目标 `DomainClassKey` capability 时，后续 Exact 配置必须 fail closed。
identity namespace 由 Provider/Adapter capability 报告并互相校验，不能由管理员用字符串强行声明为“相同”。

插件参数合同为：

| 参数 | Alpha 默认/要求 | 消费者与语义 |
| --- | --- | --- |
| `xpu-topology.provider` | 必填；首期 `annotation`，`mock` 仅测试 | process-scoped topology cache；选择事实输入和 assignment confirmer，不选择 allocation owner |
| `xpu-topology.provider-max-age` | `2m`，必须大于 0 | Provider/cache；超过 freshness 后 hard fail closed |
| `xpu-topology.resource-owners` | Alpha 必须为空；后续 Exact 才启用 | 后续 Adapter registry；Alpha 不解析或创建跨系统 owner |
| `xpu-topology.max-search-states` | `10000`，必须大于 0 | group planner；限制 bounded backtracking |
| `xpu-topology.max-candidate-domains` | `64`，必须大于 0 | group planner；限制每个 planning unit 展开的 Domain 数 |
| `xpu-topology.max-planning-attempts` | `3`，必须大于 0 | allocate integration；限制冲突后的完整 replan 次数 |
| `xpu-topology.planning-timeout` | `50ms`，必须大于 0 | group planner；超时返回 `XPUTopologyPlanningBudgetExceeded` |

这些是 scheduler plugin/runtime 参数，不承载 workload policy，也不承载具体 Device/Domain/Fabric ID。`domainClass` 只引用
第 5.1 节定义的 Alpha 固定 catalog；scheduler、webhook、controller、Provider 和后续 Adapter 必须读取同一份 class 定义，不能在
plugin arguments 中复制另一套 class 解释。

Helm 安装或升级需要同时传 scheduler/admission gate，并用完整 scheduler 配置覆盖文件：

~~~bash
helm upgrade --install volcano ./installer/helm/chart/volcano \
  --namespace volcano-system \
  --set-string custom.scheduler_feature_gates=XPUTopologyAwareScheduling=true \
  --set-string custom.admission_feature_gates=XPUTopologyAwareScheduling=true \
  --set-file custom.scheduler_config_override=./volcano-scheduler-xpu.conf
~~~

不使用 Helm 时，对 scheduler 与 webhook-manager Deployment 同时增加同名 `--feature-gates` 参数，并在 scheduler 挂载的
`volcano-scheduler.conf` 中加入上述 plugin。feature gate 只让代码路径可用，不会自动修改 ConfigMap 或插入 plugin。

#### 2.5.3 Plugin 构造、生命周期与注册

建议实现目录为 `pkg/scheduler/plugins/xpu-topology-aware/`，Go package 可命名为 `xputopologyaware`，并在
`pkg/scheduler/plugins/factory.go` 注册 builder：

~~~go
// pkg/scheduler/plugins/xpu-topology-aware/
const PluginName = "xpu-topology-aware"

func New(arguments framework.Arguments) framework.Plugin
func ValidatePluginOption(option conf.PluginOption) error

func (p *xpuTopologyAwarePlugin) Name() string
func (p *xpuTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session)
func (p *xpuTopologyAwarePlugin) OnSessionClose(ssn *framework.Session)

// pkg/scheduler/plugins/factory.go
framework.RegisterPluginBuilder(xputopologyaware.PluginName, xputopologyaware.New)
framework.RegisterPluginConfigValidator(
    xputopologyaware.PluginName,
    xputopologyaware.ValidatePluginOption,
) // proposed
~~~

`Name/OnSessionOpen/OnSessionClose` 是当前 `framework.Plugin` 必须实现的方法，`New(Arguments) Plugin` 是当前 builder 形状。
`RegisterPluginConfigValidator`/`ValidatePluginOption` 是本文提议的新启动校验能力：当前 `New` 不能返回 error，而且每个 Session 都会
重新构造 plugin，因此不能把配置合法性、gate/plugin 组合或 drain 校验藏在 `OnSessionOpen`。首次启动在开放任何 Session 前校验；
热更新先校验完整候选配置，成功后才原子替换，失败时保留上一份配置。

长生命周期 Provider、topology cache 和 immutable snapshot publisher 由 process-scoped topology manager 拥有，不能由每个 Session 的
`New` 重复创建。Alpha 没有 scheduler-side ledger、Adapter registry 或 external recovery worker。scheduler 配置加载器从同一份
plugin arguments 构造并校验 manager config；
`OnSessionOpen` 只能取得 `ClusterInfo` 已配对的一份 immutable topology view，不能 type-assert cache 后做第二次 snapshot。

#### 2.5.4 Plugin 必须注册/实现的调度函数

`OnSessionOpen` 的最小形状如下；JobValid、Predicate、BatchNodeOrder 与 EventHandler 是当前 framework 可复用能力，
GroupTopologyPlan 是 Alpha 需要新增并评审的最小组规划合同；TopologyReserve 留给后续 Exact：

~~~go
func (p *xpuTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session) {
    p.view = ssn.DeviceTopologyView() // proposed; paired with ClusterInfo snapshot

    ssn.AddJobValidFn(p.Name(), p.jobValid)
    ssn.AddPredicateFn(p.Name(), p.predicate)
    ssn.AddBatchNodeOrderFn(p.Name(), p.batchNodeOrder)

    ssn.AddGroupTopologyPlanFn(p.Name(), p.planGroup)       // proposed Alpha hook

    ssn.AddEventHandler(&framework.EventHandler{
        AllocateFunc:   p.onAllocate,
        DeallocateFunc: p.onDeallocate,
    })
}
~~~

| 函数/回调 | 是否已有 framework 注册点 | 必须承担的责任 | 明确不能做 |
| --- | --- | --- | --- |
| `ValidatePluginOption` | 否，需新增 config validator | 严格解析参数，要求 `enablePredicate/enableNodeOrder=true`，校验 gate/plugin 组合所需的静态配置；unknown key/value 返回 error | 启动 Provider、访问 live allocation state、依赖某次 Session |
| `jobValid` | 是，`AddJobValidFn` | 编译 canonical PodGroup policy，校验 catalog/class、请求形状和 hard enforceability，返回结构化 reason | 复制 gang readiness 或把 hard 改成 soft |
| `predicate` | 是，`AddPredicateFn` | 在普通 Predicate 候选上复核 NodeUID、freshness、health、Domain/Fabric membership 与 hard exact feasibility | 只看 Node aggregate xPU 数量，或重新引入已被普通 Predicate 排除的 Node |
| `batchNodeOrder` | 是，`AddBatchNodeOrderFn` | 只对 `soft` policy 的 Domain/Fabric preference 和 internal Compact 产生确定性附加分；共享同一 immutable view | 用 score 实现 hard filter，或覆盖其他 plugin 分数 |
| `planGroup` | 否，需新增 `AddGroupTopologyPlanFn` | 对当前 AdmissionSet 做 side-effect-free Node/Domain/Device plan，并遵守已恢复 anchor 和 search budget | reserve ID、调用 Adapter、修改 Statement/NodeInfo live state |
| `onAllocate/onDeallocate` | 是，`AddEventHandler` | 维护 Session-local overlay，使同一 Session 后续 plan 看见 tentative Allocate/Deallocate | 把普通 Allocate event 当成已绑定 Pod 的 assignment evidence |
| `OnSessionClose` | 是，`framework.Plugin` | 丢弃 Session-local compiled policy、score cache、anchor 和 overlay | 停止 process Provider，或释放不存在的全局 reservation |

`planGroup` 的建议签名为：

~~~go
type GroupTopologyPlanFn func(
    context.Context,
    *GroupTopologyPlanContext,
) (*TopologyPlacementPlan, error)

type GroupTopologyPlanContext struct {
    Group             GroupRef
    AdmissionSet      []*api.TaskInfo
    CandidateNodes    map[api.TaskID][]*api.NodeInfo
    AssignedNodes     map[api.TaskID]*api.NodeInfo // finalization 时固定
    NodeResourceState map[string]*api.NodeInfo     // planner-owned clones
    Anchor            *PodDerivedGroupAnchor
    Topology           *DeviceTopologySnapshot
}
~~~

`CandidateNodes` 只能来自 Queue/NodeShard/HyperNode/普通 Predicate 已产生的候选交集；`AssignedNodes` 非空时，final plan 必须使用这些
固定 Node。返回 plan 必须覆盖整个 AdmissionSet，否则返回 error 且不进入 Bind。allocate action 负责选择调用时机，
plugin 不接管 task iterator、gang readiness 或 `Statement` operation ownership。

当前 `BindContextHandler.SetupBindContextExtension` 是 per-Pod、无 error 返回的附加接口。Alpha 使用它或等价的 BindContext
扩展把 scheduler-owned assignment annotation 带入 Pod Bind；它不负责跨 Pod rollback，也不声称多 Pod Bind 原子性。后续 Exact
再增加 handoff/batch contract，不能把 Alpha 的逐 Pod Bind解释成完整事务。

`xpu-topology-aware` 不注册第二个 `HyperNodeGradientForJobFn/SubJobFn`。当前该路径是 first-plugin-wins，无法与
`network-topology-aware` 安全组合；xPU 通过普通候选后的 hard predicate 和可组合的 group plan 缩小集合。它也不注册
`JobReadyFn` 复制 gang 语义：Job/SubJob readiness 仍由现有 gang plugin 拥有。

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

type DeviceTopologyDomainClass string

type DeviceTopologyDomainSelector struct {
    Scope       DeviceTopologyDomainScope `json:"scope"`
    DomainClass DeviceTopologyDomainClass `json:"domainClass"`
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
- `scope` 只接受 `Node/Fabric`；`domainClass` 必填，并满足 API review 后冻结的 DNS-label 风格语法；
- V4 Go/canonical schema 不暴露 `tier/tierName`；served OpenAPI 仅保留 reject-only legacy tombstone 字段并用 CEL 拒绝，
  annotation 使用 strict decoding，不能 prune 后按默认 class 调度；
- `scope=Fabric` 只匹配 Provider 明确发布的 Fabric；HyperNode 或 IB/RoCE 可达性不能隐式满足；
- `domainClass` 必须解析到同一 `resourceName + scope` 的管理员 catalog；未知 class 对 hard/soft 都是 authoring error，已知但
  不支持 exact 执行的 hard class fail closed；
- `podSelector` 只允许在 PodGroup 级 `applyTo=Pod`；
- 同一作用单元的 hard policies 使用 AND；soft policies 只累计有证据的 preference；
- 同一作用单元不得对同一 `resourceName + applyTo + scope` 的重叠 Pod 集合提交不同 `domainClass`；首个 Alpha 将其作为
  冲突拒绝，而不是猜测两个 class 的相交或把其中一个当 fallback；
- canonicalization 按完整 normalized policy stable sort，并折叠 exact duplicates；Pod selectors 重叠且约束相同时，每个 Task
  只生成一份 `ApplyTo + DomainClassKey` selection，约束不同时按冲突拒绝。Group policy 不允许 selector，因此去重后每个
  `DomainClassKey` 至多对应一个 Group anchor selection；
- 未配置 `DeviceTopology` 时，当前调度行为不变。

#### `scope`、`domainClass` 与具体 Domain identity

三个概念处于不同层次：

| 字段/对象 | 回答的问题 | 示例 | 是否唯一 identity |
| --- | --- | --- | --- |
| `scope` | Domain 的身份、ownership 与 membership 边界在哪里 | `Node`、`Fabric` | 否 |
| `domainClass` | workload 在该边界内需要哪一类稳定拓扑合同 | `local-scale-up`、`pcie-root`、`scale-up-fabric` | 否 |
| `LocalDomainKey/FabricKey` | Planner 最终选择了哪个具体 Domain 实例 | `node-a/hccs-0`、`fabric-f1` | 是 |

`scope=Node` 只说明成员不得跨出一个 NodeUID；它本身不能区分同一 Node 上的 HCCS/NVLink island 与 PCIe root。
`scope=Fabric` 只说明成员来自 Provider 显式发布的跨 Node Fabric；它本身不能区分 scale-up、storage 或其他 Fabric 类别。
`domainClass` 因此只在 `(resourceName, scope)` 内解释，且多个具体 Domain 可以共享同一个 class。workload 不能在
`domainClass` 中填写 `hccs-0`、`fabric-f1` 等实例 ID；具体实例只能由 Planner 选择并写入 plan/anchor。

完整的类别身份是：

~~~go
type DomainClassKey struct {
    ResourceName corev1.ResourceName
    Scope        DeviceTopologyDomainScope
    Name         DeviceTopologyDomainClass
}
~~~

例如，以下三个 key 互不相同：

~~~text
{nvidia.com/gpu,       Node,   local-scale-up}
{huawei.com/Ascend910, Node,   local-scale-up}
{nvidia.com/gpu,       Fabric, scale-up-fabric}
~~~

Alpha catalog 中的 class 定义固定不变；它不属于 `DomainClassKey`，也不能把 `domainClass` 裸字符串提升为全局 ID。

#### `applyTo` 的量词与作用单元

`domain.scope/domainClass` 回答“在哪个身份边界内使用哪一类 Domain”，`applyTo` 回答“哪些 workload 成员必须共同满足
该拓扑约束”。它们是正交维度，不能用 `scope=Node/Fabric` 推导 `applyTo=Pod/Group`。

对一条 policy 选中的 Pod 集合 `S`，以及 Domain `d` 的显式成员集合 `Members(d)`：

- `applyTo=Pod`：`∀ p ∈ S, ∃ d_p: Devices(p) ⊆ Members(d_p)`。每个 Pod 独立选择自己的
  `d_p`，不同 Pod 可以位于不同 Node、LocalDomain 或 Fabric；
- `applyTo=Group`：`∃ d, ∀ p ∈ S: Devices(p) ⊆ Members(d)`。整个作用单元共享同一个
  Domain 选择；`scope=Node` 时意味着同一 Kubernetes Node 和同一本地 Device Domain，`scope=Fabric` 时意味着
  所有成员都属于同一个显式 Fabric。Fabric policy 自身只限制 Fabric member local Domains 的设备并集；只有另外声明
  `applyTo=Pod + scope=Node` policy 时，才同时要求每个 Pod 的设备落在一个指定 class 的本地 Domain。

policy 所在字段决定 Group 的外部边界，`applyTo` 决定该边界内是否联合求解：

| policy 所在位置 | `applyTo` | 作用单元 |
| --- | --- | --- |
| `PodGroupSpec.DeviceTopology` | `Pod` | 对 PodGroup 中每个 selector 命中的 Pod 独立应用 |
| `PodGroupSpec.DeviceTopology` | `Group` | 对整个 PodGroup 联合应用 |
| `SubGroupPolicySpec.DeviceTopology` | `Pod` | 对每个实际 SubGroup 中的 Pod 独立应用 |
| `SubGroupPolicySpec.DeviceTopology` | `Group` | 对每个实际生成的 SubGroup 分别联合应用 |

四种组合都有明确语义：

| `applyTo` | `scope` | 约束语义 |
| --- | --- | --- |
| `Pod` | `Node` | 每个 Pod 分别选择一个同 class 的 Node-local Domain；不同 Pod 可选择不同 Node/Domain |
| `Group` | `Node` | 整个作用单元共享一个具体 Node-local Domain，因此也共享一个 NodeUID |
| `Pod` | `Fabric` | 每个 Pod 分别选择一个同 class 的显式 Fabric；不同 Pod 可选择不同 Fabric |
| `Group` | `Fabric` | 整个作用单元共享一个具体显式 Fabric，可分布在该 Fabric 的多个 Node 上 |

例如，分布式训练可以同时声明 `applyTo=Pod + scope=Node + domainClass=local-scale-up`，要求每个 Pod 内的多张设备
来自一个本地高速互联 Domain；再声明 `applyTo=Group + scope=Fabric + domainClass=scale-up-fabric`，要求所有训练 Pod
位于同一个 scale-up Fabric。只保留前一条会允许
不同 Pod 漂移到不同 Fabric；把前一条错误提升为 Group 约束，又会不必要地要求所有 Pod 位于同一 Node-local Domain。

`applyTo=Group` 不改变现有 `JobReady/SubJobReady`、`minMember` 或 SubGroup readiness。`AdmissionSet` 仍是
winning Statement 本轮新增的 Allocate operations；当 `minMember < replicas` 时，首次成功 Bind 后，后续 Session 从已绑定 Pod 推导稳定
`TopologyGroup` anchor，后续 admission wave 必须继续满足同一 anchor。Group 因此不等于
“一次 Statement 中的 Pod 集合”，也不要求一次调度全部 replicas。

### 3.2 Public API 不暴露 allocationStrategy

V4 继续不暴露 Public `allocationStrategy`/`allocationPolicy`。内部唯一策略是 deterministic Compact：

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
        "domain": {"scope": "Fabric", "domainClass": "scale-up-fabric"}
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
这个 Condition 只是 fail-closed validation gate，不是第二份 policy；scheduler 的 workload topology 语义仍只来自 canonical spec，
并且绝不把用户 workload annotation 当成运行时 assignment。用户的 `scheduling.volcano.sh/device-topology` 不能携带 Device ID、
reservation token 或 Adapter handoff；scheduler/provider 另用受控的 `volcano.sh/xpu-assignment` Pod annotation 记录已选 DeviceKeys，
只在成功 Bind 的 Pod 上作为恢复输入。

### 3.5 Policy 更新合同

每份 canonical spec 计算 `PolicyFingerprint`。规范化必须包括 default 后的 `resourceName/mode/applyTo/scope/domainClass`、
排序后的 policies 和 label selector，但不能包括对象 `resourceVersion` 或其他存储元数据。Alpha catalog 固定不变，policy compile
直接使用这份固定 class 定义；plan/evidence 只需记录实际选择的 class、Domain/Fabric 和 membership。

语义更新只在 Group 没有已绑定成员时允许。Alpha 不查独立 reservation ledger，而是以已绑定/运行 Pod 作为 anchor activity 的可观测事实：

~~~text
没有已绑定/运行成员 Pod
AND 没有已经进入本轮 Bind 的成员
~~~

规则如下：

1. 没有已绑定成员时允许更新；新的 policy fingerprint 使旧 plan、fit cache 和 placement hint 全部失效；
2. 已有已绑定成员后，只允许 canonical fingerprint 完全相同的 no-op 更新；
3. 活动状态下修改 `mode/resource/scope/domainClass/applyTo/selector` 必须由 webhook 或 controller 拒绝；
4. 删除 policy 也是语义更新，不能借删除绕过 drain；
5. VCJob 和生成 PodGroup 必须使用 UID/resourceVersion CAS 保持同一 fingerprint；
6. Pod template rollout 产生不同 annotations 时，旧 PodGroup 不原地切换策略；controller 应创建新的作用单元或要求用户先 drain；
7. Pod cache 读取不一致、assignment annotation 缺失或 Node replacement 时按“不可恢复”处理，保持 hard Group Pending，而不是允许更新或猜测。

## 4. ID、Node 重建与失效

### 4.1 三层名称不能混用

V4 明确区分：

| 层 | 示例 | 语义 |
| --- | --- | --- |
| Provider identity | `annotation-v1` | 哪个启用的 source contract 发布事实 |
| Source-local value | `GPU-4c2e`、`nvlink-0`、`fabric-0` | payload 中稳定但有明确作用域的值 |
| Canonical scheduler key | 下面定义的 `DeviceKey/LocalDomainKey/FabricKey` | cache、plan、Pod assignment 和 fingerprint 使用的完整身份；后续 ledger 也复用 |

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
- `DomainClassKey` 是 `(ResourceName, Scope, Name)` 的稳定类别合同；多个具体 Domain 可以共享同一个 class，class 不是 Domain identity；
- source generation、fabric generation 和 snapshot revision 是版本，不是 ID；
- 数组下标、可漂移 GPU index、NodeName、HyperNodeName 都不能单独作为 Device/Domain/Fabric 身份。

NodeName 只用于查询、日志和 Kubernetes Bind；所有 assignment、membership 和 plan 比较使用 NodeUID。

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
    Class                 DomainClassKey
    NodeName              string // diagnostic only
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
    Class                 DomainClassKey
    OwnerNodeUID          types.UID
    SourceGeneration      uint64
    Members               []FabricMember
    MembershipFingerprint string
}
~~~

`MemberDeviceKeys/MemberDomainKeys` 是 source-normalized 的显式 membership；`EffectiveDeviceKeys` 是 Normalizer 校验
无环和去重后派生的 closure。一个 Device 可以同时属于不同 class 的重叠 local Domains，例如同属
`local-scale-up` 和 `pcie-root`；每个具体 Domain 对象只携带一个 `Class`。Fabric 也必须携带 class，其 membership 必须
显式指向 member local Domains，不能从 Node/HyperNode 关联推断。Normalizer 必须校验 `Class.ResourceName` 与对象 key 的
resource 相同，且 `Class.Scope` 对 `DeviceDomain` 为 `Node`、对 `FabricDomain` 为 `Fabric`。

可选 `DeviceLink{Source, Target, Score, LinkClass}` 只用于有证据的 soft score/诊断。缺边、非对称边或厂商 link 名称不能
推导 hard Domain；首个 Alpha 的 hard 正确性必须在完全没有 Link 数据时仍成立。

### 4.4 Node 删除和同名重建

当 `node-a` 的 UID 从 `uid-old` 变为 `uid-new`：

1. 在 `SchedulerCache.Mutex` 下先把 `uid-old` 从 candidate indexes 移除；
2. 旧 UID 的 provider readiness 不得转移给新 UID；新 UID 从 `Pending` 开始；
3. 旧 UID 的 facts 和 candidate index 失效；已绑定 Pod 若携带旧 NodeUID 的 assignment，只能作为“不可恢复”诊断输入；
4. Alpha 不维护独立的 old-UID allocation tombstone；后续 Exact 如需在 Pod 消失后保护外部 owner，再单独定义 tombstone/release 合同；
5. 新 UID 不继承旧 UID 的 source generation、Domain membership、Health 或 Free 状态；
6. 迟到的旧 UID `ReplaceFacts/ClearFacts` 即使 NodeName 相同也必须拒绝；
7. 新 Node 完成一次有效 `ReplaceFacts` 或 `ClearFacts` 后，readiness 才从 `Pending` 进入 `Synced`；
8. 包含旧 UID 的 Fabric 立即不可用，Provider 必须以更新 generation 重新发布并解析完整成员集合；
9. 不能把旧 UID 的 assignment annotation 或 anchor 映射到新 UID；相关 Group hard 调度保持 Pending。

~~~mermaid
stateDiagram-v2
    state replacement <<fork>>
    [*] --> OldActive: Node uid-old observed
    OldActive --> OldTombstone: Node deleted
    OldActive --> replacement: same name uid-new observed
    replacement --> OldTombstone: retain old Pod references for diagnosis
    replacement --> NewPending: create new identity
    OldTombstone --> [*]: old Pods gone or new plan selected
    NewPending --> NewSynced: valid ReplaceFacts or ClearFacts
    NewPending --> NewPending: invalid or delayed old-UID update
~~~

## 5. Provider、Fabric 与 Cache

### 5.1 ResourceTopologyDescriptor 与 DomainClass catalog

`domainClass` 是管理员面向 workload 发布的稳定合同。workload author、Node annotation publisher 和 Adapter 都不能各自解释
同一个裸字符串。Alpha 直接固定一份 scheduler-side catalog；webhook、controller、Provider normalizer、scheduler 和 Adapter
都读取这份相同的 class 定义。Alpha 不支持运行期间修改该定义，后续如需增加或改变 class，另开设计和兼容性评审：

~~~go
type DomainClassDescriptor struct {
    Key         DomainClassKey
    Description string
}

type ResourceTopologyDescriptor struct {
    ResourceName corev1.ResourceName
    Classes      []DomainClassDescriptor
}
~~~

管理员侧示例：

~~~yaml
resourceName: nvidia.com/gpu
domainClasses:
  - scope: Node
    name: local-scale-up
    description: Node-local high-bandwidth device domain
  - scope: Node
    name: pcie-root
    description: Devices behind one PCIe root complex
  - scope: Fabric
    name: scale-up-fabric
    description: Explicit cross-node scale-up fabric
~~~

合同如下：

1. `(resourceName, scope, name)` 唯一标识一个 class；`name=local-scale-up` 可以在 NVIDIA 与 Ascend descriptor 中分别出现；
2. Provider 可以把 `NVLink island`、`HCCS domain` 等 source facts 规范化为管理员声明的 `local-scale-up`，但不能临时创造 catalog 外 class；
3. Alpha descriptor 不含数字 rank；hard policy 只匹配 workload 明确指定的 class，不隐式扩大到其他 class；
4. policy compiler 找不到 class 时返回 `XPUTopologyDomainClassUnknown`；class 已知但当前 Provider capability 不能满足
   hard assignment 执行时返回 `XPUTopologyDomainClassUnsupported` 或更具体的 `XPUAssignmentNotEnforceable`；
5. catalog 内容在 Alpha 生命周期内不可变；所有组件使用同一份固定定义。将来如需允许多个候选 class，应另行评审
   `acceptableDomainClasses` 等显式 API，不能通过修改现有 class 含义或静默 fallback 实现。

`Description` 是管理员与受信 Provider 之间的语义合同，不是 scheduler 可从图中自行证明的带宽标准。Normalizer 能验证的是
catalog 引用、resource/scope、显式 membership、closure 与 ID 一致性；Provider validation 可验证厂商事实。管理员配置和受信
publisher 是 class 语义的信任根，V4 不声称仅凭 `name=local-scale-up` 能推导或测量 NVLink/HCCS 性能。

V4 不接受 V3 的 workload `tier/tierName`。迁移必须由用户/controller 显式把旧类别映射成 catalog 中的 `domainClass`，创建
新的 canonical policy。因为 structural schema 可能在 admission webhook 前 prune 未知字段，不能只从 Go type 删除旧字段：
任何承载该 policy 的 served schema（包括首个 V4 实现；若曾试发 V3，也包括其 served version）必须在对应
PodGroup/VCJob 路径保留仅用于拒绝的 legacy tombstone 字段，并以 CEL
`!has(self.tier) && !has(self.tierName)` 永久拒绝；Go Public type 和 canonical model 不暴露它们。不能依赖客户端
`fieldValidation=Strict`，也不能让 pruning 把旧 policy 变成缺省策略。

### 5.2 Provider update contract

~~~go
type ProviderIdentityRef struct {
    ProviderID string
    Namespace  string
}

type ProviderNodeKey struct {
    ProviderID  string
    Namespace   string
    ResourceName corev1.ResourceName
    NodeUID     types.UID
}

type TopologyProviderCapabilities struct {
    Identity     ProviderIdentityRef
    ResourceName corev1.ResourceName
    DomainClasses []DomainClassKey
}

type ProviderNodeUpdate struct {
    ProviderID          string
    IdentityNamespace   string
    ResourceName        corev1.ResourceName
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
`ProviderNodeKey{ProviderID, Namespace, ResourceName, NodeUID}` 的 readiness 是 `Pending | Synced`：

- `ReplaceFacts` 是该 source 对当前 Node 的完整事实替换；
- `ClearFacts` 表示 Provider 已检查该 identity contract 在该 Node 上没有 inventory，它只能清除同一 `ProviderNodeKey` 并完成其初次同步；
- invalid payload 不能完成初次同步，也不能覆盖最后一次有效事实；
- 同一 `ProviderNodeKey` 的 source generation 对 topology content 单调；相同 generation 只允许内容完全相同的 heartbeat；
- Kubernetes `resourceVersion` 是 opaque 字符串，只做“是否仍等于当前观察对象”比较，不做数值排序；
- Provider 时间戳用于 freshness，不用于 Node update 排序。

Provider 注册时的 `TopologyProviderCapabilities.DomainClasses` 必须是 catalog 的子集且 resource/scope 完全匹配，不允许
通配符。catalog 已知但没有启用的 Provider 声明某 class 时是 `XPUTopologyDomainClassUnsupported`；Provider 已声明支持、但某
Node 的事实仍 Pending/stale 时分别是 `XPUTopologyDataNotReady/XPUTopologyStale`；事实有效但没有足够容量时才进入
fragmented/unavailable 判断。这样“实现不支持”和“当前没有资源”不会使用同一个 reason。

Provider payload 中的 class 名是对管理员 catalog 的引用。以 Ascend Node-local facts 为例：

~~~json
{
  "resourceName": "huawei.com/Ascend910",
  "devices": [
    {"id": "npu0", "health": "Healthy"},
    {"id": "npu1", "health": "Healthy"}
  ],
  "localDomains": [
    {
      "id": "hccs-0",
      "domainClass": "local-scale-up",
      "deviceIDs": ["npu0", "npu1"]
    }
  ]
}
~~~

Normalizer 必须把 payload 的 `resourceName + Node scope + domainClass` 解析为 `DomainClassKey`，再生成
`DeviceDomain{Key, Class, ...}`。未知 class、scope 不匹配或 class membership 非法的 payload 不能完成初次同步，也不能覆盖最后一次
有效事实。V4 不允许 Provider 用“第 0 层”等本地序号代替 class。

Annotation payload 由受信 publisher 写入 Node；workload ServiceAccount 和 scheduler ServiceAccount 不得有该 key 的写权限。
由于 Node patch RBAC 不能限制单个 annotation key，部署必须使用 ValidatingAdmissionPolicy 或 webhook 对 publisher identity
做 fail-closed 限制。

### 5.3 Annotation Fabric：单 owner、完整声明

Alpha 的一个 `FabricKey` 只有一个 authoritative owner Node。owner Node 的 annotation 必须发布该 Fabric 的完整集合：

~~~json
{
  "id": "rack-1/fabric-0",
  "resourceName": "nvidia.com/gpu",
  "domainClass": "scale-up-fabric",
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

owner payload 的 `domainClass` 同样必须解析为 descriptor 中
`DomainClassKey{ResourceName: nvidia.com/gpu, Scope: Fabric, Name: scale-up-fabric}`，并写入
`FabricDomain.Class`。这条 canonical 匹配链路使 workload 的 `scope=Fabric + domainClass=scale-up-fabric` 可执行；不能再像
V3 一样只有 workload 类别字段而 `FabricDomain` 无对应 class。

这取代早期草案中“每个成员 Node 发布局部 `fabricMembership` 再合并 version vector”的 Annotation Alpha 路径。
未来 cluster-scoped Fabric CRD/Provider 可以采用不同 authority，但必须输出同一 canonical `FabricDomain`。

### 5.4 Canonical facts 与 Pod assignment 分离

~~~text
Topology facts
  Device/Domain/Fabric identity, membership, source generation, freshness, observed health

Bound Pod assignment
  PodUID, NodeName/NodeUID, resourceName, provider, canonical DeviceKeys

Session view
  ClusterInfo 中的一份 immutable DeviceTopologySnapshot + Session-local Group anchor/plan overlay
~~~

`Health` 与 Pod assignment 正交。Alpha 的可规划设备判断至少要求：

~~~text
Health == Healthy && ProviderReadiness == Synced && DeviceKey 属于当前 NodeUID/topology snapshot
~~~

才是 Alpha 可规划候选。已绑定 Pod 的设备不会因为一个 Session 内存结束而被重新选择；下一轮候选仍由现有 Kubernetes/provider
资源视图决定。Alpha 不用 topology cache 二次扣减普通 Node 资源，也不把 source 中“存在且健康”解释成外部 reservation 的 Free 证明。

### 5.5 DeviceTopologySnapshot 进入 ClusterInfo

**本文提议**在 `api.ClusterInfo` 增加：

~~~go
type ClusterInfo struct {
    // existing fields omitted
    DeviceTopology *DeviceTopologySnapshot
}
~~~

并在 `framework.Session` 保存同一指针或其只读引用。所有 map、slice、set 和对象在 publish 后不可变；下一次 Provider
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
- planning 全程使用 `ClusterInfo.DeviceTopology.Revision`；Bind 前只重新校验 plan 引用的 NodeUID、DeviceKey 和 membership，不创建外部 reservation。

### 5.6 Provider 重活、publish 与锁序

“进入同一次 Snapshot”只有在 publish 与 Node identity 也被协调时才成立。V4 的固定路径是：

~~~text
Node informer event
  -> enqueue existing Node work
  -> under SchedulerCache.Mutex record current NodeName/UID/resourceVersion
     and mark a replacement UID as Pending while invalidating old candidate facts
  -> release all locks
  -> parse, normalize and build graph closure
  -> when a Provider validation hook exists, validate topology facts outside locks
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
immutable pointer，不再获取 topology write lock。Provider parse、Normalizer 重活、外部 RPC 和持久 I/O 都在两把锁外执行。
Provider 校验失败只会使相应 facts 不可用；已通过 Provider/canonical 校验的 facts 仍可发布给 `soft` advisory。Alpha 不把 Adapter
或 reservation ledger 作为 topology ingestion 的硬前置。后续 Exact 若增加 allocation ledger，必须另行遵守上述锁序，并且绝不能
在持锁期间调用 Adapter。

如果锁外工作结束后 UID/resourceVersion 或 source generation 已变化，结果直接丢弃并重新入队；不能把旧 parse 结果发布给新 Node。
Node identity 的 Pending/invalidation 和 `sc.Nodes` 可见性必须在同一主锁下更新，使任何 Session 至多看到：

- 旧 Node + 旧 UID 的有效 view；或
- 新 Node + 新 UID 的 `Pending` view；

不能看到新 Node 与旧 UID topology 的组合。

### 5.7 Snapshot 内容与最终 revalidation

`DeviceTopologySnapshot` 至少包含：

~~~go
type DomainRef struct {
    Scope          DeviceTopologyDomainScope
    LocalDomainKey *LocalDomainKey
    FabricKey      *FabricKey
}

type DeviceTopologySnapshot struct {
    Revision             uint64
    PublishedAt          time.Time
    NodeIdentities       map[string]NodeIdentity
    ProviderReadiness    map[ProviderNodeKey]ProviderNodeSyncState
    Devices              map[DeviceKey]TopologyDevice
    LocalDomains         map[LocalDomainKey]DeviceDomain
    Fabrics              map[FabricKey]FabricDomain
    DomainClasses        map[DomainClassKey]DomainClassDescriptor
    DomainsByClass       map[DomainClassKey][]DomainRef
    ProvidersByClass     map[DomainClassKey][]ProviderIdentityRef
    DeviceToDomains      map[DeviceKey][]LocalDomainKey
    NodeToDomains        map[types.UID][]LocalDomainKey
    SchedulableByDomain  map[LocalDomainKey][]DeviceKey
}
~~~

`DomainRef` 是 snapshot 内部带判别字段的只读引用：`Scope=Node` 时只允许 `LocalDomainKey`，`Scope=Fabric` 时只允许
`FabricKey`。也可以在实现中拆成 `LocalDomainsByClass` 与 `FabricsByClass` 两个强类型索引，但不能每次规划再扫描并猜测类别。
所有 index 必须只包含 class、resource 和 scope 一致且已通过 descriptor 校验的对象，并按 canonical key 稳定排序。
`ProvidersByClass` 来自已启用 Provider 的受控 capability 注册，不从当前是否恰好有一个 Domain 对象反向猜测支持能力。
固定 catalog 不在运行期间切换；catalog 缺失或非法时 topology manager 不 Ready，不能用另一份 class 定义继续规划。

Plan 记录全局 `Revision` 用于诊断，并校验实际引用对象的 membership、source generation 和 NodeUID。提交时不要求整个 `Revision`
完全相等；Alpha 在 Bind 前只重新验证 plan 引用对象，避免无关 Node heartbeat 使所有计划失效；后续 Exact 再增加 final reservation。

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
- xPU context 只保存该 Group 的 canonical policy fingerprint、Session-local placement anchor 和 assignment plan；跨 Session 的恢复
  输入来自已经绑定的 Pod。

`AdmissionSet` 是 winning Statement 中本轮新增 Allocate operations 的完整逻辑视图：

~~~text
AdmissionSet(statement, group)
  = 该 statement 中属于 group 的全部新增 Allocate operations
~~~

Alpha 的规则是：

1. Group 尚未 Ready 时，包含本轮使 `JobReady/SubJobReady` 成立的确定性集合；
2. Group 已 Ready 时，包含本轮实际新增的全部 Allocate operations，不能退化为空；
3. 已绑定/运行且有可恢复 assignment 的成员是固定约束，不重新选择 Node/Domain/Fabric；
4. `AdmissionSet` 中任一 Task 缺少完整 plan，则整个集合不进入 Bind；
5. `SaveOperations/RecoverOperations/Merge` 只复制可试算的 plan context，不能复制外部 token、participant 或 reservation owner。

### 6.2 Pod-derived Group anchor

Pod-derived group anchor 防止 `minAvailable < replicas` 时不同 admission wave 漂移到另一个 hard Fabric/Domain。它是从已绑定 Pod
重建的 Session 内存对象，不是需要单独持久化的 Group 资源：

~~~go
type PodDerivedGroupAnchor struct {
    GroupRef        TopologyGroupRef
    ResourceName    corev1.ResourceName
    Scope           DeviceTopologyDomainScope
    Class           DomainClassKey
    LocalDomainKey  *LocalDomainKey
    FabricKey       *FabricKey
}

type PodXPUAssignment struct {
    Version      int                  `json:"version"`
    ResourceName corev1.ResourceName  `json:"resourceName"`
    Provider     string               `json:"provider"`
    DeviceKeys   []DeviceKey          `json:"deviceKeys"`
}
~~~

每条 `applyTo=Group` hard policy 恰有一个 `PodDerivedGroupAnchor`；`Class.Scope` 决定使用 LocalDomainKey 还是 FabricKey。
一个 Group 可以同时对不同 resource/scope 计算多个 Session-local anchor，但每个已绑定 Pod 只保存一份该 resource 的 assignment。

生命周期：

- 首 wave 没有已绑定成员时，anchor 只存在于当前 Session 的 plan；不提前写 PodGroup；
- Pod Bind 成功后，assignment annotation 与 `spec.nodeName` 成为后续 Session 的恢复输入；
- 后续 AdmissionSet 必须先恢复并满足同一 anchor；
- hard policy 的 anchor 无法无歧义恢复时 fail closed，不能静默选择第二个 Fabric；
- policy fingerprint 不同的 plan 不能复用当前 Session 的旧 anchor；已有绑定成员后 semantic mutation 直接拒绝；
- NodeUID、DeviceKey、class 或 membership 不一致时，不能把同名 Node/class 重新解释为旧 anchor；
- 可选 PodGroup anchor 摘要只作快速索引，丢失或过期后必须从成员 Pod 重建；
- Alpha 不承诺 Pod 消失后仍能恢复外部 reservation；需要该能力时另开 durable evidence/ledger 合同。

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

### 6.4 Policy compile 与 Node-local hard filter

compiler 先把 workload selector 规范化为：

~~~text
DomainClassKey{
  ResourceName: policy.resourceName,
  Scope:        policy.domain.scope,
  Name:         policy.domain.domainClass,
}
~~~

然后从 snapshot 的 `DomainClasses` 取得固定 class 定义、校验 Provider capability。缺 catalog
entry 返回 `XPUTopologyDomainClassUnknown`；没有 Provider capability 返回 `XPUTopologyDomainClassUnsupported`；Provider 已声明但
数据 Pending/stale 使用对应 data reason；Provider 不能消费/确认 selected DeviceKey 时返回 `XPUAssignmentNotEnforceable`。compiler 不能把
unknown class 改成“任意 Domain”，也不能根据任何内部数字 rank 选择更宽 class。

compiler 必须按两个正交维度进入四条显式分支：

| `applyTo + scope` | Planner 行为 | 跨 wave 状态 |
| --- | --- | --- |
| `Pod + Node` | 对每个 Pod 独立选择一个精确 class 的 LocalDomainKey | 不共享 anchor；各 Pod 可不同 |
| `Group + Node` | 为该 TopologyGroup 联合选择一个能容纳目标成员的 LocalDomainKey/NodeUID | 已绑定 Pod 恢复并固定该 LocalDomainKey |
| `Pod + Fabric` | 对每个 Pod 独立选择一个包含其 DeviceKeys 的同 class FabricKey | 不共享 anchor；各 Pod 可不同 |
| `Group + Fabric` | 为该 TopologyGroup 联合选择一个包含所有成员 DeviceKeys 的 FabricKey | 已绑定 Pod 恢复并固定该 FabricKey |

`Group + Node` 还必须把已 Binding/Running 成员作为固定占用，并在同一 LocalDomain 中对当前 AdmissionSet 重放普通 Node 与
Device 容量；不存在共同实例时整组失败，不能退化成每 Pod 各选一个 local Domain。

`Pod + Node` 分支中，一个 Pod 在一个 Node 上请求 `k` 个整卡 Device 时：

1. 从 `DomainsByClass[DomainClassKey]` 找到该 NodeUID、resource、`scope=Node`、class 精确匹配的 local Domains；
2. 使用 `SchedulableByDomain` 的显式 Device 集合；
3. 逐 Device 检查 Healthy、fresh、provider synced、NodeUID 归属和 Provider assignment contract；普通资源可用性仍由现有 Predicate/Node 记账处理；
4. 仅当一个 Domain 至少有 `k` 个 Device 时接受；
5. 两个 Domain 的 `6 + 2` 不能满足同一 Domain 请求 `8`；
6. Provider 不能消费/确认 selected DeviceKey 的 hard policy 返回 `XPUAssignmentNotEnforceable`；
7. 没有完整方案时返回结构化 xPU reason，不伪装成普通 Node aggregate shortage。

### 6.5 Fabric plan

`Pod + Fabric` 对每个 Pod 分别从 `DomainsByClass[DomainClassKey{ResourceName, Fabric, Name}]` 选择一个包含其 DeviceKeys
的 FabricKey；`Group + Fabric` 则选择一个满足 anchor、并覆盖全部固定成员与 AdmissionSet 的共同 FabricKey。两者都只在所选
Fabric 的完整 member local Domains 内分配；前者不要求不同 Pod 使用相同 Fabric，后者必须使用相同 Fabric。
Fabric membership 只定义允许使用的 local Domain/device 并集，不自动施加“每 Pod 单一本地域”。若 workload 还声明了
`Pod + Node + domainClass` policy，Planner 必须同时满足该本地约束；Fabric policy 不能替代它。

候选集合是：

~~~text
normal Predicate Nodes
INTERSECT NodeShard Nodes
INTERSECT selected HyperNode/network scope（如果配置）
INTERSECT explicit Fabric member Nodes
INTERSECT per-Pod local Domain feasibility（仅在另有 Pod+Node policy 时）
~~~

### 6.6 Deterministic Compact planner

Planner 是 side-effect-free 的。它不能调用 Adapter 或 live ledger reserve。建议顺序：

1. Task 按候选 Domain 最少、请求 Device 数最多、现有 Task order、PodUID 排序；
2. Domain/Device 按 3.2 的 deterministic Compact 顺序枚举；
3. plan-owned `NodeInfo` clone 重放普通 `AddTask` 资源记账，防止多个 Task 分别可放但合计超量；
4. greedy 无法完成时可做 bounded backtracking；
5. `maxSearchStates/maxCandidateDomains/maxPlanningAttempts/deadline` 到达时返回
   `XPUTopologyPlanningBudgetExceeded`，不能假报 `NotEnoughResources`；
6. provisional plan 无 reservation、token 或外部副作用；只有 winning Statement 的 final plan 可以进入现有逐 Pod Bind，并写入
   scheduler-owned assignment annotation；外部 reservation 不属于 Alpha。

### 6.7 示例一：Ascend 16 NPU 的 HCCS local-scale-up

管理员 catalog 声明：

~~~yaml
resourceName: huawei.com/Ascend910
domainClasses:
  - scope: Node
    name: local-scale-up
~~~

Provider 在 `node-a` 上规范化出两个共享同一 class、但 identity 不同的 Domain：

~~~text
DeviceDomain{Key: node-a/hccs-0, Class: {huawei.com/Ascend910, Node, local-scale-up}}
  -> npu0, npu1, npu2, npu3, npu4, npu5, npu6, npu7
DeviceDomain{Key: node-a/hccs-1, Class: {huawei.com/Ascend910, Node, local-scale-up}}
  -> npu8, npu9, npu10, npu11, npu12, npu13, npu14, npu15
~~~

workload policy：

~~~yaml
deviceTopology:
  policies:
    - resourceName: huawei.com/Ascend910
      applyTo: Pod
      mode: hard
      domain:
        scope: Node
        domainClass: local-scale-up
~~~

当 Pod 的一个普通 Container 请求 `huawei.com/Ascend910: 8` 时，两个具体 Domain 都是候选；deterministic Compact
选择 canonical key 最小的 `node-a/hccs-0`，assignment 为 `npu0..npu7`。当请求改为 `10` 时，Node aggregate free
虽然是 `16`，但任一 `local-scale-up` Domain 最大只有 `8`，Planner 不能拼接 `hccs-0` 的 8 张与 `hccs-1` 的 2 张，
因此返回 `XPUDeviceDomainFragmented`，不创建 reservation。

### 6.8 示例二：同一 Node scope 内的两个 DomainClass

同一 `node-a` 可以发布重叠 membership：

~~~text
node-a/nvlink-0
  Class = {nvidia.com/gpu, Node, local-scale-up}
  Members = GPU0..GPU3

node-a/pcie-root-0
  Class = {nvidia.com/gpu, Node, pcie-root}
  Members = GPU0..GPU7
~~~

GPU0..GPU3 同时属于两个具体 Domain 是合法的，因为两个对象具有不同 class/identity。以下 policy 即使看到
`pcie-root-0` 有 8 张空闲卡，也只能在 `local-scale-up` index 中选择：

~~~yaml
domain:
  scope: Node
  domainClass: local-scale-up
~~~

请求 4 张可以选择 `nvlink-0`；请求 8 张必须 fragmented fail。只有 workload 显式写成：

~~~yaml
domain:
  scope: Node
  domainClass: pcie-root
~~~

请求 8 张才可以选择 `pcie-root-0`。这证明 `scope=Node` 只确定 identity/membership 边界，不能代替 class selector。

### 6.9 示例三：Pod 本地域与 Group Fabric 同时约束

集群有两个同 class 的具体 Fabric：

~~~text
FabricDomain F1, Class={nvidia.com/gpu, Fabric, scale-up-fabric}
  -> node-a/local-0, node-b/local-0, node-c/local-0, node-d/local-0
FabricDomain F2, Class={nvidia.com/gpu, Fabric, scale-up-fabric}
  -> node-e/local-0, node-f/local-0, node-g/local-0, node-h/local-0

每个 local-0:
  Class={nvidia.com/gpu, Node, local-scale-up}, Members=GPU0..GPU7
~~~

4 个训练 worker、每个请求 4 GPU 的 PodGroup 声明两条 AND hard policy：

~~~yaml
deviceTopology:
  policies:
    - resourceName: nvidia.com/gpu
      applyTo: Pod
      mode: hard
      domain:
        scope: Node
        domainClass: local-scale-up
    - resourceName: nvidia.com/gpu
      applyTo: Group
      mode: hard
      domain:
        scope: Fabric
        domainClass: scale-up-fabric
~~~

一种满足 policy 的合法 placement（不是对 Compact 决胜结果的唯一断言）：

~~~text
selected FabricKey = F1
worker-0 -> node-a/local-0 -> GPU0..GPU3
worker-1 -> node-b/local-0 -> GPU0..GPU3
worker-2 -> node-c/local-0 -> GPU0..GPU3
worker-3 -> node-d/local-0 -> GPU0..GPU3
~~~

非法 placement：

~~~text
worker-0, worker-1 -> F1
worker-2, worker-3 -> F2
~~~

后一结果中每个 Pod 都满足自己的 `Pod + Node + local-scale-up`，但不存在一个具体 Fabric `d` 包含所有 Group 成员，
因此不满足 `Group + Fabric + scale-up-fabric` 的 `∃d∀p`。

### 6.10 示例四：`replicas > minMember` 时跨 wave 固定具体 Fabric

假设前一示例改为 `replicas=8, minMember=4`，并用 queue quota/当轮可用候选把首轮可 admission 数限制为 4；下一 Session
释放该限制。第一次 winning Statement 的 AdmissionSet 因而只有 `worker-0..3`，Planner
在 `scale-up-fabric` class 下选择具体 `FabricKey=F1`，并把每个成功 Bind 的 Pod assignment annotation 保留下来：

~~~yaml
version: 1
resourceName: nvidia.com/gpu
provider: nvidia-nvml-v1
deviceKeys:
  - <node-uid>/GPU-aaaa
  - <node-uid>/GPU-bbbb
~~~

第二个 admission wave 的 `worker-4..7` 仍先按 class 编译 policy，但 scheduler Session 首先扫描已绑定的 `worker-0..3`，
从它们的 `spec.nodeName + xpu-assignment.deviceKeys` 映射出 `F1`；之后只能使用 `F1` 的 member local Domains。
它们可以使用 `node-a..node-d/local-0` 尚余的 `GPU4..GPU7`。即使同 class 的 `F2` 此时更空闲，也不能漂移。这里：

~~~text
domainClass=scale-up-fabric       -> 用户选择的稳定类别
Pod.spec.nodeName + DeviceKeys    -> 已绑定 Pod 提供的恢复事实
PodDerivedGroupAnchor(F1)         -> 当前 Session 从 Pod 重建的具体实例
~~~

因此 `applyTo=Group` 的作用域是稳定 `TopologyGroupRef`，不是首个 AdmissionSet，也不要求一次调度全部 replicas。

## 7. Alpha 请求形状、Pod assignment 与 Provider

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

type TopologyDomainSelection struct {
    ApplyTo        DeviceTopologyApplyTo
    Class          DomainClassKey
    LocalDomainKey *LocalDomainKey // exactly one of LocalDomainKey/FabricKey
    FabricKey      *FabricKey
}

type TopologyTaskPlacement struct {
    TaskID          api.TaskID
    PodUID          types.UID
    NodeName        string
    NodeUID         types.UID
    DomainSelections []TopologyDomainSelection
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

Alpha 中每个目标 resource 的 `Assignments` 恰有一项。Bind 前由 scheduler 将其压缩为受控 Pod annotation：

~~~json
{
  "version": 1,
  "resourceName": "nvidia.com/gpu",
  "provider": "nvidia-nvml-v1",
  "deviceKeys": ["<node-uid>/GPU-aaaaaaaa"]
}
~~~

Provider/Device Plugin 至少必须能消费或确认 `DeviceKeys`，并保持该 annotation 在已绑定 Pod 上可读。重建 anchor 时使用
PodUID、Pod `spec.nodeName`、NodeUID、ResourceName、Provider 和 DeviceKeys；任何 NodeUID/DeviceKey 不一致都使 Group Pending。
annotation 可附带 `localDomainKeys`、`fabricKeys`、`groupRef` 或 `planDigest` 作为快速索引，但这些派生字段丢失/过期时必须从
Pod 与当前 topology snapshot 重建，不能成为第二份权威事实。
`DomainSelections` 按 policy/class stable key 排序，避免多 resource 或 Node+Fabric 双 policy 丢失拓扑约束。Filter/Score/Allocate 阶段
不得再次运行厂商 chooser 并替换 plan 中的 ID。

### 7.3 Provider/Device Plugin identity contract

~~~go
type DomainClassCapability struct {
    Class DomainClassKey
}

type XPUAssignmentProvider interface {
    IdentityContract() DeviceIdentityContract
    DomainClassCapabilities() []DomainClassCapability
    ValidateTopology(ctx context.Context, facts NodeTopologyFacts) error
    SelectDevices(ctx context.Context, req DeviceAssignmentRequest) ([]DeviceKey, error)
    ValidateAssignment(ctx context.Context, req DeviceAssignmentRequest, keys []DeviceKey) error
}
~~~

~~~go
type DeviceIdentityContract struct {
    ProviderID string
    Namespace  string
    ResourceName corev1.ResourceName
}
~~~

Alpha 不定义 scheduler-side exact-allocation owner。Provider identity contract 必须与
`DeviceIdentityContract{ProviderID, Namespace, ResourceName}` 完全匹配；`DomainClassCapabilities()` 只声明 Provider 能解释哪些
固定 catalog class，不创建 class、不改变 `DomainClassKey` identity，也不能用通配符绕过 hard policy。Provider 能发现某 class 但
不能消费或确认选中的 DeviceKeys 时，`soft` 仍可使用有证据的 preference，`hard` 对相关 workload 返回
`XPUAssignmentNotEnforceable` 并保持 Pending。

Plain Device Plugin 数量和 kubelet `GetPreferredAllocation` 不是 scheduler-selected exact reservation 接口。
未经验证时它们只允许 `soft` advisory；不能让 hard workload 进入 Bind。Alpha 不要求 Provider 实现 Reserve/Commit/Release/Recover；
这些是后续 Exact 的 Adapter contract。

## 8. 后续 Exact 事务语义：名称开放，行为固定

> 本节是后续 Exact 的设计保留区，不属于 Pod-derived Topology Alpha。Alpha 复用现有 Statement 与逐 Pod Bind，
> 不实现本节的 ledger、participant、durable evidence 或 complete-batch barrier。

### 8.1 当前缺口

当前 `Statement` 可以回滚 Task/Node speculative operations，但不会自动回滚 allocate action 的所有旁路状态；当前 bind 路径也不是
全组闸门。因此后续 Exact 若需要跨系统事务，必须增加 framework change。

以下接口名都只是候选：

~~~text
BeforeStatementCommit / TransactionParticipant
AddBindGroup / AddBindBatch
GroupCommitHandle / XPUTopologyHandoff
AddGroupTopologyPlanFn / AddTopologyReserveFn
~~~

maintainer 可以选择最终命名和抽象层次，但 8.2 至 8.8 的语义及失败注入测试不能因命名争论被删除。
第 2.5.4 节的 `AddGroupTopologyPlanFn/AddTopologyReserveFn` 是 plugin-facing 建议注册点；本节的 hook/participant/batch
接口是 framework 与 bind path 的 transaction 形状。两者必须连接到同一个 winning Statement 和 group context，不能各自形成
独立 owner。

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
2. 重新校验 PodUID、NodeUID、policy/anchor、请求数量和引用的 Domain/Fabric membership；
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

当前 `executePreBinds()` 的“失败一个、继续绑定其他成功项”不能用于后续 Exact group path；Alpha 明确接受现有逐 Pod 行为，
不把它包装成全组原子提交。

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

后续 Exact 如需承诺：

> 在任何 Pod 开始 Kubernetes Bind 前，AdmissionSet 中所有 exact assignment 已完成 final reservation、Adapter
> preparation、anchor/evidence 持久化和全组 PreBind。

它不承诺 Kubernetes API 的多 Pod Bind 原子性。第一个 Pod 成功、第二个 Pod 失败时：

- 成功 Pod 的 allocation 进入 authoritative observation/reconciliation；
- 失败和未绑定成员执行幂等 compensation；
- 不确定 Device 保持不可用；
- Job controller 是否补偿已绑定 Pod 是更上层启动/容错策略。

## 9. Alpha recovery 边界与后续 Exact ledger

### 9.1 Alpha Pod-derived recovery

Alpha 不维护 `Free/Held/Binding/Allocated/ReconcilePending` 的 scheduler-side allocation ledger，也不定义外部 release state machine。
Session 打开时从已绑定/运行成员 Pod 的 `spec.nodeName` 与 `volcano.sh/xpu-assignment` 读取恢复输入：

1. 只接受 `AllocatedStatus` 且 `NodeName` 非空的成员；未绑定 Pod 不建立跨 Session anchor；
2. assignment annotation 必须能解析出 `resourceName/provider/deviceKeys`，并通过当前 NodeUID 和 topology snapshot 校验；
3. 成员映射到同一 LocalDomain/Fabric 时建立本次 Session 的 `PodDerivedGroupAnchor`；缺失或冲突则 hard Pending；
4. PodGroup 摘要、metrics 和日志只能作为索引/诊断，丢失后从 Pod 重建；
5. 只有一个 active scheduler leader 负责同一 Group；不实现 active-active fencing、外部 owner recovery 或精确 Released。

### 9.2 后续 Exact allocation state machine

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

### 9.3 仅延期 topology-aware victim selection

**后续研究**可以延期：用 Domain/Fabric 碎片、link 或 replacement plan 优化 victim 选择。

**后续 Exact 必做**：任何现有 preemption/reclaim/eviction 导致的设备释放都经过同一个 ledger/Adapter path。

- `Statement.Evict()` 或 Deallocate callback 只能记录 `ReleaseRequested`/`ReconcilePending`，不能立即 `Free`；
- `Releasing` Pod、Node `FutureIdle` 和 nomination 不是 authoritative Device release；
- victim Pod 仍在 Node 或 Adapter 仍报告 allocation 时，其 Device 不能被 final reserve；
- eviction 被取消/回滚时保留或恢复同一个 owner，不能创建第二份 owner；
- 只有 Adapter/Claim/可信 runtime source 明确返回 `Released` 后才能 `Free`；
- release 后 fingerprint/availability 变化时，新 workload 重做完整 plan；
- Pipeline 只保存可重建 hint，不提前 reserve 等待 victim 的 Device。

### 9.4 显式 reconciliation result

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

### 9.5 重启和 leader 切换

在后续 Exact 没有共享持久 reservation record 之前，保证范围才是：单 active leader + process-local ledger + backend durable Adapter lease。
这不是 Pod-derived Alpha 的保证范围。
Follower 可以 warm informer/provider read-only state，但不能 reserve、commit 或 handoff。

新 leader 必须：

1. 等待 Node/Provider 初次同步；
2. 调用 Adapter `Recover()`；
3. 用 plan digest、NodeUID、DeviceKey 和 anchor/evidence 重建 ledger；
4. 把无法匹配的项置为 `ReconcilePending`；
5. 只有该 resource 恢复完成后才允许新的 hard plan。

不得声称仅靠内存 CAS 支持 active-active 多 scheduler exact reservation。

### 9.6 PodGroup Condition/Reason 写入路径

建议复用现有 `PodGroupCondition`，不新增一套 xPU status API。调度期路径是：

~~~text
xPU filter/plan/assignment validation produces structured result
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
`Scheduled` outcome，避免用户看到陈旧 True。Condition message 可以包含 resource、scope、domainClass、required count、最大可用 Domain
count 和 provider/adapter 状态，但不能把 DeviceID/PodUID/token 用作 metrics label。

建议 reason：

~~~text
XPUTopologyPolicyConflict
XPUTopologyPolicyInvalid
XPUTopologyPolicyMutationForbidden
XPUTopologyDomainClassUnknown
XPUTopologyDomainClassUnsupported
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

## 10. Alpha 与后续 Exact 失败矩阵

> 本表中标注“后续 Exact”的行不属于 Alpha 放行条件。Alpha 的失败处理以 Pod annotation、NodeUID/topology 校验、Group Pending
> 和现有逐 Pod Bind 为准，不创建 scheduler-side reservation owner。

| 失败点 | hard 行为 | Alpha/后续 Exact 结果 | Condition/Reason |
| --- | --- | --- | --- |
| gate 关闭但收到新建/新增 xPU policy | admission reject | 不产生 canonical active policy | admission error / `XPUTopologyFeatureDisabled` |
| gate 与 plugin 只启用一个 | scheduler 不 Ready 或拒绝热更新 | 不启动/切换 topology manager | configuration error |
| plugin 参数非法或 Alpha 配置带 resource owner | scheduler 不 Ready 或保留上一份配置 | Alpha 不创建跨系统 owner | configuration error |
| 有已绑定 topology Pod 时移除 plugin 或改变 policy identity | 拒绝配置切换/更新，要求先 drain | 已绑定 Pod annotation 继续作为只读恢复输入 | configuration error / `XPUTopologyPolicyMutationForbidden` |
| 新 Node provider 仍 Pending | 当前候选 fail closed | 不建立新 assignment；旧 UID 不复用 | `XPUTopologyDataNotReady` |
| annotation/typed policy 冲突 | Group Pending | 不产生 plan | `XPUTopologyPolicyConflict` |
| V4 policy 携带旧 `tier/tierName` 或缺 `domainClass` | admission reject；controller 路径 Group Pending | 不产生 canonical plan | `XPUTopologyPolicyInvalid` |
| `(resourceName, scope, domainClass)` 不在 catalog | policy fail closed | 不产生 plan/reservation | `XPUTopologyDomainClassUnknown` |
| class 已知但 Provider 不能消费/确认 DeviceKey | Group Pending | 不产生 Bind/assignment；不创建 exact owner | `XPUTopologyDomainClassUnsupported` / `XPUAssignmentNotEnforceable` |
| 活动期修改 policy | 拒绝更新；旧 fingerprint 继续有效 | 不迁移 owner | `XPUTopologyPolicyMutationForbidden` |
| Provider stale/invalid | 不使用新 hard plan | 已绑定 Pod 不被内存回滚；新成员保持 Pending | `XPUTopologyStale` |
| Node 同名重建 | 新 UID Pending；旧 key 不候选 | 旧 Pod assignment 不映射到新 UID；Alpha 不维护外部 tombstone | `XPUTopologyDataNotReady` |
| Fabric owner/member UID 变化 | Fabric 不可用，等待 owner 重发完整集合 | active key 保留 | `XPUFabricDomainUnavailable` |
| 6+2 请求同 Domain 8 | Node 不可行 | 无 reservation | `XPUDeviceDomainFragmented` |
| 请求来自多 Container/init/shared unit | Group Pending 或 admission reject | 无 reservation | `XPUTopologyUnsupportedPodRequest` |
| Provider 不能消费/确认 selected DeviceKey | hard 不调度 | 无 assignment/Bind，不创建 per-ID owner | `XPUAssignmentNotEnforceable` |
| bounded planner 耗尽 | retryable Pending | 无 reservation | `XPUTopologyPlanningBudgetExceeded` |
| assignment annotation 缺失/非法（Alpha） | Group Pending | 不建立 anchor，不从摘要或 index 猜测 | `XPUAssignmentNotEnforceable` |
| 已绑定成员的 anchor 冲突（Alpha） | Group Pending | 不选择第二个 Domain/Fabric | `XPUDeviceDomainFragmented` / `XPUFabricDomainUnavailable` |
| final NodeUID/DeviceKey/membership 改变（Alpha） | 丢弃并重新规划 | 不进入 Bind；无 CAS/ledger 修改 | `XPUTopologyDataNotReady` |
| 任一 ID reserve 冲突（后续 Exact） | 完整 replan，不局部换卡 | all-or-nothing CAS | `XPUReservationConflict` |
| Adapter Reserve/Prepare 失败（后续 Exact） | 零 Bind | 逆序补偿；不明则 ReconcilePending | adapter reason / `XPUReconcilePending` |
| anchor/evidence 写入失败（后续 Exact） | 零 Bind | 明确失败则补偿；结果不明则 reconcile | `XPUReconcilePending` |
| 任一 PreBind 失败（后续 Exact） | 整 batch 提交零个 BindContext | rollback checkpoint/Statement/Adapter/ledger | `XPUPreBindFailed` |
| 单 Pod Bind 在 batch dispatch 后失败（后续 Exact） | 不声称组原子回滚 | 已绑定项 reconcile；未绑定项补偿 | bind reason / `XPUReconcilePending` |
| eviction 请求已发出但未确认 release（后续 Exact） | Device 不进入新 plan | 保持 owner/ReconcilePending | `XPUReconcilePending` |
| Reconcile 返回空/缺项/Unknown（后续 Exact） | 不复用 ID | 保持 ReconcilePending | `XPUReconcilePending` |
| scheduler restart/recovery failure（后续 Exact） | 对该 resource 暂停 hard | recovered owner 或 quarantine | `XPUTopologyDataNotReady` |

soft policy 的 topology data/adapter 不可用时只失去相应 preference，并记录低基数退化原因；它不能让一个普通 Predicate
失败的 Node 重新可调度，也不创建具体 ID reservation。

## 11. 验证与验收计划

### 11.1 Feature gate、plugin 配置与生命周期

- `XPUTopologyAwareScheduling` 注册为 Alpha、默认 `false`；默认 scheduler 配置不含 `xpu-topology-aware`；
- scheduler gate × plugin 四种组合全部覆盖，只有二者同时启用且参数有效时 Ready；
- admission gate 关闭时拒绝创建/新增/修改 policy，但允许删除 policy；scheduler core guard 阻止存量非空 policy 落入普通路径；
- admission gate 开启但目标 scheduler config 缺少 plugin、参数非法、不可读或 digest 不一致时仍拒绝非空 policy；
- Helm render 测试证明 scheduler 与 admission 都收到同名 gate，scheduler ConfigMap 包含完整 plugin 配置；
- `ValidatePluginOption` 拒绝 unknown key、非正 duration/budget、`enablePredicate=false`、`enableNodeOrder=false` 和 Alpha 非空
  `resource-owners`；后续 Exact 再校验 Adapter registry；热更新失败时保留上一份配置；
- gate/plugin 启用但 authoritative catalog/config source 不可读时 scheduler readiness fail closed；单个 Node provider facts Pending
  只使对应 hard 候选/Job Pending，不阻塞整个 scheduler；
- `New/Name/OnSessionOpen/OnSessionClose` 生命周期测试证明每个 Session 只使用 `ClusterInfo` 配对 view，且 close 不释放 Provider/cache；
- `OnSessionOpen` 注册 JobValid、Predicate、BatchNodeOrder、GroupPlan 和 overlay EventHandler；不注册 Reserve、第二个 HyperNode gradient
  或复制 `JobReadyFn`；
- enabled-no-policy 的回归结果与 plugin-disabled 调度决定一致；只允许出现受控的 snapshot/回调开销；
- plugin removal、Provider/identity 变更在已有绑定 topology Pod 或 semantic policy activity 存在时被拒绝；
- 单 active leader 的配置读取与 Session 重建测试通过；Alpha 不依赖 Reservation/SchedulerEpoch 才能恢复 Pod-derived anchor。

### 11.2 API 和 canonicalization

- `hard/soft` default 和 invalid value reject；`scope/domainClass` 必填且语法校验；
- V4 Go type 不暴露旧字段；served PodGroup/VCJob OpenAPI 的 reject-only tombstone + CEL 与 annotation strict decoder 均拒绝
  `tier/tierName`，即使客户端未启用 `fieldValidation=Strict` 也不能 prune 后接受；
- 同名 class 在不同 resource/scope 下编译成不同 `DomainClassKey`；workload 不能用具体 Domain ID 充当 class；
- catalog 未声明的 class 对 hard/soft 都按 authoring error 拒绝；已知但不可 exact 执行的 hard class fail closed；
- Public schema、Pod annotation 和 VCJob field 规范化后 fingerprint 一致；
- `PolicyFingerprint` 包含规范化后的 `domainClass`；固定 catalog 只提供 class 定义，不参与 workload 版本判断；
- reordered/exact-duplicate policies 得到同一 fingerprint，duplicate Group policy 只生成一个 anchor selection；重叠 selector 的
  不同 class fail closed；
- annotation 无 `allocationStrategy`，Public schema 也拒绝该字段；
- direct PodGroup 与 Pod annotation 相同可接受，不同 fail closed；
- VCJob 与 child annotation 不同 fail closed；
- canonical spec 为空但 `XPUTopologyPolicyConflict=True` 时，`JobValid/JobEnqueueable` 仍阻止调度；
- Deployment/ReplicaSet、StatefulSet、bare Pod 均只生成 canonical PodGroup policy；
- scheduler 测试证明它从不直接读取 workload annotation；
- quiescent 更新成功并废弃旧 plan；活动期 mutation 被拒绝；
- invalid annotation 不回落成默认 hard，也不按无 policy 调度。

### 11.3 Identity、Provider 和 Snapshot

- 两个 Node 都有 `device0/domain0` 时 canonical keys 不冲突；
- `DeviceDomain.Class.Scope=Node`、`FabricDomain.Class.Scope=Fabric`，resource/class 不一致的 Provider payload 被拒绝；
- Provider 只能引用 descriptor catalog：HCCS/NVLink source facts 可映射到 `local-scale-up`，未知 class 不能完成同步；
- `DomainsByClass` 只包含通过 descriptor 校验的对象，且 local/fabric 引用判别正确、稳定排序；
- Alpha catalog 内容固定；catalog 缺失或非法时相关能力保持 Pending/fail closed；
- 同名 Node 换 UID 后旧 facts 退出 candidate index，新 UID 为 Pending；
- 迟到的旧 UID update/clear 不影响新 UID；
- old UID assignment annotation 不映射到新 UID；Alpha 不要求 external allocation tombstone；
- ProviderID、identity namespace 和 source device value 各自参与正确字段，不能互相代替；
- 重复 source generation 只允许 content-identical heartbeat；
- Fabric 只有 owner annotation 可声明，member 重复声明被拒绝；
- owner 必须发布完整集合，member UID/generation 变化后必须重发；
- `ClusterInfo.Nodes` 与 `DeviceTopology.NodeIdentities` 在并发 Node replacement 下不存在新 Node/旧 UID 混合；
- Snapshot 指针 publish 后不可变；Provider update 不改变旧 Session view；
- provider parse/Adapter RPC 在锁外；锁序测试和 race test 不出现反向加锁；
- 不存在 dual-snapshot retry count 或 plugin 二次捕获。

### 11.4 Filter、Compact 和 Group

| 场景 | 期望 |
| --- | --- |
| Ascend 16 NPU 分成两个 8-device `local-scale-up` Domain，请求 8 | 选择一个完整 HCCS Domain |
| 同上请求 10 | aggregate 16 也失败，返回 `XPUDeviceDomainFragmented` |
| 同 Node 同时有 `local-scale-up` 4 卡和 `pcie-root` 8 卡，请求 class=`local-scale-up` 8 | 失败，不能因 scope 相同扩大到 `pcie-root` |
| 单 Node 两 Domain free=6+2，请求 8，hard | 失败，不能跨 Domain 拼接 |
| 单 Domain free=8，请求 8 | 选择该 Domain 的 8 个稳定 DeviceKeys |
| 两个等价 Domain | 多次运行选择完全一致 |
| `Pod+Node` 的两个 Pod 分别位于两个同 class local Domain | 合法；验证 `∀p∃d_p` |
| `Group+Node` 的两个 Pod 分散到两个 local Domain/Node | 非法；共同 Domain 可容纳时锚定唯一 LocalDomainKey |
| `Pod+Fabric` 的两个 Pod 分别选择 F1/F2 | 合法；每个 Pod 的 DeviceKeys 分别属于自己的 Fabric |
| Fabric 未显式声明，只共享 HyperNode/IB | 不能满足 hard Fabric |
| `Pod+Node+local-scale-up` 与 `Group+Fabric+scale-up-fabric` 同时声明 | 每 Pod 本地域有效，且全 Group 共享一个具体 FabricKey |
| `Group+Fabric` 的成员分散到 F1/F2 | 非法；验证 `∃d∀p` |
| `minAvailable=4, replicas=10`，用 quota 强制首 wave 仅 4 个 | 首 wave 建立 anchor；解除 quota 后其余 Task 继续满足同一 anchor |
| winning Statement 含已 Ready Group 的新增 Task | 全部新增 Allocate operation 都进入 AdmissionSet |
| normal Predicate 已排除 Node | xPU score/plan 不得重新引入 |
| planner budget 耗尽 | 返回专用 retryable reason，不假报资源不足 |

### 11.5 后续 Exact transaction 和 reconciliation

- 多 Container/init/fractional 请求被拒绝；
- Adapter identity 匹配但未声明目标 `DomainClassKey` capability 时 hard fail closed；
- Alpha assignment annotation 精确包含 `version/resourceName/provider/deviceKeys`；后续 Exact 可额外绑定 PodUID/ContainerName；
- final CAS 任一冲突时全部 ID 保持原状态；
- reserve 失败后 checkpoint 恢复 worksheet、fit error、nomination 和 recorder decision；
- Adapter Prepare 第 N 项失败时前 N-1 项逆序幂等补偿；
- Alpha annotation 缺失/非法时相关 Group 不建立 anchor；
- batch 第 N 个 PreBind 失败时提交 BindContext 零个、Kubernetes Bind 零次（后续 Exact）；
- prepared batch 不重复进入 per-context `executePreBinds()`；
- batch dispatch 后单 Pod Bind 失败不伪造对成功 Pod 的原子回滚；
- eviction 到 authoritative Released 之前 Device 一直不可 reserve；
- `Reconcile()` 返回 `Allocated/Released/Unknown` 三态；空结果和缺项均按 Unknown；
- restart/leader promotion 在 Recover 完成前拒绝 hard；
- 同一 token 的 Reserve/Prepare/Commit/Compensate/Release 重试满足幂等性。

### 11.6 回归、E2E 与性能

- 未配置 policy 时 network-topology-aware、gang、Statement 和现有 device paths 行为不变；
- HyperNode API 不增加 Device/Reservation 字段；
- Mock/KWOK 覆盖 fragmented/fitting Domain、explicit Fabric、Node replacement、health update 和失败回滚；
- 至少一个真实 Exact Adapter 覆盖选中 ID、进程重启恢复、release 和故障注入，才可称为后续 Exact 能力；Alpha 只声明已验证的
  Provider assignment identity 与 Pod-derived anchor；
- benchmark 对比 plugin disabled、enabled-no-policy、soft 和 hard；
- metrics 覆盖 provider age/error、snapshot publish latency、plan duration/budget、assignment annotation/recovery result 和结构化 reason；
- Event/metrics label 禁止使用 raw DeviceID、PodUID 或 ReservationID。

## 12. PR 拆分与交付阶段

### 12.1 PR 0：API 与 framework contract review

- 冻结 `XPUTopologyAwareScheduling` gate、`xpu-topology-aware` plugin 名、双重启用/禁用和 safe-drain 合同；
- 冻结 plugin arguments、严格 validator、process manager 与 per-Session plugin 的生命周期边界；
- 冻结 `DeviceTopologySpec` 的 hard/soft、applyTo、scope/domainClass，并明确拒绝 `tier/tierName`；
- 确定 `ResourceTopologyDescriptor` 的固定 authoritative 载体和 webhook/controller/Provider/scheduler 的统一读取路径；
- 明确 Public API 无 allocationStrategy；
- 评审 Pod annotation key、canonicalization 和 mutation webhook；
- 冻结 `volcano.sh/xpu-assignment` annotation 的最小字段、canonical DeviceKey 和 Pod Bind 传递路径；
- 接受 Session-local anchor、单 leader、缺失/冲突 Pending 和现有逐 Pod Bind 边界；完整 batch/compensation/reconciliation 留作后续 Exact。

### 12.2 PR 1：canonical model、Provider 和 identity

- 注册默认关闭的 Alpha feature gate、plugin builder/config validator 和 Helm scheduler/admission gate 配置；
- 实现 gate/plugin 组合校验、存量 typed-policy guard、热更新拒绝与 activation digest/readiness；
- 建立 process-scoped topology manager；plugin `New/OnSessionOpen/OnSessionClose` 不拥有 Provider 生命周期；
- 实现 `DomainClassKey`、descriptor catalog、Device/LocalDomain/Fabric IDs 和 keys；
- `DeviceDomain/FabricDomain.Class`、`DomainsByClass` 与固定 catalog 校验后的 topology publish；
- Node UID/RV ordering、Pending/Synced、旧 UID assignment 失效；
- Annotation/Mock Provider、schema/security 和 freshness；
- single-owner complete Fabric declaration；
- immutable topology objects、indexes 和 unit tests。

### 12.3 PR 2：ClusterInfo snapshot 与 Advisory MVP

- `ClusterInfo.DeviceTopology` 和同一次 `SchedulerCache.Snapshot()` 捕获；
- 按固定锁序 publish immutable pointer；
- Pod/VCJob/PodGroup canonicalization 和 Condition reason；
- 实现 plugin 的 JobValid、Predicate、BatchNodeOrder 与 Session overlay EventHandler；
- `scope/domainClass` compiler、Node/Fabric class filter、soft score、internal deterministic Compact；
- plugin-disabled/no-policy 回归和并发 snapshot 测试。

本阶段可称 Advisory MVP。没有可消费/确认 DeviceKey 的 Provider 时，hard 保持 fail closed；它不创建外部 owner。

### 12.4 PR 3：Pod-derived Topology Alpha

- 单普通 Container 整卡 validation 和完整 assignment identity；
- side-effect-free group planner、AdmissionSet 和 Session-local anchor；
- scheduler-owned `volcano.sh/xpu-assignment` 随成功 Pod Bind 保留；
- 后续 Session 从 bound Pod 的 NodeName + DeviceKeys 恢复 Group anchor；
- action bypass、Node replacement、annotation 缺失/冲突和单 leader 测试；
- 不新增 ledger、durable evidence、complete-batch PreBind 或双 scheduler owner。

### 12.5 后续 Exact transaction

- final all-or-nothing ledger reserve；
- Adapter durable reservation、prepare/commit/compensate/recover；
- complete-batch PreBind、atomic batch acceptance 和显式 Allocated/Released/Unknown reconciliation；
- eviction/release ledger integration、active-active fencing、failure injection 和 leader handoff tests。

### 12.6 PR 4：Fabric/Group E2E 与发布

- explicit Fabric + HyperNode intersection；
- `Pod+Node` 与 `Group+Fabric` 双 class policy E2E；
- 跨 wave anchor E2E；
- KWOK scale、provider churn、Node replacement、Fabric owner replacement；
- metrics、runbook、Helm/ConfigMap 用户文档和 safe-drain 演练；
- Provider assignment identity 和 Pod-derived Alpha 先发布；真实 Adapter 验收后再发布后续 Exact 能力边界。

## 13. 兼容性、明确不做与开放问题

### 13.1 兼容原则

1. feature gate 默认关闭、默认 scheduler 配置不含 plugin；未配置 `DeviceTopology` 时零调度行为变化；
2. HyperNode 仍只拥有 Node/网络层次；
3. Job/SubJob readiness 不复制；
4. normal resource fit 不被 xPU topology view 二次扣减；
5. existing deviceshare/DRA 未成为后续 compatible Adapter 前不共享 exact owner；
6. soft 不制造 hard filter，hard 不静默降级；
7. Mock/Annotation 不升级成真实硬件 exact-ID 证据；
8. Kubernetes Binding 不描述为整组原子提交；
9. feature/plugin/Adapter identity 配置不一致时 fail closed；
10. 禁用 Alpha feature 前必须处理受保护的已绑定 topology Pod 和 semantic policy activity；后续 Exact 另有 reservation/reconciliation drain 合同；
11. V4 与 V3 schema 不做透明兼容；旧 `tier/tierName` 必须显式迁移为 catalog class，不能 silent prune/fallback；
12. gate/plugin 失配、存量 policy 未 drain 或 plugin config 无效时必须拒绝启动/热更新或阻止该 Job，不能回落到普通调度。

### 13.2 已决定，不再作为开放问题

- Node-scope identity 必须包含 NodeUID；
- DeviceTopologySnapshot 放入 ClusterInfo 同次捕获；
- Public mode 使用 hard/soft；
- Public selector 使用 `scope + domainClass`；`DomainClassKey` 不是具体 Domain ID；
- 数值层级不进入 workload API，hard class 不隐式 fallback；
- Alpha Public API 不含 allocationStrategy；
- scheduler 只读取 PodGroup canonical spec；
- activity 后 policy semantic mutation 被拒绝；
- Alpha 请求形状为单普通 Container 整卡；
- Annotation Fabric 为 single owner complete declaration；
- topology-aware victim choice、durable release 和完整事务均不属于首个 Alpha；
- 后续 Exact 如实现跨系统事务，必须另有 checkpoint、全组 PreBind、零提前 Bind、幂等 compensation 和显式三态 reconciliation；
- feature gate 名为 `XPUTopologyAwareScheduling`、Alpha 默认关闭；scheduler plugin 名为 `xpu-topology-aware`，必须双重显式启用；
- process-scoped topology manager 与 per-Session plugin 生命周期分离；`OnSessionClose` 不停止 Provider/cache；
- Alpha 的首个 Provider 目标为 NVIDIA，resource 为 `nvidia.com/gpu`；XPU-01 使用 nvml-mock 验证 selected DeviceKey、annotation
  和恢复；真实 reservation/release 留给后续 Exact。

### 13.3 仍需社区评审的 P0/P1 问题

| 优先级 | 问题 | 不确定的只是 | 已固定的底线 |
| --- | --- | --- | --- |
| P0 | Alpha 的 Pod assignment annotation 传递形状 | BindContext extension、Pod metadata 写入位置和 key 命名 | 已绑定 Pod 必须保留 NodeName + canonical DeviceKeys；摘要不能成为唯一事实源 |
| P0 | 后续 Exact transaction framework 采用最小 hook 还是 generic participant | Go API 名称、注册和兼容形状 | 第 8 章语义与测试不可缩减 |
| P0 | 后续 Exact 的 reservation/evidence 载体 | PodGroup status、Claim、Adapter record 或独立 store | 必须 durable、CAS、可恢复、可 reconcile；不阻塞 Alpha |
| P0 | NVIDIA Exact Adapter 的具体执行形状 | NVIDIA Device Plugin 扩展、NVIDIA-specific companion 或其他 NVIDIA runtime integration | 后续 Exact 必须接受 scheduler-selected GPU UUID 并实现 Recover/Released evidence；nvml-mock 通过不替代真实硬件 runtime 验收 |
| P0 | `ResourceTopologyDescriptor` 的 authoritative 载体与一致分发 | Alpha 固定 catalog 的具体交付文件/ConfigMap 形状 | webhook/controller/Provider/scheduler 必须消费同一份固定 class 定义；运行期间不可修改 |
| P1 | plugin config validator 的 framework 形状 | generic validator registry、让 builder 返回 error 或 scheduler 专用校验 | 必须在首个 Session 前严格失败；热更新失败保留上一配置，不能只在 `OnSessionOpen` warning |
| P1 | early domain feasibility 如何与 HyperNode candidate path 组合 | 新 hook 的位置和数据结构 | 不注册第二个 first-plugin-wins gradient |
| P1 | `domainClass` 名称语法与 catalog 演进规则 | DNS label 的精确限制、废弃窗口和版本策略 | key 至少包含 resource+scope+name；V4 拒绝旧字段和隐式 fallback |
| P1 | 是否需要显式多个 acceptable classes | 独立 API 形状和优先级 | 首个 Alpha 只接受单一精确 class；不得用 internal rank 自动扩大 |
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
| Feature gate | `pkg/features/volcano_features.go` | 注册 `XPUTopologyAwareScheduling` Alpha，默认 `false` |
| 进程参数 | `cmd/scheduler/main.go`、`cmd/webhook-manager/main.go` | 复用现有 `--feature-gates` plumbing，scheduler/admission 同名启用 |
| Helm 配置 | `installer/helm/chart/volcano/values.yaml`、`templates/scheduler.yaml`、`templates/admission.yaml` | 传递两个 component gate；完整覆盖 scheduler ConfigMap 时保留其他 plugin |
| Scheduler config validation | `pkg/scheduler/util.go`、`pkg/scheduler/scheduler.go`、`pkg/scheduler/framework/plugins.go`（建议扩展） | gate/plugin 矩阵、严格 plugin option 校验、热更新 drain 与上一配置保留 |
| Plugin factory | `pkg/scheduler/plugins/factory.go` | 注册 `xpu-topology-aware` builder 与 proposed config validator |
| 当前 ClusterInfo | `pkg/scheduler/api/cluster_info.go` | 增加 immutable `DeviceTopology` 字段 |
| 当前 snapshot | `pkg/scheduler/cache/cache.go:SchedulerCache.Snapshot` | 同主锁观察点捕获 cluster + topology pointer |
| Session 初始化 | `pkg/scheduler/framework/session.go:openSession` | 从 ClusterInfo 取得同一 topology view |
| Topology manager | `pkg/scheduler/topology/manager/`（建议） | process-scoped Provider/cache/snapshot publisher 生命周期与 activation digest；Alpha 不持有 allocation ledger |
| Provider/normalizer | `pkg/scheduler/topology/provider/`（建议） | Annotation/Mock parse、validate、NodeUID/RV/generation |
| Descriptor catalog | Alpha 固定 ConfigMap/静态配置；scheduler canonical type（建议） | 管理员 class 合同、只读加载和一致分发 |
| live topology | `pkg/scheduler/cache/topology_cache.go`（建议） | facts、descriptor view、readiness、class indexes、immutable publish；Pod assignment 由 Pod/cache 读取 |
| Pod canonicalization | `pkg/controllers/podgroup/pg_controller_handler.go`、Job controller、webhook | authoring source 转换、冲突和 mutation validation |
| Public types | `staging/src/volcano.sh/apis/pkg/apis/scheduling/`、batch API | `DeviceTopologySpec` typed schema 与生成代码 |
| 当前 Statement | `pkg/scheduler/framework/statement.go` | 保留普通 Commit；Alpha 只接入 Session-local group plan，不新增 exact participant |
| 当前 bind path | `pkg/scheduler/cache/cache.go:AddBindTask/executePreBinds/BindTask` | Alpha 复用逐 Pod Bind，并确保 Task PodAnnotations 随 Bind 保留；complete-batch gate 属于后续 Exact |
| Condition | `pkg/scheduler/framework/session.go:UpdatePodGroupCondition`、`job_updater.go` | 聚合具体 xPU reason 并回写 PodGroup status |
| Plugin | `pkg/scheduler/plugins/xpu-topology-aware/`（建议） | `New/Name/OnSessionOpen/OnSessionClose`；JobValid、Predicate、BatchNodeOrder、group plan、assignment annotation 和 Session overlay |
| Provider | `pkg/scheduler/topology/provider/`（建议） | identity contract、selected DeviceKey 消费/确认、topology facts；后续 Adapter lifecycle 另行实现 |
| 后续 Adapter | `pkg/scheduler/topology/adapter/`（建议） | exact reserve/handoff/recover/reconcile，不属于 Alpha |

## 附录 B. 关键不变量

1. NodeName 相同不代表 Node identity 相同；所有 Node-owned key 包含 NodeUID。
2. Source ID、Provider identity 和 canonical scheduler key 是不同层次。
3. 一次 Session 只使用 `ClusterInfo` 携带的一份 immutable topology view。
4. publish 与 Node identity 在 `SchedulerCache.Mutex` 下协调；重活和 RPC 在锁外。
5. scheduler 只读取 `PodGroup.spec.deviceTopology`。
6. Public hard/soft 不包含 allocationStrategy；internal Compact 必须确定性。
7. Public selector 是 `scope + domainClass`；V4 schema 拒绝 `tier/tierName`。
8. `scope` 只定义 identity/ownership/membership 边界；`DomainClassKey` 是非唯一类别，不是 Domain ID。
9. DeviceDomain 与 FabricDomain 都携带已由 descriptor 校验的 Class，并进入稳定 class index。
10. Alpha catalog 的 class 定义固定不变；运行期间不更新或重解释活动 class。
11. 已绑定 topology Pod 后 policy 不原地切换；无绑定成员时才允许 semantic mutation。
12. Annotation Fabric 只有一个 owner，且 owner 发布完整成员集合。
13. AdmissionSet 覆盖 winning Statement 中该 Group 的全部新 Allocate operations。
14. Alpha Pod assignment annotation 至少包含 `version/resourceName/provider/deviceKeys`；DeviceKeys 必须包含 NodeUID-safe identity。
15. 已绑定 Pod 的 `spec.nodeName + xpu-assignment` 是跨 Session anchor 的恢复输入；缺失/冲突时 Group hard Pending。
16. PodGroup anchor 摘要、metrics 和日志可丢失并重建，不是第二份事实源。
17. Alpha 不把 Pod eviction、FutureIdle、列表缺项或 observation 缺席解释成 external Released。
18. 后续 Exact 若引入 reservation，必须另有 durable evidence、compensation 和 release/reconcile 不变量。
19. Kubernetes per-Pod Bind 非原子，设计不声称可以原子 unbind。
20. 非空 xPU policy 只有在 `XPUTopologyAwareScheduling` gate 与 `xpu-topology-aware` plugin 同时有效时才可调度；任何失配都 fail closed。
21. plugin `New/OnSessionOpen/OnSessionClose` 是 per-Session 生命周期；Provider/cache 属于 process manager；Alpha 不持有 global ledger/Adapter owner。
22. xPU plugin 不注册竞争性的 HyperNode gradient，也不复制 gang `JobReadyFn`；group plan 只能缩小已有候选集合。

## 附录 C. V4 状态边界

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
  XPUTopologyAwareScheduling Alpha feature gate, default false
  xpu-topology-aware optional scheduler plugin and strict activation validation
  process-scoped topology manager plus per-Session plugin callbacks
  NodeUID-safe canonical IDs and old-UID invalidation
  scope + DomainClass Public policy and administrator descriptor catalog
  DeviceDomain/FabricDomain Class + DomainsByClass
  ClusterInfo.DeviceTopology immutable paired view
  hard/soft Public API and internal deterministic Compact
  one PodGroup canonicalization path
  quiescent-only policy mutation
  single-container whole-device Pod-derived Topology Alpha
  scheduler-owned xpu-assignment Pod annotation
  Session-local PodDerivedGroupAnchor rebuilt from bound Pods
  single active scheduler leader
  action bypass, NodeUID replacement and annotation conflict guards

后续研究
  topology-aware victim selection
  DRA/MIG/vGPU/multi-container
  cluster-scoped Fabric authority
  durable reservation/evidence and complete-batch Bind gate
  Allocated/Released/Unknown reconciliation and eviction/release ledger correctness
  active-active durable reservation
  Kubernetes group atomic binding/start barrier
~~~

本文不把 V4 设计、Mock 验证、社区开放 PR 或文档中的 proposed API 写成 Volcano 当前已实现能力。
