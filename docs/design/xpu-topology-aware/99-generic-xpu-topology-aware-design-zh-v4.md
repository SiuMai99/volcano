# Volcano 通用 xPU 拓扑感知调度设计（收敛版 V4）

> 状态：结构化设计提案，不代表 Volcano 已实现或社区已接受。
>
> 本文以 [V3](./99-generic-xpu-topology-aware-design-zh-v3.md) 为基线，保留其 Node 身份、Session Snapshot、
> Annotation canonicalization 与现有 Statement/Pod Bind 路径，并把 workload topology taxonomy 收敛为
> `scope + domainClass`：删除 Public `tier/tierName`，同时补齐 Node-local Domain 与 Fabric Domain 的统一类别合同。
> Alpha 只复用现有 Statement 与逐 Pod Bind，不扩展跨系统 allocation state 或多 Pod Bind 协调。
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
V4 Alpha 使用已绑定 Pod 的 NodeName 与 assignment annotation 恢复 anchor，并明确不扩展外部 allocation lifecycle 或多 Pod 原子 Bind。

### 1.2 一条主路径，而不是平行调度器

~~~text
direct PodGroup.spec.deviceTopology（Alpha canonical policy source）
  -> schema/canonical validation
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
Pod-derived group anchor 是附着于现有 Job/SubJob/Statement 的 Session-local 内部语义，不是新的 CRD；恢复只扫描已绑定成员 Pod。

### 1.3 两级能力，禁止混写保证

| 能力级别 | 可以承诺 | 不能承诺 |
| --- | --- | --- |
| Advisory MVP | Annotation/Mock topology ingestion、immutable snapshot、Node-local Domain filter 数据、`soft` score、deterministic Compact、结构化 reason | 运行时一定使用 scheduler 选中的 Device ID；`hard` 自动降级；多 Pod 原子绑定 |
| Pod-derived Topology Alpha | compatible Provider/Device Plugin 能消费或确认选中 ID；`hard` fail closed；已绑定 Pod 保存 NodeName 与 assignment；后续 wave/Session 从 Pod 恢复 anchor | 外部 allocation lifecycle；跨系统 crash recovery/release；Kubernetes API 级多 Pod 原子绑定；active-active fencing |

没有能够消费或确认 scheduler-selected DeviceKey 的 Provider/Device Plugin 时：

- `soft` 可以作为 advisory score，且不创建伪造的 per-device owner；
- `hard` 对相关 workload 保持 Pending，并返回 `XPUAssignmentNotEnforceable`；
- 管理员配置不得把 `hard` 静默改为 `soft`。

Mock Provider 可以验证规划、DeviceKey 和 annotation 恢复，但不能作为真实硬件 exact-ID 执行证据；真实硬件执行验收不属于本需求。

Advisory MVP 与 Pod-derived Topology Alpha 都只有在 feature gate 和 plugin 同时启用后才存在。

### 1.4 文档中的四种状态

后文使用下列标签，避免把提议写成当前实现：

- **当前实现**：仓库中已经存在且本设计核对过的行为；
- **可复用能力**：已有组件可以继续承担的所有权；
- **本文提议**：V4 要新增或修改的合同；
- **后续研究**：不进入首个 Pod-derived Topology Alpha 验收路径的能力。

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
9. 当前 `framework.Plugin` 只要求 `Name/OnSessionOpen/OnSessionClose`，现有 Session 也没有组级 topology plan 注册点或多 Pod Bind 协调接口。

因此，现有路径提供的是逐 Pod Bind 基线；本文不把它扩展为多 Pod 提交屏障。

### 2.2 可复用能力

| 现有组件 | 继续拥有 | xPU 不应接管 |
| --- | --- | --- |
| Queue/DRF/capacity/gang | 队列、资源公平性、Job/SubJob readiness | Device identity 和 allocation selection |
| HyperNode/network-topology-aware | Node/网络层次、network gradient 和 score | Device、Domain、Health、外部 allocation owner |
| Predicate/NodeInfo | Kubernetes Node 级资源、taint、volume、port 等可行性 | 单个 Domain 内的 Device membership |
| JobInfo/SubJobInfo | workload/group 上下文和 Task membership | Provider payload 与全局 Device owner |
| Statement | 本轮 Task/Node speculative mutation 的 owner | Provider 解析与硬件事实 |
| SchedulerCache | 集群 live state、一次 Session snapshot、bind handoff | 厂商 runtime 分配协议 |
| Provider/Device Plugin | source facts 的摄取/规范化、能力声明，以及对 scheduler 已选 DeviceKeys 的消费或确认 | 选择 DeviceKeys、Domain/Fabric 规划和 scheduler 调度状态 |

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
~~~

关键所有权如下：

- Provider 只拥有 source ingestion、normalization、capability 和 selected-key validation；最终 DeviceKeys 由 scheduler planner 选择；
- 管理员 `ResourceTopologyDescriptor` 拥有 workload 可见的 DomainClass catalog；Provider 只能引用并规范化到该 catalog；
- topology live cache 拥有 canonical facts、descriptor view、readiness 和 indexes；本 Alpha 不拥有外部 allocation state；
- `ClusterInfo.DeviceTopology` 是 Session 的不可变只读视图；
- xPU plugin 做 policy compile、filter、score 和 side-effect-free plan；Session-local anchor/plan 只服务本轮调度；
- Statement 仍拥有普通 Task/Node mutation，并把最终 DeviceKeys 转换为每个 Pod 的 assignment annotation；
- 已绑定 Pod 是跨 Session anchor 的唯一可恢复输入；不新增 PodGroup 摘要状态；
- Provider/Device Plugin 负责消费或确认 scheduler-owned assignment；外部 allocation lifecycle 不属于本需求。

### 2.4 后续研究

下列能力不进入首个 Pod-derived Topology Alpha：

- DRA `claimName` Public API 和 ResourceSlice Provider；
- MIG、vGPU、共享/分数设备、多 Container、Init Container 的 allocation lifecycle；
- topology-aware victim selection 和 device-level pipeline 持久化；
- 自动推导 Fabric、NCCL ring、厂商 link-bandwidth 规划；

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
- plugin 配置控制一次 Session 是否注册 xPU filter、score 和 group plan；
- gate 不能替代 plugin，plugin 也不能自行绕过 gate；未声明 `deviceTopology` 的 workload 不产生 xPU filter、score、plan 或 assignment。

启动/配置重载矩阵如下：

| Scheduler gate | `xpu-topology-aware` plugin | 结果 |
| --- | --- | --- |
| 关闭 | 未配置 | 功能关闭；不启动 Provider/topology manager，不改变普通 workload 调度行为 |
| 关闭 | 已配置 | 配置非法；首次启动 fail closed；热更新行为属于后续发布流程 |
| 开启 | 未配置 | 配置非法；scheduler 不进入 Ready，不能让 xPU policy 落入普通调度路径 |
| 开启 | 已配置 | 进入 xPU activation path；`New` 按现有 plugin 规则解析参数，无法使用时对相关 xPU policy fail closed |

admission gate 关闭时，webhook 拒绝新建非空 policy，也拒绝给已有对象新增或修改 policy；只允许删除 policy。admission gate
开启也不是单独的 authoring 开关：webhook 只执行 schema/canonical policy 校验，不读取 scheduler 运行时配置、catalog readiness 或
Provider 实时健康。scheduler plugin 缺失、catalog 不可读或 Provider contract 不完整时，scheduler core 对非空 policy fail closed。
scheduler core 还必须有一个不依赖 xPU plugin callback 的 typed-policy guard：gate/plugin 未形成有效组合时，任何已持久化的非空
`PodGroup.spec.deviceTopology` 都不可按普通 Job 调度。这是升级、回滚和误配置的最后 fail-closed 边界，不是第二个调度实现。
controller/schema path 必须始终保留已持久化 intent，不能因为本进程 gate 关闭就把 policy 清空后生成一个
“无 topology 要求”的 PodGroup。

~~~mermaid
flowchart LR
    FG[Feature gate] --> V[Activation guard]
    PC[Scheduler tiers plugin entry] --> V
    V -->|invalid| NR[Startup not Ready]
    V -->|valid| TM[Process-scoped topology manager]
    TM --> SV[Immutable Session topology view]
    SV --> PL[xpu-topology-aware callbacks]
    WG[Admission gate] --> WH[Webhook policy validation]
    WH --> PG[Canonical PodGroup policy]
    PG --> PL
~~~

feature gate 是进程启动参数，不能通过 scheduler ConfigMap 热更新。scheduler plugin 配置虽然可随配置文件热更新，
但 Alpha 不冻结跨进程 reload、统一 drain 或 activation digest。移除 plugin、切换 Provider 或改变 identity namespace 时，发布流程
必须先保证非空 xPU policy 不会落入普通调度；具体升级/关闭演练属于后续发布验收。Alpha 不检查或恢复外部 allocation 状态。

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
~~~

Alpha 只使用 Provider 的 observation/assignment-confirmation：`soft` 可打分，hard 只有在 Provider 能消费或确认
scheduler-selected DeviceKey 且能保留 Pod assignment 时才可执行。Alpha 不配置跨系统 allocation owner；Provider identity namespace
由 capability 报告并校验，不能由管理员用字符串强行声明为“相同”。

插件参数合同为：

| 参数 | Alpha 默认/要求 | 消费者与语义 |
| --- | --- | --- |
| `xpu-topology.provider` | 必填；首期 `annotation`，`mock` 仅测试 | process-scoped topology cache；选择事实输入和 assignment confirmer，不选择 allocation owner |
| freshness/budget | 不冻结为 Public plugin 参数 | Provider freshness 和 planner 的有限预算由实现阶段根据对象大小与调度循环确定；超出预算只能保持 Pending |

这些是最小 scheduler plugin/runtime 入口，不承载 workload policy，也不承载具体 Device/Domain/Fabric ID。`domainClass` 只引用
Alpha catalog；catalog 的具体交付和非 scheduler 组件读取方式不作为跨进程协议冻结。

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

func (p *xpuTopologyAwarePlugin) Name() string
func (p *xpuTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session)
func (p *xpuTopologyAwarePlugin) OnSessionClose(ssn *framework.Session)

// pkg/scheduler/plugins/factory.go
framework.RegisterPluginBuilder(xputopologyaware.PluginName, xputopologyaware.New)
~~~

`Name/OnSessionOpen/OnSessionClose` 是当前 `framework.Plugin` 必须实现的方法，`New(Arguments) Plugin` 是当前 builder 形状。
`xpu-topology-aware` 的参数处理沿用现有 plugin：`New` 先建立默认配置，再用 `Arguments.GetString/GetBool/GetInt/GetFloat64`
解析参数；类型错误、格式错误或超出范围时记录 warning 并保留默认值，无法安全运行的 Provider 配置则标记为不可用，由 xPU policy
路径 fail closed。`enablePredicate/enableNodeOrder` 仍由现有 Session 根据 `PluginOption` 控制回调是否执行，不新增 framework validator。
feature gate 与 plugin 是否形成有效 activation 组合属于 scheduler/policy guard，不由 `New` 单独判断。

长生命周期 Provider、topology cache 和 immutable snapshot publisher 由 process-scoped topology manager 拥有，不能由每个 Session 的
`New` 重复创建。Alpha 不拥有外部 allocation 状态或 recovery worker。scheduler 配置加载器与 plugin `New` 使用同一份
plugin arguments 构造 manager config；
`OnSessionOpen` 只能取得 `ClusterInfo` 已配对的一份 immutable topology view，不能 type-assert cache 后做第二次 snapshot。

#### 2.5.4 Plugin 必须注册/实现的调度函数

Alpha 只复用现有 JobValid、Predicate、BatchNodeOrder 与 EventHandler。完整 Group plan 在 `allocate` 的 xPU 私有集成点执行，
暂不冻结通用 framework 的组规划注册 API；未来出现第二个消费者时，再以真实调用链为依据抽象公共边界：

~~~go
func (p *xpuTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session) {
    p.view = ssn.DeviceTopologyView() // proposed; paired with ClusterInfo snapshot

    ssn.AddJobValidFn(p.Name(), p.jobValid)
    ssn.AddPredicateFn(p.Name(), p.predicate)
    ssn.AddBatchNodeOrderFn(p.Name(), p.batchNodeOrder)

    ssn.AddEventHandler(&framework.EventHandler{
        AllocateFunc:   p.onAllocate,
        DeallocateFunc: p.onDeallocate,
    })
}
~~~

| 函数/回调 | Alpha 落点 | 必须承担的责任 | 明确不能做 |
| --- | --- | --- | --- |
| `New` | 是，当前 plugin builder | 建立默认配置并解析 provider 等静态参数；类型/格式/范围错误告警并回退或标记不可用，不启动 Provider、不访问 live allocation state | 把 gate/plugin 组合校验或动态 topology readiness 藏在参数解析中 |
| `jobValid` | 是，`AddJobValidFn` | 编译 canonical PodGroup policy，校验 catalog/class、请求形状和 hard enforceability，返回结构化 reason | 复制 gang readiness 或把 hard 改成 soft |
| `predicate` | 是，`AddPredicateFn` | 在普通 Predicate 候选上复核 NodeUID、freshness、health、Domain/Fabric membership 与 hard topology feasibility | 只看 Node aggregate xPU 数量，或重新引入已被普通 Predicate 排除的 Node |
| `batchNodeOrder` | 是，`AddBatchNodeOrderFn` | 只对 `soft` policy 的 Domain/Fabric preference 和 internal Compact 产生确定性附加分；共享同一 immutable view | 用 score 实现 hard filter，或覆盖其他 plugin 分数 |
| `groupPlan` | `allocate` 内的 xPU 私有 planner 调用 | 对 winning Statement 可见的完整成员做 side-effect-free Node/Domain/Device plan，并遵守已恢复 anchor | 新增通用 framework callback、修改 live state 或产生外部副作用 |
| `onAllocate/onDeallocate` | 是，`AddEventHandler` | 维护 Session-local overlay，使同一 Session 后续 plan 看见 tentative Allocate/Deallocate | 把普通 Allocate event 当成已绑定 Pod 的 assignment evidence |
| `OnSessionClose` | 是，`framework.Plugin` | 丢弃 Session-local compiled policy、score cache、anchor 和 overlay | 停止 process Provider，或修改外部 allocation owner |

`allocate` 私有 planner 的输入只包括：本轮完整的待调度成员、普通候选交集、已绑定 Pod 推导的 anchor、paired immutable snapshot
和 winning Statement 的 final Node placement。规划结果必须覆盖本轮成员，否则不进入 Bind；planner 不接管 task iterator、gang readiness
或 `Statement` operation ownership。它必须是纯函数式的，具体 context 结构、索引和有限预算留在实现阶段，不作为 framework API。

当前 `BindContextHandler.SetupBindContextExtension` 是 per-Pod、无 error 返回的附加接口。Alpha 使用它或等价的 BindContext
扩展把 scheduler-owned assignment annotation 带入 Pod Bind；它不负责跨 Pod rollback，也不声称多 Pod Bind 原子性。

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

建议在 `PodGroupSpec` 和 `SubGroupPolicySpec` 增加 `DeviceTopology *DeviceTopologySpec`；VCJob、PodTemplate 等 ergonomic
字段延期，后续若增加必须先转换到生成的 PodGroup/SubGroup canonical spec。scheduler 不直接读取这些 source。
`hard/soft` 刻意与现有 `NetworkTopologyMode` 术语保持一致，不另造 `Required/Preferred` 的同义 API。

默认和校验规则：

- `mode` 默认 `hard`；`applyTo` 默认 `Pod`；
- `mode` 只接受 `hard/soft`；无效值必须拒绝，不能默认成 `hard`；
- `scope` 只接受 `Node/Fabric`；`domainClass` 必填，并满足 API review 后冻结的 DNS-label 风格语法；
- V4 Go/canonical schema 不暴露 `tier/tierName`；只有实际 served 过 V3 的 version 才保留 reject-only legacy tombstone 并用 CEL 拒绝，
  annotation 使用 strict decoding，不能 prune 后按默认 class 调度；
- `scope=Fabric` 只匹配 Provider 明确发布的 Fabric；HyperNode 或 IB/RoCE 可达性不能隐式满足；
- `domainClass` 必须解析到同一 `resourceName + scope` 的管理员 catalog；未知 class 对 hard/soft 都是 authoring error，已知但
  Provider 不能确认 selected DeviceKey 的 hard class fail closed；
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

Public API 同样不暴露：Device/Domain/Fabric ID、selected Node、Health、AllocationState、snapshot revision 或 Provider payload。

### 3.3 Alpha canonical authoring contract

首个 Alpha 只冻结一个 canonical authoring 路径：用户直接提交 `PodGroup.spec.deviceTopology` typed field，scheduler 只读取这份
canonical spec。VCJob/partition 字段、Deployment/ReplicaSet/StatefulSet PodTemplate annotation 和 bare-Pod ergonomic 入口延期，
后续若加入必须先把输入转换为同一 typed field，并单独评审冲突与版本语义。

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

未知字段、重复 JSON key 或非 canonical 类型必须拒绝，不能由宽松 JSON 解码静默丢弃。对象大小和 policy 数量只设实现阶段的
最小防护，不在 Alpha 合同中冻结具体数字。

~~~mermaid
flowchart TD
    Direct["Direct PodGroup typed spec"] --> Canonical["PodGroup.spec.deviceTopology"]
    Canonical --> Scheduler["Scheduler reads only canonical spec"]
    Deferred["VCJob/PodTemplate ergonomic sources"] -. "deferred" .-> Canonical
~~~

支持路径：

| 创建方式 | Alpha 状态 | canonical PodGroup |
| --- | --- | --- |
| 直接 PodGroup | Alpha canonical；webhook 只做 schema/canonical 校验 | 用户提交的 PodGroup |
| VCJob/partition | 延期；不作为 Alpha 必需输入 | 后续转换为 typed spec |
| Deployment/ReplicaSet/StatefulSet/PodTemplate | 延期；不把 annotation 当平行 policy | 后续转换为 typed spec |
| bare Pod | 延期；普通 Pod 不因 annotation 自动获得 Alpha policy | 后续单独评审 |

### 3.4 Authority、优先级与冲突

Alpha 不冻结多源优先级。canonical source 只有已持久化的 PodGroup typed spec；任何延期 source 在进入 Alpha 前必须先物化为
该字段。这样不会出现 owner/template/Pod annotation 相互覆盖，也不需要跨组件 fingerprint 协调。

冲突解除前，xPU 的 `JobValid/JobEnqueueable` gate 必须识别 controller 写入的
`PodGroupUnschedulable=True, Reason=XPUTopologyPolicyConflict`，即使 canonical spec 为空或仍是旧值也保持 Group Pending。
这个 Condition 只是 fail-closed validation gate，不是第二份 policy；scheduler 的 workload topology 语义仍只来自 canonical spec，
并且绝不把用户输入当成运行时 assignment。scheduler/provider 另用受控的 `volcano.sh/xpu-assignment` Pod annotation 记录已选 DeviceKeys，
只在成功 Bind 的 Pod 上作为恢复输入。

### 3.5 Policy 更新合同

每份 canonical spec 计算 `PolicyFingerprint`。规范化必须包括 default 后的 `resourceName/mode/applyTo/scope/domainClass`、
排序后的 policies 和 label selector，但不能包括对象 `resourceVersion` 或其他存储元数据。Alpha catalog 固定不变，policy compile
直接使用这份固定 class 定义；plan/evidence 只需记录实际选择的 class、Domain/Fabric 和 membership。

语义更新只在 Group 没有已绑定成员时允许。Alpha 以已绑定/运行 Pod 作为 anchor activity 的可观测事实：

~~~text
没有已绑定/运行成员 Pod
AND 没有已经进入本轮 Bind 的成员
~~~

规则如下：

1. 没有已绑定成员时允许更新；新的 policy fingerprint 使旧 plan、fit cache 和 placement hint 全部失效；
2. 已有已绑定成员后，只允许 canonical fingerprint 完全相同的 no-op 更新；
3. 活动状态下修改 `mode/resource/scope/domainClass/applyTo/selector` 必须由 webhook 或 controller 拒绝；
4. 删除 policy 也是语义更新，不能借删除绕过活动期保护；
5. 由于 Alpha 只有 PodGroup typed source，普通 resourceVersion/CAS 足以保护更新；VCJob/template 传播规则延期；
6. 后续 ergonomic source 若产生不同 policy，必须创建新的作用单元或先完成独立迁移评审；
7. Pod cache 读取不一致、assignment annotation 缺失或 Node replacement 时按“不可恢复”处理，保持 hard Group Pending，而不是允许更新或猜测。

## 4. ID、Node 重建与失效

### 4.1 三层名称不能混用

V4 明确区分：

| 层 | 示例 | 语义 |
| --- | --- | --- |
| Provider identity | `annotation-v1` | 哪个启用的 source contract 发布事实 |
| Source-local value | `GPU-4c2e`、`nvlink-0`、`fabric-0` | payload 中稳定但有明确作用域的值 |
| Canonical scheduler key | 下面定义的 `DeviceKey/LocalDomainKey/FabricKey` | cache、plan、Pod assignment 和 fingerprint 使用的完整身份 |

`ProviderID` 绝不能被当成 `devices[].id`；`devices[].id` 也不能脱离 NodeUID、resource 和 identity namespace
直接成为 scheduler map key。

### 4.2 DeviceID、DomainID 与 FabricID

~~~go
// Source value types are opaque values from one validated provider contract.
type SourceDeviceID string
type SourceDomainID string
type SourceFabricID string

// DeviceID is the normalized identity from a validated provider contract.
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
- `DeviceID` 是 Provider identity contract 下的 normalized value；它自身不是 scheduler map key，两个 Node 上可以相等；
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
    Name string
    UID  types.UID
}

type NodeObservation struct {
    Identity         NodeIdentity
    ResourceVersion string // source-correlation metadata, not identity
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
4. Alpha 不维护独立的 old-UID allocation tombstone；包含旧 UID 的 assignment 只能作为不可恢复输入；
5. 新 UID 不继承旧 UID 的 source generation、Domain membership、Health 或 Free 状态；
6. 迟到的旧 UID `ReplaceFacts/ClearFacts` 即使 NodeName 相同也必须拒绝；
7. 新 Node 完成一次有效 `ReplaceFacts` 或 `ClearFacts` 后，readiness 才从 `Pending` 进入 `Synced`；
8. 包含旧 UID 的 Fabric 立即不可用，Provider 必须以更新 generation 重新发布并解析完整成员集合；
9. 不能把旧 UID 的 assignment annotation 或 anchor 映射到新 UID；相关 Group hard 调度保持 Pending。

~~~mermaid
stateDiagram-v2
    state replacement <<fork>>
    [*] --> OldActive: Node uid-old observed
    OldActive --> OldInvalid: Node deleted
    OldActive --> replacement: same name uid-new observed
    replacement --> OldInvalid: invalidate old facts and references
    replacement --> NewPending: create new identity
    OldInvalid --> [*]: old references remain diagnostic only
    NewPending --> NewSynced: valid ReplaceFacts or ClearFacts
    NewPending --> NewPending: invalid or delayed old-UID update
~~~

## 5. Provider、Fabric 与 Cache

### 5.1 ResourceTopologyDescriptor 与 DomainClass catalog

`domainClass` 是管理员面向 workload 发布的稳定合同。workload author、Node annotation publisher 和 Provider 都不能各自解释
同一个裸字符串。Alpha 固定一份 scheduler-side catalog；scheduler/plugin 是运行时权威消费者，webhook/controller 只校验字段形状，
Provider 只把事实映射到 scheduler 已加载的 class。Alpha 不支持运行期间修改该定义，后续如需增加或改变 class，另开设计和兼容性评审：

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
5. catalog 内容在 Alpha 生命周期内不可变；非 scheduler 组件不通过共享控制面读取它。将来如需允许多个候选 class，应另行评审
   `acceptableDomainClasses` 等显式 API，不能通过修改现有 class 含义或静默 fallback 实现。

`Description` 是管理员与受信 Provider 之间的语义合同，不是 scheduler 可从图中自行证明的带宽标准。Normalizer 能验证的是
catalog 引用、resource/scope、显式 membership、closure 与 ID 一致性；Provider validation 可验证厂商事实。管理员配置和受信
publisher 是 class 语义的信任根，V4 不声称仅凭 `name=local-scale-up` 能推导或测量 NVLink/HCCS 性能。

V4 不接受 V3 的 workload `tier/tierName`。迁移必须由用户/controller 显式把旧类别映射成 catalog 中的 `domainClass`，创建
新的 canonical policy。因为 structural schema 可能在 admission webhook 前 prune 未知字段，不能只从 Go type 删除旧字段：
只有在确认某个 served version 曾实际暴露 V3 字段时，该 version 才需要在对应 PodGroup/VCJob schema 保留仅用于拒绝的
legacy tombstone，并以 CEL `!has(self.tier) && !has(self.tierName)` 拒绝；若没有实际 served V3，则不添加这套兼容字段。
Go Public type 和 canonical model 永不暴露它们；不能让 pruning 把已发布旧 policy 变成缺省策略。

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
- Kubernetes `resourceVersion` 是 opaque 字符串，只作为 Provider 观测元数据和诊断关联，不是 Node identity；同 UID 的 Node 元数据更新不使 facts 失效，也不做数值排序；
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
6. owner 的新 generation 可以完整替换成员；被删除成员立即从可用 membership 中移除，不进入 candidate index；
7. owner 删除、换 UID、stale 或 ClearFacts 会立即使 Fabric 对新 hard plan 不可用；
8. owner identity 变更不能继承旧 membership；新 owner 必须重新发布完整集合，旧声明立即失效；
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
资源视图决定。Alpha 不用 topology cache 二次扣减普通 Node 资源，也不把 source 中“存在且健康”解释成外部 allocation 状态。

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
closure、Provider RPC 或全量 clone。
它保证 Nodes/Jobs/Queues 与 DeviceTopology 来自同一次 `SchedulerCache.Snapshot()` 的观察点，因此：

- 不需要第二次 topology snapshot 调用；
- 不需要比较后“最多重试两次”的 magic retry；
- plugin 不得在 `OnSessionOpen` 中 type-assert cache 再取一个较新的 topology view；
- planning 全程使用 `ClusterInfo.DeviceTopology.Revision`；Bind 前只重新校验 plan 引用的 NodeUID、DeviceKey 和 membership，不产生外部副作用。

### 5.6 Provider 重活与 immutable publish

“进入同一次 Snapshot”只有在 publish 与 Node identity 也被协调时才成立。V4 只冻结以下可观察边界，不冻结具体锁名、锁序或
并发原语：

~~~text
Node informer event
  -> record the current NodeName/UID; invalidate old candidate facts only when UID changes or the Node is deleted
  -> do parse, normalize, graph closure and Provider validation outside cache locks
  -> before publish, re-check NodeName/UID and source generation
  -> publish a new immutable topology snapshot
~~~

`SchedulerCache.Snapshot()` 在同一观察点读取集群对象与已发布的 immutable topology pointer；它不执行解析、Provider RPC 或全量
clone，也不要求第二次 topology snapshot 或固定次数的 retry。Provider parse、Normalizer 重活、外部 RPC 和持久 I/O 不得在
cache synchronization 的临界区执行。具体同步实现必须避免旧 parse 结果覆盖新 Node，但不作为 Alpha framework contract。

Provider 校验失败只会使相应 facts 不可用；已通过 Provider/canonical 校验的 facts 仍可发布给 `soft` advisory。Alpha 不把外部
allocation 状态作为 topology ingestion 的硬前置。

如果锁外工作结束后 NodeName/UID 或 source generation 已变化，结果直接丢弃并重新入队；同 UID 的 ResourceVersion 变化不单独使结果失效，不能把旧 parse 结果发布给新 UID Node。
Node identity 的 Pending/invalidation 和 `sc.Nodes` 可见性必须在同一 cache 观察点更新，使任何 Session 至多看到：

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
    SchedulableByDomain  map[LocalDomainKey][]DeviceKey
}
~~~

`DomainRef` 是 snapshot 内部带判别字段的只读引用：`Scope=Node` 时只允许 `LocalDomainKey`，`Scope=Fabric` 时只允许
`FabricKey`。Alpha 只要求 `DomainsByClass` 和 `SchedulableByDomain` 两个规划索引；其他索引可在实现阶段按 profile 派生，
不作为 plugin contract。所有 index 必须只包含 class、resource 和 scope 一致且已通过 descriptor 校验的对象，并按 canonical key 稳定排序。
Provider capability 是受控注册事实，不从当前是否恰好有一个 Domain 对象反向猜测支持能力。
固定 catalog 不在运行期间切换；catalog 缺失或非法时 topology manager 不 Ready，不能用另一份 class 定义继续规划。

Plan 记录全局 `Revision` 用于诊断，并校验实际引用对象的 membership、source generation 和 NodeUID。提交时不要求整个 `Revision`
完全相等；Alpha 在 Bind 前只重新验证 plan 引用对象，避免无关 Node heartbeat 使所有计划失效。

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
5. `SaveOperations/RecoverOperations/Merge` 只复制可试算的 plan context，不能复制外部 allocation state。

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
- 不写入 PodGroup anchor 摘要；恢复直接扫描成员 Pod，避免引入可过期的重复状态源；
- Alpha 不处理 Pod 消失后的外部 allocation lifecycle；相关状态不作为后续 Session 的恢复事实。

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

Planner 是 side-effect-free 的，不能修改外部或 Session 之外的 live state。建议顺序：

1. Task 按候选 Domain 最少、请求 Device 数最多、现有 Task order、PodUID 排序；
2. Domain/Device 按 3.2 的 deterministic Compact 顺序枚举；
3. plan-owned `NodeInfo` clone 重放普通 `AddTask` 资源记账，防止多个 Task 分别可放但合计超量；
4. greedy 无法完成时可做实现阶段确定的有限回溯；具体状态数、候选数和时间阈值不进入 Alpha plugin contract；
5. 规划超过实现阶段的有限预算时保持 Pending，并记录低基数诊断，不假报普通资源不足；
6. provisional plan 不产生外部副作用；只有 winning Statement 的 final plan 可以进入现有逐 Pod Bind，并写入 scheduler-owned
   assignment annotation。

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
因此返回 `XPUDeviceDomainFragmented`，不进入 Bind。

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

type TopologyTaskPlacement struct {
    PodUID          types.UID
    NodeName        string
    NodeUID         types.UID
    Assignments     []TopologyContainerAssignment
}

type TopologyPlacementPlan struct {
    GroupRef                 TopologyGroupRef
    PolicyFingerprint        string
    SnapshotRevision         uint64 // paired snapshot used for planning
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
assignment annotation 不附带 `localDomainKeys`、`fabricKeys`、`groupRef` 或 `planDigest` 等派生字段。planner 只返回满足
当前 policy 的 NodeUID、DeviceKeys 和必要的 GroupRef；Bind 前重新校验这些引用，不能由 Provider 或后续 callback 重选 ID。

### 7.3 Provider/Device Plugin identity contract

~~~go
type DomainClassCapability struct {
    Class DomainClassKey
}

type XPUAssignmentProvider interface {
    IdentityContract() DeviceIdentityContract
    DomainClassCapabilities() []DomainClassCapability
    ValidateTopology(ctx context.Context, facts NodeTopologyFacts) error
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

Alpha 不定义 scheduler-side allocation owner。scheduler planner 拥有最终 DeviceKeys 选择权；Provider 只能校验或消费 selected keys。
Provider identity contract 必须与
`DeviceIdentityContract{ProviderID, Namespace, ResourceName}` 完全匹配；`DomainClassCapabilities()` 只声明 Provider 能解释哪些
固定 catalog class，不创建 class、不改变 `DomainClassKey` identity，也不能用通配符绕过 hard policy。Provider 能发现某 class 但
不能消费或确认选中的 DeviceKeys 时，`soft` 仍可使用有证据的 preference，`hard` 对相关 workload 返回
`XPUAssignmentNotEnforceable` 并保持 Pending。

Plain Device Plugin 数量和 kubelet `GetPreferredAllocation` 不是 scheduler-selected assignment 确认接口。
未经验证时它们只允许 `soft` advisory；不能让 hard workload 进入 Bind。Alpha 只要求 Provider 能确认选中的 DeviceKeys，
并将 assignment 保留在已绑定 Pod 上。

## 8. Alpha recovery 边界

### 8.1 Alpha Pod-derived recovery

Alpha 不维护 scheduler-side allocation owner，也不定义外部 allocation lifecycle。
Session 打开时从已绑定/运行成员 Pod 的 `spec.nodeName` 与 `volcano.sh/xpu-assignment` 读取恢复输入：

1. 只接受 `AllocatedStatus` 且 `NodeName` 非空的成员；未绑定 Pod 不建立跨 Session anchor；
2. assignment annotation 必须能解析出 `resourceName/provider/deviceKeys`，并通过当前 NodeUID 和 topology snapshot 校验；
3. 成员映射到同一 LocalDomain/Fabric 时建立本次 Session 的 `PodDerivedGroupAnchor`；缺失或冲突则 hard Pending；
4. metrics 和日志只能作为诊断信息，不承担恢复事实；
5. 只有一个 active scheduler leader 负责同一 Group；恢复输入仅来自已绑定 Pod，不协调外部 allocation owner。

### 8.2 PodGroup Condition/Reason 写入路径

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
count 和 provider 状态，但不能把 DeviceID/PodUID 用作 metrics label。

Alpha 只冻结少量可供 controller/scheduler 判断的 outcome 类别；具体细分 reason、message 和指标标签由实现阶段确定，不能成为
跨组件 API 依赖。最小集合为：

~~~text
XPUTopologyPolicyInvalid
XPUTopologyPolicyMutationForbidden
XPUTopologyDataNotReady
XPUTopologyUnavailable
XPUAssignmentNotEnforceable
XPUTopologyResolved
~~~

策略 authoring 冲突出现在 scheduler Session 之前时：

- admission webhook 可直接拒绝请求；
- auto-PodGroup controller 遇到成员 annotation 冲突时，不覆盖 canonical spec，并通过 status-subresource retry helper 写入
  `XPUTopologyPolicyConflict`，同时记录 Event；
- controller 写回与 scheduler 写回都使用 resourceVersion conflict retry，不能覆盖对方的无关 status 字段。

## 9. Alpha 失败矩阵

> Alpha 的失败处理以 Pod annotation、NodeUID/topology 校验、Group Pending 和现有逐 Pod Bind 为准，
> 不创建 scheduler-side allocation owner。

| 失败点 | hard 行为 | Alpha 结果 | Condition/Reason |
| --- | --- | --- | --- |
| gate 关闭但收到新建/新增 xPU policy | admission reject | 不产生 canonical active policy | admission error / implementation-defined policy-invalid |
| gate 与 plugin 只启用一个 | 首次启动不 Ready | 不启动 topology manager；热更新行为属于后续发布流程 | configuration error |
| plugin 参数非法或 Alpha 配置带 resource owner | 首次启动不 Ready | Alpha 不创建跨系统 owner；不冻结旧配置保留语义 | configuration error |
| 有已绑定 topology Pod 时移除 plugin 或改变 policy identity | 拒绝配置切换/更新 | 已绑定 Pod annotation 继续作为只读恢复输入；具体迁移/关闭顺序属于后续发布验收 | configuration error / `XPUTopologyPolicyMutationForbidden` |
| 新 Node provider 仍 Pending | 当前候选 fail closed | 不建立新 assignment；旧 UID 不复用 | `XPUTopologyDataNotReady` |
| annotation/typed policy 冲突 | Group Pending | 不产生 plan | `XPUTopologyPolicyInvalid` |
| direct PodGroup V4 policy 缺 `domainClass`，或在实际 served V3 version 上携带旧 `tier/tierName` | direct PodGroup admission reject；无 served V3 时不新增旧字段兼容路径 | 不产生 canonical plan | `XPUTopologyPolicyInvalid` |
| `(resourceName, scope, domainClass)` 不在 catalog | policy fail closed | 不产生 plan | `XPUTopologyPolicyInvalid` |
| class 已知但 Provider 不能消费/确认 DeviceKey | Group Pending | 不产生 Bind/assignment | `XPUAssignmentNotEnforceable` |
| 活动期修改 policy | 拒绝更新；旧 fingerprint 继续有效 | 不改变已绑定 Pod 的 anchor | `XPUTopologyPolicyMutationForbidden` |
| Provider stale/invalid | 不使用新 hard plan | 已绑定 Pod 不被内存回滚；新成员保持 Pending | `XPUTopologyDataNotReady` |
| Node 同名重建 | 新 UID Pending；旧 key 不候选 | 旧 Pod assignment 不映射到新 UID；旧引用只作诊断 | `XPUTopologyDataNotReady` |
| Fabric owner/member UID 变化 | Fabric 不可用，等待 owner 重发完整集合 | 旧 membership 立即失效 | `XPUTopologyUnavailable` |
| 6+2 请求同 Domain 8 | Node 不可行 | 不进入 Bind | `XPUDeviceDomainFragmented` |
| 请求来自多 Container/init/shared unit | Group Pending 或 admission reject | 不进入 Bind | `XPUTopologyUnsupportedPodRequest` |
| Provider 不能消费/确认 selected DeviceKey | hard 不调度 | 无 assignment/Bind | `XPUAssignmentNotEnforceable` |
| planner 超出实现阶段的有限预算 | retryable Pending | 不进入 Bind | `XPUTopologyUnavailable` |
| assignment annotation 缺失/非法（Alpha） | Group Pending | 不建立 anchor，不从 GPU index 或其他派生信息猜测 | `XPUAssignmentNotEnforceable` |
| 已绑定成员的 anchor 冲突（Alpha） | Group Pending | 不选择第二个 Domain/Fabric | `XPUTopologyUnavailable` |
| final NodeUID/DeviceKey/membership 改变（Alpha） | 丢弃并重新规划 | 不进入 Bind；不修改外部 allocation 状态 | `XPUTopologyDataNotReady` |

soft policy 的 topology data/provider 不可用时只失去相应 preference，并记录低基数退化原因；它不能让一个普通 Predicate
失败的 Node 重新可调度，也不改变外部 allocation 状态。

## 10. 验证与验收计划

### 10.1 Feature gate、plugin 配置与生命周期

- `XPUTopologyAwareScheduling` 注册为 Alpha、默认 `false`；默认 scheduler 配置不含 `xpu-topology-aware`；
- scheduler gate × plugin 四种组合全部覆盖；二者未同时启用时 activation fail closed，参数非法时只阻止相关 xPU policy 落入普通路径；
- admission gate 关闭时拒绝创建/新增/修改 policy，但允许删除 policy；scheduler core guard 阻止存量非空 policy 落入普通路径；
- admission gate 开启时只做 policy schema/canonical 校验；scheduler plugin 缺失、参数非法或 catalog 不可读时 scheduler 对非空 policy fail closed；
- Helm render 测试证明 gate/plugin 的静态配置可交付；不要求 scheduler/admission 共享运行时 digest；
- `New` 的参数解析覆盖缺省值、类型错误、格式错误和范围错误；非法 Provider 配置不能让 xPU policy 落入普通调度路径；
- gate/plugin 启用但 scheduler catalog 不可读时 scheduler readiness fail closed；单个 Node provider facts Pending
  只使对应 hard 候选/Job Pending，不阻塞整个 scheduler；
- `New/Name/OnSessionOpen/OnSessionClose` 生命周期测试证明每个 Session 只使用 `ClusterInfo` 配对 view，且 close 不释放 Provider/cache；
- `OnSessionOpen` 注册 JobValid、Predicate、BatchNodeOrder 和 overlay EventHandler；Group planner 由 allocate 私有调用；不注册 Reserve、第二个 HyperNode gradient
  或复制 `JobReadyFn`；
- enabled-no-policy 的回归结果与 plugin-disabled 调度决定一致；只允许出现受控的 snapshot/回调开销；
- plugin removal、Provider/identity 变更必须保持非空 policy fail closed；已有绑定 Pod 的 semantic mutation 仍被拒绝，具体 drain 属后续发布设计；
- 单 active leader 的配置读取与 Session 重建测试通过；恢复只依赖已绑定 Pod 的 Pod-derived anchor。

### 10.2 API 和 canonicalization

- `hard/soft` default 和 invalid value reject；`scope/domainClass` 必填且语法校验；
- V4 Go/canonical model 不暴露旧字段；Alpha 只验证 direct PodGroup typed spec 的 strict schema/canonicalization。若确认某个
  served V3 version 实际暴露过 `tier/tierName`，仅对该 version 增加 reject-only tombstone + CEL，证明即使客户端未启用
  `fieldValidation=Strict` 也不会在 prune 后接受；没有实际 served V3 时不新增兼容字段，VCJob 等延期入口不纳入本 Alpha 验收；
- 同名 class 在不同 resource/scope 下编译成不同 `DomainClassKey`；workload 不能用具体 Domain ID 充当 class；
- catalog 未声明的 class 对 hard/soft 都按 authoring error 拒绝；已知但不可 exact 执行的 hard class fail closed；
- direct PodGroup canonical spec 的 normalized policy 产生稳定 `PolicyFingerprint`；Pod assignment annotation 不参与用户 policy
  fingerprint，VCJob/其他 source 的 field fingerprint 待其未来接入时另行定义；
- `PolicyFingerprint` 包含规范化后的 `domainClass`；固定 catalog 只提供 class 定义，不参与 workload 版本判断；
- reordered/exact-duplicate policies 得到同一 fingerprint，duplicate Group policy 只生成一个 anchor selection；重叠 selector 的
  不同 class fail closed；
- annotation 无 `allocationStrategy`，Public schema 也拒绝该字段；
- direct PodGroup 与 Pod annotation 相同可接受，不同 fail closed；
- VCJob/child annotation 多源冲突不属于 Alpha canonical path；后续 source 接入前必须单独定义冲突规则；
- canonical spec 为空但 `XPUTopologyPolicyConflict=True` 时，`JobValid/JobEnqueueable` 仍阻止调度；
- VCJob/Deployment/StatefulSet/bare Pod ergonomic source 在 Alpha 中不产生 topology policy；
- scheduler 测试证明它只读取 PodGroup canonical spec，不读取 workload annotation；
- quiescent 更新成功并废弃旧 plan；活动期 mutation 被拒绝；
- invalid annotation 不回落成默认 hard，也不按无 policy 调度。

### 10.3 Identity、Provider 和 Snapshot

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
- provider parse 在锁外；并发/race test 不允许旧 parse 覆盖新 identity；
- 不存在 dual-snapshot retry count 或 plugin 二次捕获。

### 10.4 Filter、Compact 和 Group

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

### 10.5 回归、E2E 与性能

- 未配置 policy 时 network-topology-aware、gang、Statement 和现有 device paths 行为不变；
- HyperNode API 不增加 Device 或外部 allocation 字段；
- Mock/KWOK 覆盖 fragmented/fitting Domain、explicit Fabric、Node replacement、health update 和失败回滚；
- Provider assignment identity 与 Pod-derived anchor 只在已验证的范围内声明；
- 后续发布验收再 benchmark plugin disabled、enabled-no-policy、soft 和 hard；Alpha 只要求基本可观测性，不冻结 P50/P95/P99、CPU/RSS、GC 或吞吐阈值；
- metrics 只保留低基数的 provider/snapshot/plan/recovery 结果；具体指标名、budget 计数和 reason taxonomy 由实现阶段确定；
- Event/metrics label 禁止使用 raw DeviceID 或 PodUID。

## 11. PR 拆分与交付阶段

### 11.1 PR 0：API 与最小 framework contract review

- 冻结 `XPUTopologyAwareScheduling` gate、`xpu-topology-aware` plugin 名、双重启用/禁用和 fail-closed 合同；
- 冻结最小 plugin arguments、`New` 参数解析、activation guard、process manager 与 per-Session plugin 的生命周期边界；不冻结 reload/drain 状态机；
- 冻结 `DeviceTopologySpec` 的 hard/soft、applyTo、scope/domainClass，并明确拒绝 `tier/tierName`；
- 确定 scheduler-side `ResourceTopologyDescriptor` catalog 的固定语义；不冻结跨组件共享读取协议；
- 明确 Public API 无 allocationStrategy；
- 评审 Pod annotation key、canonicalization 和 mutation webhook；
- 冻结 `volcano.sh/xpu-assignment` annotation 的最小字段、canonical DeviceKey 和 Pod Bind 传递路径；
- 接受 Session-local anchor、单 leader、缺失/冲突 Pending 和现有逐 Pod Bind 边界。

### 11.2 PR 1：canonical model、Provider 和 identity

- 注册默认关闭的 Alpha feature gate、plugin builder、scheduler/policy activation guard 和 Helm scheduler/admission gate 配置；
- 实现 gate/plugin 组合校验、存量 typed-policy guard 和 scheduler-side catalog readiness；不实现 activation digest；
- 建立 process-scoped topology manager；plugin `New/OnSessionOpen/OnSessionClose` 不拥有 Provider 生命周期；
- 实现 `DomainClassKey`、descriptor catalog、Device/LocalDomain/Fabric IDs 和 keys；
- `DeviceDomain/FabricDomain.Class`、`DomainsByClass` 与固定 catalog 校验后的 topology publish；
- Node UID identity、ResourceVersion observation metadata、Pending/Synced、旧 UID assignment 失效；
- Annotation/Mock Provider、schema/security 和 freshness；
- single-owner complete Fabric declaration；
- immutable topology objects、indexes 和 unit tests。

### 11.3 PR 2：ClusterInfo snapshot 与 Advisory MVP

- `ClusterInfo.DeviceTopology` 和同一次 `SchedulerCache.Snapshot()` 捕获；
- 在重新校验 NodeUID/source generation 后 publish immutable pointer；具体锁序留给实现；
- PodGroup canonicalization 和最小 Condition outcome；VCJob/PodTemplate source 延期；
- 实现 plugin 的 JobValid、Predicate、BatchNodeOrder 与 Session overlay EventHandler；
- `scope/domainClass` compiler、Node/Fabric class filter、soft score、internal deterministic Compact；
- plugin-disabled/no-policy 回归和并发 snapshot 测试。

本阶段可称 Advisory MVP。没有可消费/确认 DeviceKey 的 Provider 时，hard 保持 fail closed；它不创建外部 owner。

### 11.4 PR 3：Pod-derived Topology Alpha

- 单普通 Container 整卡 validation 和完整 assignment identity；
- side-effect-free group planner、AdmissionSet 和 Session-local anchor；
- scheduler-owned `volcano.sh/xpu-assignment` 随成功 Pod Bind 保留；
- 后续 Session 从 bound Pod 的 NodeName + DeviceKeys 恢复 Group anchor；
- action bypass、Node replacement、annotation 缺失/冲突和单 leader 测试；
- 不新增外部 allocation owner 或多 Pod Bind barrier。

### 11.5 PR 4：Fabric/Group E2E 与后续发布验收

- explicit Fabric + HyperNode intersection；
- `Pod+Node` 与 `Group+Fabric` 双 class policy E2E；
- 跨 wave anchor E2E；
- provider churn、Node replacement、Fabric owner replacement；规模、性能、真实硬件、升级/回滚/drain 演练均为后续发布验收，不是 Alpha 合同前置；
- metrics、runbook、Helm 用户文档和发布清单；
- Provider assignment identity 和 Pod-derived Alpha 在已验证范围内发布。

## 12. 兼容性、明确不做与开放问题

### 12.1 兼容原则

1. feature gate 默认关闭、默认 scheduler 配置不含 plugin；未配置 `DeviceTopology` 时零调度行为变化；
2. HyperNode 仍只拥有 Node/网络层次；
3. Job/SubJob readiness 不复制；
4. normal resource fit 不被 xPU topology view 二次扣减；
5. existing deviceshare/DRA 不与本文的 assignment identity 混用；
6. soft 不制造 hard filter，hard 不静默降级；
7. Mock/Annotation 不升级成真实硬件 exact-ID 证据；
8. Kubernetes Binding 不描述为整组原子提交；
9. feature/plugin/Provider identity 配置不一致时 fail closed；
10. gate/plugin/catalog 失配时非空 policy 必须 fail closed；具体关闭、升级和 drain 流程属于后续发布验收；
11. V4 与 V3 schema 不做透明兼容；旧 `tier/tierName` 只有在确认实际 served 过时才启用 reject-only 兼容规则，不能 silent prune/fallback；
12. Alpha 不冻结跨 scheduler/admission/controller 的 activation digest、共享 catalog 协议或过细 reason taxonomy。

### 12.2 已决定，不再作为开放问题

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
- topology-aware victim choice、外部 allocation lifecycle 和多 Pod 原子 Bind 均不属于首个 Alpha；
- feature gate 名为 `XPUTopologyAwareScheduling`、Alpha 默认关闭；scheduler plugin 名为 `xpu-topology-aware`，必须双重显式启用；
- process-scoped topology manager 与 per-Session plugin 生命周期分离；`OnSessionClose` 不停止 Provider/cache；
- scheduler planner 拥有最终 DeviceKeys 选择权；Provider 不提供 chooser，只做 selected-key 校验/消费；
- Group planner 先作为 allocate 内部集成，不新增通用组规划注册 API；
- Alpha 的首个 Provider 目标为 NVIDIA，resource 为 `nvidia.com/gpu`；XPU-01 使用 nvml-mock 验证 selected DeviceKey、annotation
  和恢复。

### 12.3 仍需社区评审的 P0/P1 问题

| 优先级 | 问题 | 不确定的只是 | 已固定的底线 |
| --- | --- | --- | --- |
| P0 | Alpha 的 Pod assignment annotation 传递形状 | BindContext extension、Pod metadata 写入位置和 key 命名 | 已绑定 Pod 必须保留 NodeName + canonical DeviceKeys；摘要不能成为唯一事实源 |
| P0 | `ResourceTopologyDescriptor` 的 scheduler-side 载体 | 静态配置、安装文件或 ConfigMap 的交付形状 | DomainClassKey 语义固定；scheduler 不得在未知 class 上规划；运行期间不可修改 |
| P1 | allocate 内部 planner 的插入点 | 是否复用现有 action helper 或新增私有 package | side-effect-free、paired snapshot、final Node/DeviceKey revalidation；不冻结通用 framework hook |
| P1 | activation guard 的 scheduler 落点 | scheduler 配置加载、process manager readiness 与 policy core guard 的最小组合 | 不新增无调用链的 generic validator；错误配置不能让非空 policy 进入普通调度 |
| P1 | `domainClass` 名称语法与 catalog 演进规则 | DNS label 的精确限制、废弃窗口和版本策略 | key 至少包含 resource+scope+name；V4 拒绝旧字段和隐式 fallback |
| P1 | 是否需要显式多个 acceptable classes | 独立 API 形状和优先级 | 首个 Alpha 只接受单一精确 class；不得用 internal rank 自动扩大 |
| P1 | Fabric mock 与真实 Fabric 验收的发布节奏 | milestone 顺序 | 普通网络不可自动推导 Fabric；真实硬件不阻塞 Alpha 文档合同 |

### 12.4 后续研究清单

- topology-aware victim selection；
- DRA claim selector 和 ResourceSlice Provider；
- multi-container/init-container lifecycle；
- MIG/vGPU/shared geometry；
- cluster-scoped Fabric authority；
- link-aware ring/collective communication planning；

## 附录 A. 实现映射

| 区域 | 当前源码 / 建议文件 | 责任 |
| --- | --- | --- |
| Feature gate | `pkg/features/volcano_features.go` | 注册 `XPUTopologyAwareScheduling` Alpha，默认 `false` |
| 进程参数 | `cmd/scheduler/main.go`、`cmd/webhook-manager/main.go` | 复用现有 `--feature-gates` plumbing，scheduler/admission 同名启用 |
| Helm 配置 | `installer/helm/chart/volcano/values.yaml`、`templates/scheduler.yaml`、`templates/admission.yaml` | 传递两个 component gate；完整覆盖 scheduler ConfigMap 时保留其他 plugin |
| Scheduler config validation | `pkg/scheduler/util.go`、`pkg/scheduler/scheduler.go`、`pkg/scheduler/framework/plugins.go`（建议扩展） | gate/plugin 矩阵和最小静态 option 校验；reload/drain 属后续发布 |
| Plugin factory | `pkg/scheduler/plugins/factory.go` | 注册 `xpu-topology-aware` builder；activation guard 位于 scheduler/policy 路径 |
| 当前 ClusterInfo | `pkg/scheduler/api/cluster_info.go` | 增加 immutable `DeviceTopology` 字段 |
| 当前 snapshot | `pkg/scheduler/cache/cache.go:SchedulerCache.Snapshot` | 同主锁观察点捕获 cluster + topology pointer |
| Session 初始化 | `pkg/scheduler/framework/session.go:openSession` | 从 ClusterInfo 取得同一 topology view |
| Topology manager | `pkg/scheduler/topology/manager/`（建议） | process-scoped Provider/cache/snapshot publisher 生命周期；不承担跨组件 digest |
| Provider/normalizer | `pkg/scheduler/topology/provider/`（建议） | Annotation/Mock parse、validate、NodeUID/RV/generation |
| Descriptor catalog | Alpha scheduler-side 静态配置/安装输入（具体载体待定） | 管理员 class 合同和只读加载 |
| live topology | `pkg/scheduler/cache/topology_cache.go`（建议） | facts、descriptor view、readiness、class indexes、immutable publish；Pod assignment 由 Pod/cache 读取 |
| Pod canonicalization | `pkg/controllers/podgroup/pg_controller_handler.go`、Job controller、webhook | authoring source 转换、冲突和 mutation validation |
| Public types | `staging/src/volcano.sh/apis/pkg/apis/scheduling/`、batch API | `DeviceTopologySpec` typed schema 与生成代码 |
| 当前 Statement | `pkg/scheduler/framework/statement.go` | 保留普通 Commit；Alpha 只接入 Session-local group plan，不新增提交参与者 |
| 当前 bind path | `pkg/scheduler/cache/cache.go:AddBindTask/executePreBinds/BindTask` | Alpha 复用逐 Pod Bind，并确保 Task PodAnnotations 随 Bind 保留 |
| Condition | `pkg/scheduler/framework/session.go:UpdatePodGroupCondition`、`job_updater.go` | 聚合具体 xPU reason 并回写 PodGroup status |
| Plugin | `pkg/scheduler/plugins/xpu-topology-aware/`（建议） | `New/Name/OnSessionOpen/OnSessionClose`；JobValid、Predicate、BatchNodeOrder、Session overlay；group planner 在 allocate 私有集成 |
| Provider | `pkg/scheduler/topology/provider/`（建议） | identity contract、selected DeviceKey 消费/确认、topology facts |

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
16. metrics 和日志可丢失并重建，不是恢复事实源。
17. Alpha 不把 Pod eviction、FutureIdle、列表缺项或 observation 缺席解释成 external Released。
18. Kubernetes per-Pod Bind 非原子，设计不声称可以原子 unbind。
19. 非空 xPU policy 只有在 `XPUTopologyAwareScheduling` gate 与 `xpu-topology-aware` plugin 同时有效时才可调度；任何失配都 fail closed。
20. plugin `New/OnSessionOpen/OnSessionClose` 是 per-Session 生命周期；Provider/cache 属于 process manager；Alpha 不持有外部 allocation owner。
21. xPU plugin 不注册竞争性的 HyperNode gradient，也不复制 gang `JobReadyFn`；group plan 只能缩小已有候选集合。

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
~~~

本文不把 V4 设计、Mock 验证、社区开放 PR 或文档中的 proposed API 写成 Volcano 当前已实现能力。
