# Volcano 通用 xPU 拓扑感知调度 V4：M1/M2 基础能力与 Advisory MVP 开发计划

> 上位设计：[V4 收敛设计](./99-generic-xpu-topology-aware-design-zh-v4.md)。
>
> 总体计划：[V4 开发计划](./100-generic-xpu-topology-aware-development-plan-zh-v4.md)。
>
> 冻结合同：[XPU-00 合同冻结记录](./101-generic-xpu-topology-aware-contract-review-zh-v4.md)。
>
> Provider 探针结论：[XPU-01 L1 分层证据报告](./103-generic-xpu-topology-aware-xpu-01-l1-evidence-report-zh-v4.md)。
>
> 状态：**待实施计划**。本文创建开发边界和验收门槛，不表示 XPU-02～07 已实现、已测试或已被社区接受。
>
> 源码复核日期：2026-09-20；本地 HEAD：`0a53d9eee990`。

## 1. 本阶段结论

M1/M2 可以开始实施，但必须按 **基础数据闭环先于调度行为、soft 先于 hard、所有未执行的 hard 路径 fail closed** 推进。

XPU-01 已经证明：stock `nvidia.com/gpu` + NVIDIA Device Plugin 能提供库存和 kubelet 最终 DeviceID，不能保证 Volcano
scheduler-selected UUID。M1/M2 因此只交付 topology facts、canonical policy、paired snapshot 和 soft advisory；任何 `hard`
policy 在 assignment 写入、Provider exact-key 确认和运行时对账完成前都保持 Pending，返回
`XPUAssignmentNotEnforceable`。现有 Volcano vGPU/HAMi exact UUID 案例是独立 Adapter 证据，不进入本阶段 workload API。

### 1.1 里程碑放行条件

| 里程碑 | 本阶段交付 | 放行条件 | 尚不能声明 |
| --- | --- | --- | --- |
| M1 数据基础 | 默认关闭的 gate/plugin、direct PodGroup typed API、固定 catalog、Annotation/Mock Provider、NodeUID-safe canonical model、immutable paired snapshot | API/生成链、catalog/Provider、Node replacement、snapshot/race 测试通过；非空 policy 在 activation 无效时 fail closed | 可用的 topology 调度功能、scheduler-selected DeviceID 执行 |
| M2 Advisory MVP | 只基于有效 topology facts 的 soft preference、deterministic Compact、Group soft Session overlay、结构化 reason | 无 policy 行为不变；soft 缺数据只失去 preference；hard 无 Bind；可复现演示通过 | hard topology、Pod assignment、跨 Session anchor、exact UUID、外部设备生命周期 |

### 1.2 本阶段能力矩阵

| 能力 | M1 | M2 |
| --- | --- | --- |
| direct `PodGroup.spec.deviceTopology` | 存储、default、validate、canonicalize | scheduler compiler 消费 |
| `SubGroupPolicySpec.deviceTopology` | 存储、validate、canonicalize | soft 作用单元编译 |
| Node-local Domain | Provider/Normalizer/Snapshot | soft score |
| 显式 Fabric | single-owner complete declaration、Snapshot | soft score；不从 HyperNode 推导 |
| `soft` | 不产生调度行为 | 可解释 preference，不缩小候选集 |
| `hard` | 识别并生成明确 reason | 一律 fail closed，不进入 Bind |
| scheduler-selected DeviceKey | canonical 类型和 Provider capability 形状 | 不执行、不持久化 |
| `volcano.sh/xpu-assignment` | 仅保留合同常量/解析边界 | 不写 Pod；由 XPU-11/13 交付 |
| vGPU/MIG/DRA/multi-container | 不支持 | 不支持 |

M2 的 soft score 只表达 topology membership、health 和结构容量形成的偏好，并与现有 Node aggregate resource fit 组合。M2 没有
per-device allocation owner，也不知道 stock kubelet 已把哪些 UUID 分配给其他 Pod，因此不能声明某个 Domain 当前存在精确空闲
DeviceKeys，更不能把结构上的 exact fit 写成运行时 exact allocation。

## 2. 当前源码基线

以下是本计划引用的当前实现事实，不是拟议接口：

| 区域 | 当前事实 | 对 M1/M2 的约束 |
| --- | --- | --- |
| Feature gate | `pkg/features/volcano_features.go` 尚无 `XPUTopologyAwareScheduling`；scheduler 与 webhook-manager 已复用 `DefaultMutableFeatureGate` 参数 plumbing | 新 gate 默认 `false`；两个进程使用同名 gate，不新增第二套开关系统 |
| scheduler 配置 | `pkg/scheduler/scheduler.go:loadSchedulerConf` 支持文件 reload；非法配置保留上一份配置 | catalog/provider identity 的 Alpha 变更要求进程重启；不能把 scheduler ConfigMap reload 当成 catalog 热更新 |
| plugin builder | `framework.OpenSession` 对每个 Session 调用 `PluginBuilder(Arguments)`，builder 无 error 返回 | `New` 只解析参数并构造 Session plugin；不得启动 Provider、worker 或 topology cache |
| plugin 参数 | `framework.Arguments.GetString/GetBool/GetInt/GetFloat64` 采用默认值、warning 和回退语义 | 不新增 generic validator registry；无法安全运行的 Provider 配置进入 activation fail-closed 状态 |
| PodGroup API | 当前 served API 是 `scheduling.volcano.sh/v1beta1`，`PodGroupSpec`/`SubGroupPolicySpec` 尚无 `DeviceTopology` | 在现有 v1beta1 增加 typed field，补齐 conversion/deepcopy/OpenAPI/apply-configuration/CRD 生成物；不新建平行 CRD |
| PodGroup webhook | `pkg/webhooks/admission/podgroups/validate` 目前只注册 CREATE | XPU-04 必须显式增加 UPDATE；只改校验函数而不扩 webhook operation 不算完成 |
| JobInfo | `JobInfo.SetPodGroup` 只在初次设置或 `SubGroupPolicy` 变化时重建 SubJobs | 顶层/SubGroup device policy 改变时必须失效 compiled view；不能继续使用旧 SubJob topology |
| cache snapshot | `SchedulerCache.Snapshot()` 在主锁下 clone Nodes/Jobs/Queues，`ClusterInfo` 尚无 DeviceTopology | topology pointer 必须在同一主锁观察点装入 `ClusterInfo`；plugin 不得二次 snapshot |
| Session | `openSession` 从一个 `cache.Snapshot()` 填充 Session 字段 | 新增只读 `DeviceTopology` view；每个 Session 固定使用同一 immutable pointer |
| Node event | Node 事件经 queue 到 `AddOrUpdateNode/RemoveNode`，当前主要按 NodeName 更新 cache | XPU-06 以 `NodeName + NodeUID` 识别 Node incarnation；同 UID 的标签/污点等元数据更新不使 topology 失效，UID 替换或删除才先使旧 facts 退出候选，再让新 UID 从 Pending 开始 |
| score | `Session.BatchNodeOrderFn` 将各启用 plugin 的 node score 相加 | xPU score 必须有界、确定、可组合；不能覆盖其他 plugin，也不能用 score 实现 hard filter |
| xPU 实现 | 当前无 V4 gate、plugin、catalog、Provider、snapshot、compiler 或 soft scorer | 每个 PR 只能声明自己新增的能力，文档/fixtures 不算实现 |

## 3. M1/M2 的固定实现边界

### 3.1 Activation 矩阵

M1 使用一个默认关闭的进程 feature gate `XPUTopologyAwareScheduling` 和一个 scheduler plugin
`xpu-topology-aware`。gate 让代码路径可用，plugin 让本次 scheduler 配置启用该能力；两者不互相替代。

| scheduler gate | plugin 配置 | 无 policy workload | 非空 xPU policy |
| --- | --- | --- | --- |
| off | absent | 完全沿用现有行为 | `XPUTopologyFeatureDisabled`，不得进入普通分配路径 |
| off | present | 完全沿用现有行为；记录 activation mismatch | fail closed |
| on | absent | 完全沿用现有行为 | `XPUTopologyPluginDisabled`，fail closed |
| on | present，catalog/provider 无效 | 完全沿用现有行为 | 使用具体 readiness reason，fail closed |
| on | present，M2 Ready | 完全沿用现有行为 | soft 可打分；hard 为 `XPUAssignmentNotEnforceable` |

必须在 plugin callback 之外保留一个 core final guard。否则 gate 关闭或 plugin 未配置时根本不会注册 callback，存量非空 policy
会被当成普通 workload。M2 的 guard 至少覆盖当前能产生新 Allocate/Bind 的 `allocate` 和 `backfill`；nomination 属于 allocate
内部路径。XPU-13/14 后续再扩展为包含 assignment final validation 的 Alpha guard。

webhook-manager 使用同名 gate。admission gate 关闭时不得接受新的非空 xPU policy；scheduler 即使与 admission 配置失配，仍以
自身 core guard 保护存量对象。M1/M2 不增加跨进程 activation digest，也不实现自动 drain。

### 3.2 进程与 Session 生命周期

唯一生命周期划分如下：

```text
Scheduler process
  -> process-scoped TopologyManager（最多启动一次）
       -> fixed Catalog
       -> Annotation Provider / test-only Mock Provider
       -> Normalizer
       -> immutable topology publisher

SchedulerCache
  -> Node identity coordination
  -> topology live state / published pointer
  -> ClusterInfo + DeviceTopology paired Snapshot

framework.OpenSession
  -> per-Session xpuTopologyAwarePlugin
  -> read-only topology view
  -> compiled policy / soft score cache / Group soft overlay
  -> Session close 后全部丢弃
```

约束：

- 不使用 package-global manager 或由 `New(Arguments)` 创建 singleton；
- `OnSessionClose` 不停止 Provider/cache；
- Provider parse、JSON decode、membership closure、外部调用和持久 I/O 均在 scheduler cache 主锁之外；
- Node UID 失效和 paired pointer publish 必须与 `SchedulerCache` 的 Node 观察点协调；
- M1/M2 不持有 external allocation owner，不扣减第二份设备容量，不启动 release/reconcile worker。

### 3.3 推荐 package 边界

为避免 `pkg/scheduler/api` 与新 topology package 形成 import cycle，建议按以下边界落地：

| 目录/文件 | 所有权 |
| --- | --- |
| `staging/src/volcano.sh/apis/pkg/apis/scheduling/{types.go,v1beta1/types.go}` | Public typed policy |
| `pkg/scheduler/api/device_topology.go` | scheduler canonical keys、Domain/Fabric、immutable snapshot、readiness/reason value types |
| `pkg/scheduler/topology/catalog.go` | 固定 catalog strict load/validate |
| `pkg/scheduler/topology/normalize.go` | source facts 到 canonical objects/indexes 的纯逻辑 |
| `pkg/scheduler/topology/provider/` | Provider contract、Annotation Provider；Mock 只在测试/Fake 中 |
| `pkg/scheduler/cache/topology_cache.go` | live state、NodeUID invalidation、immutable pointer publish |
| `pkg/scheduler/plugins/xpu-topology-aware/` | policy compiler、soft feasibility/score、Session overlay |

上述文件名是建议落点，不冻结成跨组件 API。跨 package 的固定依赖方向为：

```text
staging scheduling API
        ↓
pkg/scheduler/api
        ↓
pkg/scheduler/topology and pkg/scheduler/cache
        ↓
xpu-topology-aware plugin
```

### 3.4 固定 catalog 的首个实现选择

XPU-03 开始编码前需要评审冻结一个 Alpha 载体。推荐首版只支持：

- scheduler Pod 挂载的一份只读 catalog 文件；建议由现有 scheduler ConfigMap 增加独立 key；
- plugin argument 只传文件路径，不传第二份 inline class 定义；
- 进程首次有效 activation 时 strict load 一次，运行期间不切换；
- 内容改变需要 scheduler 重启；配置 reload 不改变已加载 catalog；
- 缺文件、重复 key、未知字段、重复 `DomainClassKey`、resource/scope/name 非法时 catalog 整体 NotReady；
- controller/webhook 不读取 catalog，只做 Public 字段形状与 canonical policy 校验；unknown class 由 scheduler 编译期 fail closed。

如果 reviewer 选择另一静态载体，只能替换“文件如何交付”，不能改变单一、只读、scheduler 权威和运行期间不可变四项合同。

## 4. 依赖与实施顺序

```text
XPU-02 activation/lifecycle ───────────────┐
                                           ├─> XPU-05 Provider/Normalizer ─> XPU-06 paired snapshot ─┐
XPU-03 API + catalog + canonical model ────┤                                                     ├─> XPU-07 Advisory MVP
                                           └─> XPU-04 admission/JobInfo/status ────────────────────┘

XPU-01B native exact bridge：并行研究，不阻塞 M1/M2；它只决定后续 M3 hard 是否可放行。
```

可并行边界：

- XPU-02 的 gate/plugin skeleton 可与 XPU-03 Public API 同时开始；
- XPU-03 的 canonical key/catalog interface 冻结后，XPU-04 与 XPU-05 可并行；
- XPU-06 必须等待 XPU-05 的 update/readiness 合同稳定；
- XPU-07 可以提前写纯 compiler/score fixtures，但只有 XPU-04/06 集成完成后才能称 M2。

## 5. 工作包

### 5.1 XPU-02：activation、plugin skeleton 与进程生命周期

**目标**：让代码可安装但默认关闭，并建立一个不会随 Session 重复启动的 topology 生命周期。

**主要修改**：

- `pkg/features/volcano_features.go`：注册 `XPUTopologyAwareScheduling`，Alpha，默认 `false`；
- `pkg/scheduler/plugins/factory.go`：注册 `xpu-topology-aware` builder；
- `pkg/scheduler/plugins/xpu-topology-aware/`：实现最小 `New/Name/OnSessionOpen/OnSessionClose`；
- `pkg/scheduler/{scheduler.go,util.go}`：增加 scheduler 专用 activation guard，不新增 generic validator registry；
- `pkg/scheduler/cache/interface.go` 及 Fake：增加最小 manager/store 连接面；具体构造方式必须显式依赖注入，不使用全局变量；
- Helm values/templates：复用现有 scheduler/admission feature-gate 参数，增加完整 scheduler config 示例，不修改默认配置。

**参数处理**：

- `New` 先建立默认值，再使用现有 `Arguments.Get*`；
- 类型/格式/范围错误记录 warning，保留安全默认或把 Provider 配置标记为不可用；
- gate/plugin/catalog/provider 组合由 activation guard 判断，不塞进 `New`；
- Alpha 不支持通过 scheduler config reload 启用另一 catalog/provider identity；检测到语义变化时保持 fail closed 并提示重启。

**测试**：

- gate/plugin 四组合；
- scheduler 与 admission gate 失配；
- 连续打开/关闭多个 Session，manager 只启动一次、Session state 每次重建；
- invalid arguments 不 panic、不启动第二个 manager；
- gate/plugin 都关闭且没有 policy 时，现有 scheduler config 和普通 workload 行为不变。

**完成条件**：代码默认不可见；manager 生命周期可计数验证；非空 policy 不依赖 plugin callback 才能被保护。

### 5.2 XPU-03：Public API、固定 catalog 与 canonical model

**目标**：冻结用户 intent 与 scheduler canonical identity，使 Provider/cache/plugin 使用一套 key 和排序规则。

**Public API 修改**：

- 在当前 `scheduling.volcano.sh/v1beta1` 的 `PodGroupSpec` 和 `SubGroupPolicySpec` 增加
  `DeviceTopology *DeviceTopologySpec`；
- 增加 `DeviceTopologyMode`、`DeviceTopologyApplyTo`、`DeviceTopologyDomainScope`、
  `DeviceTopologyDomainSelector`、`DeviceTopologyPolicy`、`DeviceTopologySpec`；
- `mode` 默认 `hard`，`applyTo` 默认 `Pod`；`scope` 只接受 `Node/Fabric`；`domainClass` 必填；
- `podSelector` 只允许 PodGroup 级 `applyTo=Pod`；Group policy 和 SubGroup policy 不接受该 selector；
- Public schema 不增加 `allocationStrategy`、具体 Domain/Fabric/Device ID、Provider payload、snapshot revision；
- VCJob/partition、PodTemplate annotation 和 bare Pod ergonomic source 延期。

添加字段前先审计现有 protobuf tag；当前 external/internal 类型 tag 编号并不完全一致，不能机械假定“最后字段 +1”。生成文件只通过
codegen/manifests 更新，不手工维护一套旁路定义。

**canonical model**：

- `DomainClassKey = ResourceName + Scope + Name`；
- `DeviceKey` 和 `LocalDomainKey` 包含 Owner NodeUID；`FabricKey` 属于受控 Provider/resource identity；
- canonical policy 完成 default、selector normalization、stable sort、exact dedup 和 conflict detection；
- `PolicyFingerprint` 只覆盖默认化后的用户 intent，不包含 resourceVersion、catalog 内容或 runtime facts；
- 相同 class 名在不同 resource/scope 下不冲突；未知 class 不从其他 scope/resource 猜测；
- catalog strict load、唯一性和稳定排序与 canonical model 同 PR 交付。

**生成物和验证**：

- internal/v1beta1 conversion、deepcopy、OpenAPI、apply-configuration；
- `config/crd/volcano/{bases,v1beta1}`、Helm CRD 镜像和 PodGroup artifact；
- 重复运行 codegen/manifests 无差异；
- 使用真实 API server/kind 验证 invalid enum/unknown field 不会先 prune 再按默认值接受；纯 Go parser 单测不能替代该证据。

**完成条件**：direct PodGroup typed spec 可 round-trip；canonical equality 与 fingerprint 稳定；未配置字段的旧对象兼容。

### 5.3 XPU-04：admission、更新规则、JobInfo 与可解释状态

**目标**：让 direct PodGroup 成为唯一 authoring path，并保证 invalid/冲突 policy 不会被 scheduler 当成空 policy。

**主要修改**：

- `pkg/webhooks/admission/podgroups/validate`：从 CREATE 扩展到 UPDATE，解码 old/new object；
- admission raw-object strict decoding 覆盖未知字段、重复 JSON key 和非法类型；不能只依赖已经解码后的 Go struct；
- webhook-manager 不为 policy update 增加 Pod lister、活动索引或第二权威源；admission 只校验新 persisted typed spec 的 API 形状与同 spec 冲突；
- semantic update（包括删除）允许提交并只影响未来未调度 Pod；已 Bound/Running Pod 不重调度，也不因 policy 变更创建 recovery/anchor 状态；
- `JobInfo.SetPodGroup/UnsetPodGroup/Clone` 保存 canonical policy/fingerprint，并在顶层或 SubGroup policy 变化时重建/失效对应 SubJob view；
- scheduler compiler 重新校验被 policy 命中的 Task 请求形状；M1/M2 只接受单普通 Container、正整数扩展资源且 request=limit，
  multi-container/init-container、共享/小数资源明确返回 unsupported；
- 复用 PodGroup Condition，保留 authoring conflict blocker 优先级；动态 scheduler reason 不能覆盖未解决的
  `XPUTopologyPolicyConflict`；
- direct PodGroup 之外的来源不创建 topology policy。

M1/M2 不要求 controller 读取 scheduler catalog。admission 只校验 API 形状和同一 spec 内冲突；class 是否存在由 scheduler 使用固定
catalog 判断。

**测试**：

- CREATE/UPDATE、canonical no-op、semantic mutation/删除可持久化并更新 future-Pod view；
- 顶层 policy 变化触发 compiled view/SubJob 失效；
- blocker status conflict retry，不覆盖其他 condition；
- canonical spec 为空但 blocker=True 时仍 fail closed；
- VCJob、自动 PodGroup 和 workload annotation 不意外产生 policy。

**完成条件**：无效输入在 API/admission 边界拒绝；scheduler 始终只读取已持久化 typed spec。

### 5.4 XPU-05：Annotation Provider、Mock 与 Normalizer

**目标**：把 source-specific facts 转成可验证、可替换、可失效的 canonical topology，不承担调度选择。

**Provider contract**：

- 每个 `ProviderNodeKey{ProviderID, Namespace, ResourceName, NodeUID}` 独立维护 `Pending/Synced`；
- `ReplaceFacts` 是完整替换；`ClearFacts` 是已检查后明确无 inventory；
- invalid payload 不能完成初次同步，不能覆盖最后有效 facts，也不能刷新 freshness；
- source generation 单调；同 generation 只允许 content-identical heartbeat；
- Node `resourceVersion` 只作为 Provider 观测元数据和诊断关联，不是 Node identity，也不因同 UID 的元数据更新而使 topology 失效；facts freshness 由 `SourceGeneration + ContentFingerprint` 控制；
- Provider capability 必须是固定 catalog 的显式子集，不允许 wildcard；
- Provider 只校验/消费 selected DeviceKeys，不能选择或替换 scheduler plan。

**Annotation Provider**：

- 冻结一个 scheduler 管理的 Node annotation key 和 strict JSON schema；建议 key 为
  `volcano.sh/xpu-topology`，最终命名在 XPU-05 首个 PR review 中一次确定；
- workload ServiceAccount 与 scheduler ServiceAccount 不具备该 annotation 的写权限；安装文档明确受信 publisher 和 admission 保护；
- Node-local Domain 显式列成员；Fabric 采用 single-owner complete declaration；
- HyperNode、Node label、普通 IB/RoCE 连通或 link score 不能推导 hard Fabric/Domain。

**Normalizer**：

- 校验 resource/scope/class、NodeUID、重复 ID、membership closure、环、health 和稳定排序；
- 同 source DeviceID 在不同 NodeUID 下合法，同一 NodeUID 下重复非法；
- Fabric member 必须解析到当前 NodeUID/source generation/local Domain；owner/member 任一变化使旧 Fabric 退出候选；
- 输出 immutable canonical objects 和最小 indexes，不直接修改已发布 snapshot。

Mock Provider 只用于单元/集成 fixture，不作为生产 runtime 或 exact-ID 证据。本阶段不开发通用硬件发现 DaemonSet。

**完成条件**：XPU-00 的 identity/catalog/Fabric 正反例全部有结构化 reason；Provider 不含 chooser/allocator。

### 5.5 XPU-06：live cache、Node identity 与 paired snapshot

**目标**：让每个 Session 在一次 cache observation 中获得 Nodes/Jobs/Queues 与同代 topology view。

**主要修改**：

- `pkg/scheduler/api/cluster_info.go` 增加 `DeviceTopology *DeviceTopologySnapshot`；
- `framework.Session` 保存同一只读 pointer；
- `SchedulerCache` 持有 topology live state 和 atomic published pointer；
- Node Add/Update/Delete 在当前 Node cache 临界区内按 `NodeName + NodeUID` 完成轻量 identity invalidation；同 UID 的普通元数据更新保留 topology，UID 替换/删除才 retire facts；parse/normalize/closure 在锁外；
- publish 前重新核对 NodeName/UID 和 source generation，旧结果直接丢弃；ResourceVersion 可随 Provider update 携带用于诊断，但不是 publish fence；
- `SchedulerCache.Snapshot()` 在主锁下 atomic-load 已发布 pointer，不做 parse、RPC 或全量 topology clone；
- 所有发布的 map/slice/object 不可变；Provider update 构建新对象后交换 pointer；
- 新 NodeUID 初始为 Pending，不能继承旧 UID facts/readiness；旧 Session pointer 不随新 publish 改变。

允许 Session 看到的组合只有：

```text
旧 Node + 旧 UID + 旧有效 topology view
或
新 Node + 新 UID + Pending topology view
```

禁止看到新 Node 与旧 UID topology 的混合。Node aggregate resource count 仍由现有 `NodeInfo` 负责，topology cache 不做第二次普通
资源扣减。

**测试**：

- 同名 Node UID replacement、迟到旧 update/clear、source delete、Fabric owner/member replacement；
- 同 UID 的 Node label/taint 等 metadata-only update 保留已同步 topology；
- publish 后修改 source object 不影响旧 snapshot；
- 并发 Provider update 与 `SchedulerCache.Snapshot()` race；
- 测试 hook 证明 parse/Provider callback 不在主锁内；
- 旧 Session view 固定，新 Session 获取新 revision。

**完成条件**：paired snapshot/race 全部通过；plugin 无二次 topology snapshot API。

### 5.6 XPU-07：policy compiler 与 Advisory soft score

**目标**：交付第一个用户可验证的 topology 行为，同时保持所有 hard 路径不可执行。

**M2 callback 范围**：

- `JobValidFn`：读取 direct PodGroup canonical spec，区分 unknown/unsupported/Pending/stale/not-enforceable；
- `BatchNodeOrderFn`：只对 soft policy产生有界、可组合、确定性的附加分；
- Session EventHandler：只维护 Group soft 的 tentative placement overlay；不产生 Pod assignment，不成为跨 Session anchor；
- M2 不依靠 Predicate 执行 hard policy；hard 由 compiler + core final guard 阻断。M3 再接入 hard Predicate/group planner。

**soft 规则**：

- 只在普通 Predicate 已提供的候选节点上评分，不能重新引入节点；
- topology facts 缺失、Pending、stale 或 Provider 不可用时，该 policy 对该候选贡献 0 分，而不是拒绝节点；
- 只使用 class/resource/scope 匹配且显式 membership 完整的 Domain/Fabric；
- M2 只根据 membership/health 和结构容量评分；已有普通 Node 资源 fit 仍由 Predicate 负责，不从结构容量推断当前精确空闲 UUID；
- deterministic Compact 使用稳定 key、结构 exact fit、最小可容纳 Domain、较少 Node/Domain 和最小结构碎片作为顺序；
  “结构 exact fit”仅表示 Mock/无占用 fixture 中 Domain 成员数与请求相等，不是 runtime exact allocation；
- score 相同按 canonical key 决胜；输入 map/fixture 顺序变化不得改变结果；
- 未配置 policy 时返回空/零 contribution，不改变其他 plugin 的排序；
- Group soft 可以偏好本 Session 已选实例，但 Session 结束即丢弃，不从 soft history恢复 hard anchor。

**hard 规则**：

- M2 对任意 hard policy 返回 `XPUAssignmentNotEnforceable` 或更早的 authoring/readiness reason；
- `allocate`、`backfill` 的 final guard 都必须证明没有相关 Task 进入 Bind；
- 不生成 DeviceKeys、不写 `volcano.sh/xpu-assignment`、不调用 stock/vGPU Device Plugin。

**可解释状态**：

- authoring error：`XPUTopologyPolicyInvalid` / `XPUTopologyDomainClassUnknown`；
- capability/readiness：`XPUTopologyDomainClassUnsupported`、`XPUTopologyDataNotReady`、`XPUTopologyStale`；
- M2 hard：`XPUAssignmentNotEnforceable`；
- 恢复：`XPUTopologyResolved`。

具体 message 可包含 resource/scope/domainClass、请求数量、最大可用 Domain count 和 Provider 状态；metrics/event label 不包含 raw
DeviceID、PodUID 或高基数 Domain/Fabric ID。

**完成条件**：M2 演示和 T01～T11 对应场景通过；没有任何 Pod assignment 或 external allocation side effect。

## 6. 建议 PR 拆分

| 顺序 | PR | 覆盖工作包 | 主要产物 | 合入门槛 |
| --- | --- | --- | --- | --- |
| 1 | activation skeleton | XPU-02 | gate、builder、manager ownership、activation matrix tests | 默认关闭；无 policy 回归不变；无全局 singleton |
| 2 | Public API and generated artifacts | XPU-03A | typed fields、types、conversion/deepcopy/OpenAPI/apply/CRD | codegen/manifests clean；API server schema 证据 |
| 3 | canonical model and catalog | XPU-03B | canonical keys、policy normalizer/fingerprint、fixed catalog | fixture 稳定；catalog strict fail closed |
| 4 | PodGroup authoring and status | XPU-04 | CREATE/UPDATE webhook、mutation、JobInfo invalidation、reason ownership | direct PodGroup 唯一路径；blocker 不丢失 |
| 5 | Provider and Normalizer | XPU-05 | Annotation/Mock contract、canonical facts、Fabric validation | identity/generation/freshness/Fabric 反例通过 |
| 6 | paired topology snapshot | XPU-06 | live cache、NodeUID invalidation、ClusterInfo/Session view | race/Node replacement/old Session tests |
| 7 | Advisory compiler and scorer | XPU-07A | compiler、soft feasibility/Compact、BatchNodeOrder、overlay | soft 不过滤；hard no Bind；deterministic |
| 8 | M2 integration and install evidence | XPU-07B | Helm example、Mock facts、direct PodGroup demo、capability note | 从安装到 reason 的可重放证据 |

每个 PR 只引入完成本 PR 测试所需的接口。不得先提交空的通用 framework hook、外部 allocation owner、ledger 或 batch binder，等待
未来代码“可能使用”。

## 7. 估算与人员安排

沿用总体计划的工作包估算，XPU-02～07 剩余实现约 **32～48 工程人日**：

| 工作包 | 人日 | 建议主责 |
| --- | --- | --- |
| XPU-02 | 4～6 | scheduler/framework |
| XPU-03 | 6～9 | API + scheduler model |
| XPU-04 | 6～9 | admission/controller + scheduler API |
| XPU-05 | 6～9 | Provider/runtime |
| XPU-06 | 6～9 | cache/framework |
| XPU-07 | 4～6 | scheduler/plugin |

两名工程师可按以下方式并行：

- A：XPU-03 API → XPU-04 admission/JobInfo → XPU-07 integration；
- B：XPU-02 lifecycle → XPU-05 Provider → XPU-06 cache/snapshot → XPU-07 scorer。

`pkg/scheduler/cache/cache.go`、`framework/session.go` 和 Public API 的每个阶段只设一个主责，避免两个 PR 同时重写相同核心文件。
以上是实现与一次正常评审修改的估算，不含社区等待、真实硬件、XPU-01B、规模测试和发布演练。

## 8. 验收矩阵

### 8.1 M1 必须通过

| ID | 场景 | 断言 |
| --- | --- | --- |
| M1-01 | gate/plugin 四组合 | 非空 policy 均不会在 invalid activation 下进入普通路径 |
| M1-02 | direct PodGroup API round-trip | default/canonical/生成链一致 |
| M1-03 | unknown/conflicting policy | API 或 scheduler authoring reason 明确；不按空 policy 继续 |
| M1-04 | fixed catalog | 缺失、重复、非法、resource/scope mismatch 整版 NotReady |
| M1-05 | Provider update | Replace/Clear/generation/freshness/invalid payload 语义正确 |
| M1-06 | Node replacement / metadata update | 同名新 UID 为 Pending，旧 facts/assignment 不继承；同 UID 的 Node metadata update 不清空已同步 facts |
| M1-07 | Fabric authority | 单 owner 完整声明；member/owner 变化失效；HyperNode 不推导 Fabric |
| M1-08 | paired snapshot | 不出现新 Node/旧 UID topology；旧 Session immutable |
| M1-09 | policy update | canonical no-op、semantic mutation 和删除均可持久化；仅未来未调度 Pod 使用新 policy |

### 8.2 M2 必须通过

| ID | 场景 | 断言 |
| --- | --- | --- |
| M2-01 | 无 policy | 候选、分数和最终选择与 plugin disabled 基线一致 |
| M2-02 | soft + 有效 local Domain | 可复现 preference，不能越过普通 Predicate 候选 |
| M2-03 | soft + 显式 Fabric | 只使用 Provider 明确 membership；普通网络/HyperNode 不产生分数 |
| M2-04 | soft + facts Pending/stale/deleted | 候选仍可调度，只失去 xPU preference |
| M2-05 | deterministic | 输入乱序、重复 heartbeat、多次 Session 得到相同相对 score/tie-break |
| M2-06 | Group soft | 同 Session overlay 产生偏好；Session restart 不伪造 hard anchor |
| M2-07 | hard | `allocate` 与 `backfill` 都无 Bind，reason=`XPUAssignmentNotEnforceable` 或更具体 blocker |
| M2-08 | stock Device Plugin | 不把 Pod Running、资源数量或 kubelet 最终 ID 写成 scheduler-selected UUID 成功 |

### 8.3 M2 演示步骤

```text
1. scheduler/admission gate 开启，scheduler 配置显式加入 xpu-topology-aware
2. 挂载固定 catalog，启动 Annotation 或 Mock Provider facts
3. 提交 direct PodGroup typed soft policy
4. 观察普通 Predicate 候选不变、xPU preference 可解释且结果稳定
5. 删除或过期 facts，Pod 仍可调度但失去 preference
6. 将同一 policy 改为 hard，确认 Pod 保持 Pending、无 Bind、reason 明确
7. 关闭 gate/plugin 后运行无 policy 回归，结果与基线一致
```

## 9. 验证入口

API/生成链修改后使用现有入口：

```bash
cd staging/src/volcano.sh/apis
bash hack/update-codegen.sh
bash hack/verify-codegen.sh

cd /path/to/volcano
make generate-code
make manifests
make verify
make verify-generated-yaml
```

实现对应 package 后，聚焦验证至少覆盖：

```bash
go test ./pkg/scheduler/api ./pkg/scheduler/topology/... ./pkg/scheduler/plugins/xpu-topology-aware/...
go test ./pkg/scheduler/cache ./pkg/scheduler/framework ./pkg/scheduler/actions/allocate ./pkg/scheduler/actions/backfill
go test -race ./pkg/scheduler/cache ./pkg/scheduler/topology/... ./pkg/scheduler/plugins/xpu-topology-aware/...
go test ./pkg/webhooks/admission/podgroups/... ./pkg/controllers/podgroup ./pkg/scheduler/api
```

这些新 package/测试命令是实现后的目标入口，不表示当前已经存在。schema pruning、feature-gate 安装和 M2 演示必须在真实 API
server/kind 上补证据；静态 CRD diff、Fake client 或 Markdown 检查不能替代运行验收。

## 10. 首两周建议

### 第 1 周：建立不会返工的边界

- 冻结 catalog 文件载体、annotation key、manager/cache dependency injection seam；
- 完成 XPU-02 activation skeleton 与四组合测试；
- 完成 XPU-03A Public API 草案、protobuf tag 审计和首次 codegen/CRD diff；
- 将 XPU-00 `canonical-equivalent-order-defaults`、`unknown-class-authoring-error` 映射为可运行测试；
- 输出第一份 capability note：M1 未产生调度行为、所有 hard 保持 Pending。

### 第 2 周：完成纯逻辑后再接 cache

- 完成 canonical model、fingerprint 和 fixed catalog；
- 开始 XPU-04 CREATE/UPDATE、JobInfo invalidation；
- 完成 XPU-05 Provider update/Normalizer 的纯函数与 Fake tests；
- 评审 XPU-06 Node replacement/publish 时序，先用 race fixture 证明目标状态，再修改 `cache.go`；
- 不在 paired snapshot 完成前发布 soft scoring PR。

## 11. Stop gate 与风险处理

| 风险/发现 | 必须动作 |
| --- | --- |
| API 字段在 server-side prune 后静默消失 | 停止 XPU-04/07 集成，先修 schema/生成链；不能靠客户端 Strict 掩盖 |
| plugin 缺失时非空 policy 仍可由 allocate/backfill Bind | M2 不放行；补 core final guard |
| Node 同名换 UID 出现新 Node/旧 topology | M1 不放行；重做 invalidation/publish 协调 |
| Provider parse 位于 scheduler cache 主锁内 | M1 不放行；移出锁并增加调用计数/并发测试 |
| soft 缺数据会删除候选 | M2 不放行；改为 0 preference 和诊断 reason |
| score 输入顺序改变最终选择 | M2 不放行；补 stable sort/tie-break |
| 需要修改 stock NVIDIA Device Plugin 才能执行 hard | 转入 XPU-01B；M1/M2 不扩 scope，hard 继续 Pending |
| vGPU/HAMi 私有 annotation 被当成 generic assignment | 拒绝合入；它只能是独立 Adapter 证据 |
| 实现需要 transaction ledger/batch Bind/external owner | 停止并另立需求；不把独立能力塞入 M1/M2 |

## 12. 本阶段明确不做

- 不实现 scheduler-selected UUID 的 stock Device Plugin bridge；
- 不写 `volcano.sh/xpu-assignment`，不恢复 Pod-derived hard anchor；
- 不实现 hard Group planner、Statement operation 扩展或 BindContext assignment；
- 不实现 DRA、MIG、vGPU/shared geometry、multi-container/init-container；
- 不实现外部 reservation/release/reconcile、active-active fencing、多 Pod Bind 原子性；
- 不实现 topology-aware preempt/reclaim victim selection；
- 不新建 catalog CRD、动态 catalog 更新、历史版本或第二个控制面；
- 不增加 VCJob/Deployment/StatefulSet/bare-Pod topology authoring source；
- 不把 nvml-mock、HAMi 或文档测试升级成真实 NVIDIA runtime/L2 证据。

## 13. 文档完成与代码完成的区分

本文完成后，只能说明 XPU-02～07 的实施边界、顺序和验收已经可评审。只有对应代码、生成物、单元/race/API-server/kind 证据
全部落地后，才可以分别标记 M1 或 M2 完成。M1 完成不等于用户可用调度功能；M2 完成也不等于 hard topology、exact UUID 或
Pod-derived Topology Alpha 完成。
