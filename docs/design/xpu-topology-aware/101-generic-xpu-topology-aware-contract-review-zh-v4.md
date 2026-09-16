# Volcano 通用 xPU 拓扑感知调度 V4 合同冻结记录（XPU-00）

> 基线：[V4 设计](./99-generic-xpu-topology-aware-design-zh-v4.md)与
> [V4 开发计划](./100-generic-xpu-topology-aware-development-plan-zh-v4.md)。
>
> 状态：**实现基线草案，待 maintainer/API/runtime 联合评审**。
> 本文冻结首个 Alpha 的实现输入，不表示上游已经接受，也不表示功能已经实现。
> 源码核对日期：2026-09-16；源码基线：`7604cc7d3`，当前文档分支 HEAD 仅增加设计/计划文档。

## 1. XPU-00 的完成边界

XPU-00 不新增 feature gate、CRD 字段、scheduler plugin 或 Adapter 实现。它只解决如果不先统一、会导致
XPU-02～15 跨组件返工的合同问题。

完成 XPU-00 需要同时具备：

1. D1～D8 每项都有一个明确的 Alpha 实现基线、不可缩减的底线和变更触发条件；
2. Public/canonical、activity/evidence、Statement exact commit、batch handoff 和 Adapter probe 有最小接口草案；
3. `allocate/backfill/preempt/reclaim/gang*/shuffle` 的 hard/soft 行为有支持矩阵，不存在未声明的 bind/release 旁路；
4. 跨系统失败顺序、唯一 owner 转移点和 `Allocated/Released/Unknown` 结果有反例；
5. 正反例 fixture 具有稳定 case ID，可被后续单元、API server、Fake Adapter 和 E2E 复用；
6. 尚需社区或真实 backend 证明的内容明确标为评审项，不伪装成已实现事实。

本文中的状态含义：

| 状态 | 含义 |
| --- | --- |
| `FrozenForAlpha` | 后续工作包可以按此实现；变更需要更新本文、受影响 fixture 和依赖工作包 |
| `ProbePending` | 合同已冻结，但 backend 是否满足合同必须由 XPU-01 提供运行证据 |
| `CommunityReview` | 推荐形状已给出，maintainer 可调整命名/落点，但不得削弱已列不变量 |
| `Deferred` | 不进入首个 Alpha，调用方必须拒绝而不是静默降级 |

## 2. 决策总表

| ID | Alpha 实现基线 | 状态 | owner 角色 | 首个消费者 |
| --- | --- | --- | --- | --- |
| D1 catalog 权威载体 | 一份固定内容的 immutable ConfigMap 保存 Alpha catalog；scheduler/controller/webhook/Provider 只读，运行期间不支持修改 | `FrozenForAlpha` | A | XPU-03/04/05 |
| D2 anchor/evidence 载体 | `EvidenceStore` 逻辑合同；首选由 exact Adapter 的 durable record 实现，PodGroup status 只保存 activity fence 和引用 | `ProbePending` | S/R | XPU-11/14/15 |
| D3 framework 扩展 | winning `Statement` 持有唯一 `ExactCommitCoordinator`；新增 error-returning exact commit 和不可拆分 batch，不建立第二个调度事务系统 | `CommunityReview` | S | XPU-09/12/13 |
| D4 首个真实 Adapter | 厂商目标冻结为 NVIDIA；XPU-01 用 `nvml-mock + NVIDIA Device Plugin + NVIDIA Adapter` 探测 Node-local、单 Container、整卡 exact-ID 路径 | `ProbePending` | R | XPU-01/15 |
| D5 API/schema | V4 `DeviceTopologySpec`；class 为 DNS-1123 label；严格 JSON；固定数量/大小上限；served schema 用 reject-only tombstone 拒绝旧字段 | `FrozenForAlpha` | A | XPU-03/04 |
| D6 authoring/activity | PodGroup status 中的 `ActivityFence` 与 spec resourceVersion/generation 做 CAS；先成功写 fence，才允许 final reserve | `FrozenForAlpha` | A/S | XPU-04/11/13 |
| D7 激活/action | scheduler/admission 同名 gate + scheduler plugin + 固定 catalog/owner readiness 共同决定 activation；不支持 exact 的 action 对 hard 明确阻断 | `FrozenForAlpha` | S | XPU-02/13/14 |
| D8 组合/所有权 | 一个 winning Statement 一个 commit context；其中可含多个 GroupRef/资源 owner，但每个 Task 只有一份最终 placement，全部约束取交集 | `FrozenForAlpha` | S/R | XPU-08～14 |

## 3. D1：catalog 权威载体

### 3.1 选择

Alpha 不新增 `ResourceTopologyDescriptor` CRD，也不实现 catalog 动态更新。采用一份固定内容的受控 ConfigMap：

```text
volcano-xpu-topology-catalog
  immutable: true
  data.catalog.json: 固定、严格的 JSON
```

部署规则固定为：

1. 安装时创建唯一的 `volcano-xpu-topology-catalog` ConfigMap；
2. scheduler、controller、webhook、Provider 读取同一份固定 catalog；
3. catalog 缺失、内容非法或不符合 Alpha 固定类别表时进入 `CatalogNotReady`；
4. 运行期间禁止修改 catalog；后续要增加或改变类别，另开设计和兼容性评审，不在本 Alpha 处理。

ConfigMap 是 Alpha 的固定交付形状。这里的 `ResourceTopologyDescriptor` 是 catalog 的内存/规范模型，不是一个需要在运行期间
维护版本的 CRD。

### 3.2 权限、保留和失败

- 只有安装/部署身份可创建初始 catalog；运行中的 scheduler/controller/webhook/Provider 只读；
- catalog ConfigMap 不允许原地 update；删除或缺失时 catalog reader 不 Ready；
- 固定类别表、resource/scope/class 匹配、JSON 严格性或大小校验失败时，catalog reader 不 Ready；
- controller/webhook 不可读时拒绝新建/更新带 xPU policy 的 workload；scheduler 只阻塞受影响的 xPU workload；
- catalog 不放进 `volcano-scheduler.conf`，避免 admission/controller 消费另一份事实。

### 3.3 为什么不是当前 HyperNode/Node annotation

catalog 表达管理员定义的稳定 class；HyperNode 描述网络层次，Node annotation 是 Provider observation。二者都不能同时作为
workload selector 的权威字典，否则同一个 `domainClass` 会被不同进程、不同 Session 或不同 Node 重解释。

## 4. D2：Group anchor 与 allocation evidence

### 4.1 选择

冻结一个存储无关的 `EvidenceStore` 合同。首个真实 Exact Adapter 必须提供或伴随一个 durable 实现；PodGroup status 不保存
完整 DeviceIDs/token，只保存 activity fence、record key/revision 和可解释状态摘要。

```go
// Contract draft only. Names and package placement remain subject to review.
type EvidenceStore interface {
    CompareAndCreateGroup(context.Context, GroupEvidenceRecord, string) (RecordRevision, error)
    CompareAndUpdateGroup(context.Context, GroupEvidenceRecord, RecordRevision) (RecordRevision, error)
    GetGroup(context.Context, TopologyGroupRef) (GroupEvidenceRecord, RecordRevision, error)
    ListUnfinished(context.Context, ResourceOwnerRef) ([]GroupEvidenceRecord, error)
}

type GroupEvidenceRecord struct {
    GroupRef                  TopologyGroupRef
    PolicyFingerprint         string
    GroupSelections           []TopologyDomainSelection
    Assignments               []TopologyTaskPlacement
    PlanID                    string
    PlanDigest                string
    ReservationIDs           []string
    SchedulerEpoch            string
    Phase                     EvidencePhase
}
```

`GroupRef + PolicyFingerprint` 是稳定 record key 的输入；`RecordRevision` 是 store 返回的 CAS revision，不能由 scheduler 自增猜测。
同一 Group 的多个 admission wave 更新同一记录；不同 policy fingerprint 不得覆盖活动记录。

### 4.2 不允许的替代

- Pod annotation、Node annotation、scheduler 内存或 log 不能作为 durable evidence；
- PodGroup Condition 只能表达用户可见状态，不能替代完整 assignment record；
- Adapter `Release()` 返回 nil、Pod NotFound、Recover 列表缺项或超时不能证明 Released；
- 如果 XPU-01 证明候选 backend 无法 CAS、枚举恢复或给出 Released，M4 阻塞；不得静默退化为“内存 ledger + hard”。

如果社区不接受 Adapter-backed store，需为独立受控存储另开 ADR，并重新评估 XPU-11/14/15；逻辑合同和测试不变。

## 5. D3：framework 与 bind batch

### 5.1 选择最小 Exact coordinator

Alpha 不向所有普通 `Statement.Commit()` 注入任意 participant，也不新建平行 Statement。只有包含 hard xPU 新 Allocate 的
winning Statement 走 error-returning exact 路径，并且只有一个 coordinator：

```go
// Contract draft only.
type StatementCommitView struct {
    Operations   []OperationView
    AdmissionSet []TaskRef
    Context      ExactCommitContext
}

type ExactCommitCoordinator interface {
    Prepare(context.Context, StatementCommitView) (*PreparedExactCommit, error)
    Accept(context.Context, *PreparedExactCommit) error
    Abort(context.Context, *PreparedExactCommit, error) ReconcileResult
}

func (s *Statement) CommitExact(
    ctx context.Context,
    coordinator ExactCommitCoordinator,
) error
```

接口名可以在 review 中调整，但形状必须满足：

- coordinator 附着在 winning Statement，而不是 plugin 自建事务；
- `AdmissionSet` 来自该 Statement 的全部新增 Allocate operations，包括已 Ready Group 的后续 wave；
- `CommitExact` 返回 error，失败前不清空 operations；
- dry-run `SaveOperations/RecoverOperations` 不复制 coordinator、token、reservation 或 handoff owner；
- `Merge` 只能显式转移一次 exact context；source 转移后不能 Commit/Abort；
- 普通 workload 继续使用现有 `Commit()`，保持兼容。

### 5.2 完整 batch 接口

```go
// Contract draft only.
type PreparedBindBatch struct {
    BatchID       string
    PlanDigest    string
    HandoffDigest string
    BindContexts  []*BindContext
    Owner         CommitOwnerRef
}

type BatchBinder interface {
    PrepareBatch(context.Context, []*BindContext) (*PreparedBindBatch, error)
    AcceptBatch(context.Context, *PreparedBindBatch) error
    CancelBatch(context.Context, *PreparedBindBatch) ReconcileResult
}
```

`PrepareBatch` 必须先校验整个 batch，再对所有成员执行 PreBind。第 N 项失败时逆序补偿已执行项，返回时 Kubernetes Bind 调用数
必须为 0。`AcceptBatch` 成功是 coordinator 将 handoff/evidence 重试责任交给 bind/recovery owner 的唯一时刻。

当前 `AddBindTask()`、`executePreBinds()` 和 `batchNum` 不能拼装出此保证；XPU-12 必须增加新的不可拆分 queue item，而不是循环调用
单项接口。

## 6. D4：首个真实 Adapter 与 XPU-01 输入

首个 Adapter 的**设备厂商和资源目标冻结为 NVIDIA**；首版 trusted identity contract 建议固定为：

```text
Vendor:        NVIDIA
ProviderID:    nvidia-nvml-v1
Namespace:     nvidia.com
ResourceName:  nvidia.com/gpu
DeviceID:      NVIDIA GPU UUID
Discovery API: NVML（测试环境可由 nvml-mock 提供）
```

角色必须分开：

| 组件 | XPU-01 中的责任 | 不能据此声称 |
| --- | --- | --- |
| `nvml-mock` | 模拟 NVIDIA UUID、型号、显存、PCI/NUMA、NVLink/NVSwitch、健康事件和 NVML 调用 | 真实 GPU 算力、CUDA/NCCL 性能、真实硬件故障隔离 |
| NVIDIA Device Plugin | 基于 mock driver root 注册 `nvidia.com/gpu`，验证 ListAndWatch/Allocate 和 kubelet 注入链 | 原生 Device Plugin API 已接受 Volcano 指定 UUID |
| NVIDIA Adapter | 接受 scheduler-selected GPU UUID，负责 reservation、幂等、epoch、Recover/Reconcile/Released | Kubernetes 多 Pod Bind 原子性 |
| 真实 NVIDIA 节点 | 在 M4 核验实际容器可见 UUID 等于 plan，并补真实重启/释放证据 | 未实际覆盖的 Fabric、MIG/vGPU 能力 |

HAMi 可以作为 NVIDIA 分配实现或 companion 方案的参考，但它不是底层设备厂商。若未来采用 HAMi-backed source/Adapter，
`ProviderID` 必须诚实标识实际 source contract，并与 paired Adapter 完全匹配；不能用 HAMi 替换 `Vendor=NVIDIA`，也不能把
`ProviderID`、`nvidia.com/gpu` 和 GPU UUID 三层身份混成一个字段。D4 不再写成“选择 HAMi Adapter”。

XPU-01 使用独立 harness，不依赖尚未实现的 scheduler plugin，并分两级输出：

1. **nvml-mock conformance**：
   - 枚举稳定 GPU UUID/topology/health，并与 Node `nvidia.com/gpu` 数量对账；
   - 让 NVIDIA Adapter 指定一个非默认 UUID；相同 token 重试得到同一 assignment；
   - 重启 Adapter owner 后枚举未完成记录；缺项按 Unknown；
   - Release 返回 owner-matched Released evidence；旧 epoch/过期 token 被拒绝；
   - 注入超时但实际成功、空结果、缺项和健康变化，验证不会错误 Free。
2. **真实 NVIDIA runtime gate（M4 前必须补）**：
   - 从实际容器/NVIDIA runtime 读回 Device UUID，与 PodUID、ContainerName、ResourceName、NodeUID、plan DeviceIDs 对照；
   - 验证真实进程重启、释放后复用和至少一套 Node-local exact-ID 环境；
   - 若声明 Fabric，再补真实 NVLink/NVSwitch membership 与跨 Node/模块身份核验。

`nvml-mock` 可以完成 XPU-01 的大部分协议和故障模拟，但 Mock 通过不自动升级为“真实硬件已验收”。如果 NVIDIA Adapter 仍只能让
kubelet/Device Plugin 自主选择 UUID，而不能执行 scheduler-selected UUID，则结论为 `XPUAssignmentNotEnforceable`；Advisory 和 Mock
协议开发仍可继续，M4 不放行。

## 7. D5：Public API、schema 与 canonicalization

### 7.1 冻结字段和限制

沿用 V4 `DeviceTopologySpec`，冻结以下 Alpha 规则：

| 项目 | Alpha 规则 |
| --- | --- |
| `mode` | `hard/soft`；缺省 `hard`；其他值拒绝 |
| `applyTo` | `Pod/Group`；缺省 `Pod` |
| `scope` | 必填，且只能是 `Node/Fabric` |
| `domainClass` | DNS-1123 label，1～63 字节，小写；不是具体 Domain ID |
| `resourceName` | Kubernetes qualified resource name；必须是扩展资源，不能是 `cpu/memory` |
| `policies` | 每个 DeviceTopologySpec 最多 16 条；canonical exact duplicate 折叠后再检查 |
| `podSelector` | 只允许 PodGroup 级 `applyTo=Pod`；Group/SubGroup policy 禁止 |
| annotation | key 为 `scheduling.volcano.sh/device-topology`；UTF-8 JSON 最大 16 KiB |
| catalog | canonical JSON 最大 512 KiB、最多 256 个 descriptor；超限整版拒绝 |

同一作用单元中，`resourceName + applyTo + scope + normalized selector` 相同而 `domainClass` 不同是冲突；Alpha 不计算 class 交集，
也不提供 acceptable-classes 列表。多条不同 scope/resource 的 policy 使用 AND。

### 7.2 strict 与 legacy

- annotation decoder 拒绝未知 apiVersion/字段、重复 JSON key、尾随 token、非 canonical 类型和超限；
- internal/versioned Go type 均不暴露 `tier/tierName`；
- served PodGroup/VCJob schema 暂留 reject-only legacy tombstone，并用 CEL 永久要求字段不存在；
- 必须用真实 API server 测试证明旧字段不是 prune 后被接受；
- typed field、VCJob、owner/template annotation 和 direct PodGroup 统一调用同一纯 canonicalization 包；
- `PolicyFingerprint` 只覆盖默认化后的用户 intent；固定 catalog 只提供 class 定义，不参与 workload 版本判断。

共享纯函数的最小边界冻结为：

```go
// Contract draft only. It must not read scheduler cache, ledger, or Adapter state.
func CanonicalizeDeviceTopology(
    spec *DeviceTopologySpec,
    source AuthoringSource,
    catalog CatalogView,
) (CanonicalDeviceTopologySpec, PolicyFingerprint, field.ErrorList)
```

`CatalogView` 是已加载的固定 catalog，调用期间不能变化。unknown class 是 field error；“class 已知但 Provider/Adapter 暂不可执行”
属于后续 compile/runtime reason，不能由 canonicalizer 把 policy 删除或改成其他 class。

## 8. D6：authoring 与 activity fence

### 8.1 选择 PodGroup status CAS 作为竞态闸门

新增一个小型、可观察的 status 结构；它不是完整 evidence store：

```go
// Contract draft only.
type DeviceTopologyActivityStatus struct {
    PolicyFingerprint string
    SpecGeneration    int64
    State             ActivityState // Quiescent, Reserving, Active, Reconciling
    EvidenceRef       string
    RecordRevision    string
    SchedulerEpoch    string
}
```

final reserve 前固定执行：

```text
re-read PodGroup UID/RV/generation/policy fingerprint
  -> UpdateStatus(ActivityState=Reserving) with resourceVersion CAS
  -> re-read accepted status and verify the same fingerprint/generation
  -> final ledger hold / Adapter Reserve
```

因此 policy update 与 first reserve 竞争时只能有一个先成功：

- spec update 先成功：旧 RV 的 status fence 冲突，scheduler 放弃旧 plan 并重新编译；
- status fence 先成功：spec update 的旧 RV 冲突；客户端重试时 webhook 看到非 Quiescent，拒绝 semantic mutation；
- status/evidence 不可读或写结果未知：按 Active/Unknown 处理，不允许更新或新 owner。

删除 policy 也是 semantic mutation。只有 `Quiescent`、无 evidence/anchor/ref、无 bind handoff 且 canonical fingerprint 变化检查通过时允许。
fingerprint 相同的 no-op 更新可以通过，但仍必须保持 owner/annotation authority 一致。

## 9. D7：激活、reload 和 action 支持矩阵

### 9.1 activation digest

digest 至少覆盖：

```text
XPUTopologyAwareScheduling gate value
accepted schedulerNames（排序）
xpu-topology-aware plugin arguments/callback enablement
Provider identity + resource owner identity/capabilities
fixed catalog readiness
EvidenceStore/Adapter identity
```

scheduler、admission 和 controller 的 accepted schedulerNames 必须一致。任一目标 Pod 的 `spec.schedulerName` 不在这份集合，xPU authoring
不应被该 Volcano 实例接管；在集合内但 gate/plugin/config 不完整时，带 policy 的对象 fail closed。

首次非法配置不进入 Ready；热更新先构造并恢复新 manager，完整验证后原子替换。旧配置在 reservation/evidence/anchor/
reconciliation 未清空前继续承担管理责任。feature gate 是进程启动参数，不热更；关闭需要 drain 后统一重启 scheduler/admission。

### 9.2 action 支持矩阵

| action/入口 | 当前源码行为 | soft Alpha | hard Exact Alpha |
| --- | --- | --- | --- |
| `allocate` normal branch | winning `Statement.Commit()` | 允许；Predicate 后评分，不建立 owner | 唯一允许创建新 exact owner 的入口；必须走 `CommitExact` |
| `allocate` hard-network/SubGroup branch | trial/Save/Recover 后 `Statement.Commit()` | 允许，保持 network gradient owner | 允许，但 AdmissionSet/checkpoint 必须覆盖 merged/recovered winning Statement |
| nomination fast path | 回到 allocate 的 Statement | 允许 | 允许；最终 placement 必须完整 revalidate，不能沿用旧 device plan |
| `backfill` | 直接 `Session.Allocate()` 并可能 dispatch | 允许 preference | **阻断带 hard policy 的 Task**；首个 Alpha 不创建 exact owner |
| `preempt/reclaim` | Statement 主要提交 Evict/Pipeline | 允许现有 victim 选择 | 可请求 eviction/pipeline；不得提前 reserve victim Device；后续 allocate 重新完整计划 |
| `gangpreempt/gangreclaim` | domain nomination + Evict/Pipeline Commit | 允许现有网络行为 | 同上；nomination 只是 hint，不是 xPU anchor/assignment |
| `shuffle` | 直接 `Session.Evict()` | 允许 | 只触发 ReleaseRequested；Released 前设备不回 Free |
| bind worker | 单项队列、逐 Pod PreBind/Bind | 普通路径不变 | 只接受完整 prepared batch；之后 Kubernetes Bind 仍逐 Pod |

core final guard 必须独立于 plugin callback：任何 hard policy 的新 Allocate 没有 exact prepared batch 时拒绝 dispatch。这样即使新 action
未来直接调用 `Session.Allocate()`，也不能绕过合同。

## 10. D8：多个 Group、SubGroup 和 resource 的所有权

一个 winning Statement 只有一个 `ExactCommitContext`。它可以包含多个逻辑 Group 和多个 resource owner：

```go
// Contract draft only.
type ExactCommitContext struct {
    StatementID  string
    AdmissionSet []TaskRef
    Placements   map[TaskID]TopologyTaskPlacement
    GroupPlans   map[TopologyGroupRef]GroupConstraintPlan
    Reservations map[ResourceOwnerRef]AdapterReservation
}
```

组合规则：

1. Job-level 和 SubGroup-level policy 先按每个 Task 展开，再求约束交集；同一 Task 只有一个 Node 和一组最终 DeviceKeys；
2. 一个 Task 可被多个 GroupRef 约束，但不能产生多份互相覆盖的 placement；
3. `Group + Node`、`Group + Fabric` 各自为所属稳定 Group 保存 class-bearing selection；Pod policy 不建立共享 anchor；
4. 同 resource 的所有 policy 由一个 exact owner 产生一份 assignment，不能由两个 Adapter 重复 reserve；
5. 不同 resource owner 按稳定 key 顺序 Reserve/Prepare；任一失败按逆序 Compensate，全部成功后才进入 evidence/batch；
6. Statement 中任一 Group 失败时整个 exact commit 不接受 batch；普通 Task 不允许从同一 Statement 被拆出提前 Bind；
7. `SaveOperations/RecoverOperations` 只复制可重建的 Task/Node 试算结果，GroupPlan/reservation 必须对 winning Statement 重新生成或显式转移。

这会使多 resource 具备“协调提交 + 可审计补偿”，但不声称跨 backend 或 Kubernetes 多 Pod Bind 是分布式原子事务。

## 11. 固定提交顺序和故障责任

```text
checkpoint allocation-attempt side state
  -> derive complete AdmissionSet from winning Statement
  -> compile all Job/SubGroup policies and complete group plan
  -> write ActivityFence(Reserving) with PodGroup RV CAS
  -> final ledger all-or-nothing hold
  -> Adapter Reserve all resource owners
  -> Adapter Prepare all assignments
  -> EvidenceStore CAS write group anchor/assignment
  -> BatchBinder PrepareBatch (all PreBind, zero Kubernetes Bind)
  -> Adapter Commit
  -> BatchBinder AcceptBatch (single owner-transfer point)
  -> individual Kubernetes Bind
```

| 失败点 | 必须动作 | 禁止推断 |
| --- | --- | --- |
| ActivityFence CAS 冲突 | 放弃旧 plan，恢复 checkpoint，重新读取 policy | 不得忽略冲突沿用旧 fingerprint |
| final ledger hold 冲突 | all-or-nothing 零修改，恢复 Statement/checkpoint | 不得保留部分 DeviceKeys |
| 第 N 个 Adapter Reserve/Prepare 失败 | 逆序 Compensate 已成功 owner；不调用 PreBind/Bind | nil/error 缺失不得视为 Released |
| evidence 写明确失败 | Compensate Adapter、释放或隔离 ledger、恢复 | 不得在无 durable record 时继续 Bind |
| evidence 写结果未知 | 查询稳定 record key；进入 ReconcilePending | 不得再创建第二个 record/owner |
| 第 N 个 PreBind 失败 | rollback 全部已执行 PreBinder，再 Compensate Adapter | 成功的前 N-1 项不得入 bind queue |
| Adapter Commit 成功、AcceptBatch 失败/未知 | 保留 owner/evidence并 reconcile batch 接收结果 | 不得当作未分配或直接 Free |
| AcceptBatch 后第 N 个 Kubernetes Bind 失败 | 成功项观察 Allocated，失败项补偿/reconcile | 不得伪造整组 Kubernetes rollback |
| Release/Reconcile timeout、空或缺项 | 保持 ReconcilePending/Unknown | 不得因 Pod NotFound 或列表缺项 Free |

## 12. 源码触点与所有权

| 合同 | 当前触点 | 后续主责 |
| --- | --- | --- |
| gate/plugin/config strict validation | `pkg/features/volcano_features.go`、`pkg/scheduler/{util,scheduler}.go`、`framework/plugins.go` | XPU-02/S |
| catalog/API/canonicalization | `staging/src/volcano.sh/apis/pkg/apis/{scheduling,batch}/`、生成链 | XPU-03/A+S |
| authoring/activity | `pkg/controllers/podgroup/pg_controller_handler.go`、job controller、admission、PodGroup status | XPU-04/A |
| Provider/cache/snapshot | `pkg/scheduler/cache/{cache,event_handlers}.go`、`api/cluster_info.go`、`framework/session.go` | XPU-05/06/S |
| Group planning | `framework/session_plugins.go`、`actions/allocate/allocate.go`、Job/SubJob | XPU-08/S |
| checkpoint/Statement owner | `actions/allocate/{allocate,recorder}.go`、`framework/statement.go` | XPU-09/S |
| ledger/Adapter/evidence | 新 topology manager/adapter 包；PodGroup status reference | XPU-10/11/S+R |
| complete batch | `pkg/scheduler/cache/{interface,cache}.go`、PreBinder registry | XPU-12/S |
| bypass/release/recovery | allocate/backfill/preempt/reclaim/gang*/shuffle、Pod/Node events | XPU-13/14/S+R |

当前事实不能与提议混写：`Statement.Commit()` 仍无 error；`backfill` 直接调用 `Session.Allocate()`；`AddBindTask()` 仍单项入队；
`executePreBinds()` 会保留成功成员。本文没有改变这些实现。

### 12.1 当前调用链与插入点

普通与 hard-network/SubGroup allocate 最终都在同一个函数里提交，但前面的试算路径不同：

```text
allocate.Action.Execute
  -> allocateResourcesForQueues
     -> normal: allocateResourcesForTasks / allocateFromNomination
     -> hard-network/SubGroup: allocateForJob
        -> allocateForSubJob
        -> SaveOperations -> Discard trials -> RecoverOperations winning result
  -> Statement.Commit
     -> Statement.allocate
     -> SchedulerCache.AddBindTask
     -> processBindTask -> BindTask
     -> executePreBinds -> Bind
```

XPU-09 的 checkpoint 和 complete AdmissionSet 必须放在 `allocateResourcesForQueues` 选出 winning Statement 之后、现有 `Commit()` 之前；
不能放在单 Task `allocateResourcesForTask` 内，否则看不到完整 Group、多 SubGroup 和 Recover 后的最终 operations。

当前 backfill 是独立旁路：

```text
backfill.Action.Execute
  -> PredicateNodes / BatchNodeOrderFn
  -> Session.Allocate
  -> Session.dispatch
  -> SchedulerCache.AddBindTask
```

因此仅给 `Statement` 增加 hook 不能保护 backfill。XPU-02/13 的 core guard 必须在 `Session.Allocate/dispatch` 或更靠近 cache batch
入口再次识别 hard policy；首个 Alpha 在调用 `Session.Allocate` 前返回 `XPUTopologyActionNotSupported`。

preempt/reclaim/gangpreempt/gangreclaim 的 Statement 主要产生 Evict/Pipeline operations；shuffle 直接 `Session.Evict()`。这些路径只改变
资源未来可用性，不得把 Pod eviction 成功、`FutureIdle` 或 Pipeline 当成 Device `Released`。XPU-14 在 cache Pod event 与 Adapter
reconciliation 汇合处完成最终 Free 转移。

## 13. Fixture 规范与可追踪验收

机器可读 fixture 初稿见
[`testdata/xpu-00-contract-fixtures.yaml`](./testdata/xpu-00-contract-fixtures.yaml)。后续工作包不得复制后改名；应直接引用 case ID，
并在测试层补充运行环境和断言。

最低映射：

| fixture | 最先落地 |
| --- | --- |
| `canonical-equivalent-order-defaults`、`legacy-tier-rejected`、`unknown-class-authoring-error` | XPU-03/04 |
| `node-domain-fragmented-6-plus-2`、四种 `applyTo × scope`、`node-and-fabric-and` | XPU-08 |
| `statement-multiple-subgroups-resources`、`backfill-hard-bypass-blocked` | XPU-09/13 |
| `activity-fence-wins`、`spec-update-wins` | XPU-04/11/13 |
| `prebind-member-n-fails-zero-bind` | XPU-12/13 |
| `release-missing-is-unknown`、`old-epoch-rejected` | XPU-10/14/15 |
| `nvidia-nvml-mock-selected-uuid-idempotent` | XPU-01/10/15 Mock conformance |
| `nvidia-real-runtime-id-gate` | XPU-15/16 真实硬件 M4 gate |

## 14. 评审出口与剩余阻塞

XPU-00 可以在以下条件满足后从“实现基线草案”转为“已冻结”：

- API reviewer 接受 D1/D5/D6 的 ConfigMap、schema 和 status fence 形状；
- scheduler/framework reviewer 接受 D3 的单 coordinator、error-returning exact commit 与 complete batch 边界；
- runtime reviewer 接受 D2 evidence contract，并确认 XPU-01 的 NVIDIA Adapter、nvml-mock harness 与真实硬件补验环境；
- allocate/bind/release 责任人逐项签核 action 矩阵和失败顺序；
- 所有 `FrozenForAlpha` 决策均有 owner，所有 `ProbePending` 决策均有对应 XPU-01 证据链接。

以下不阻塞 XPU-00/XPU-01：multiple acceptable classes、真实 Fabric 发布、topology-aware victim selection、DRA、MIG/vGPU、
multi-container、active-active reservation 和 workload start barrier。它们保持 Deferred，不能混入首个 Alpha 的能力声明。
