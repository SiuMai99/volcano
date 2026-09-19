# Volcano 通用 xPU 拓扑感知调度 V4 合同冻结记录（XPU-00）

> 基线：[V4 设计](./99-generic-xpu-topology-aware-design-zh-v4.md)与
> [V4 开发计划](./100-generic-xpu-topology-aware-development-plan-zh-v4.md)。
>
> 状态：**实现基线草案，待 maintainer/API/runtime 联合评审**。
> 本文冻结首个 Alpha 的实现输入，不表示上游已经接受，也不表示功能已经实现。
> 源码核对日期：2026-09-16；源码基线：`7604cc7d3`，当前文档分支 HEAD 仅增加设计/计划文档。

## 1. XPU-00 的完成边界

XPU-00 不新增 feature gate、CRD 字段或 scheduler plugin 实现。它只解决如果不先统一、会导致
当前 Alpha 工作包跨组件返工的合同问题。Alpha 的 anchor 采用现有 HyperNode 同类的 Pod-derived 方案，不新增独立的
Group 持久化记录。

完成 XPU-00 需要同时具备：

1. 各项决策都有一个明确的 Alpha 实现基线、不可缩减的底线和变更触发条件；
2. Public/canonical、Pod-derived anchor、Pod assignment annotation、Statement 与 Provider identity probe 有最小接口草案；
3. 能产生 assignment 或 Bind 的 `allocate/backfill/nomination` 路径有 hard/soft 行为和 final guard；`preempt/reclaim/gang*/shuffle`
   只对“不改变 assignment”做旁路审计；
4. Pod 已绑定后的 anchor 恢复、annotation 缺失/冲突和单 leader 失败行为有反例；
5. 正反例 fixture 具有稳定 case ID，可被后续单元、API server、Fake Provider 和 E2E 复用；
6. 尚需社区或真实 backend 证明的内容明确标为评审项，不伪装成已实现事实。

本文中的状态含义：

| 状态 | 含义 |
| --- | --- |
| `FrozenForAlpha` | 后续工作包可以按此实现；变更需要更新本文、受影响 fixture 和依赖工作包 |
| `ProbePending` | 合同已冻结，但 backend 是否满足合同必须由 XPU-01 提供运行证据 |
| `CommunityReview` | 推荐形状已给出，maintainer 可调整命名/落点，但不得削弱已列不变量 |

## 2. 决策总表

| ID | Alpha 实现基线 | 状态 | owner 角色 | 首个消费者 |
| --- | --- | --- | --- | --- |
| D1 catalog 权威载体 | Alpha 使用随 scheduler/plugin 交付的固定 catalog；其他组件只校验 policy 形状，scheduler 对未知 class fail closed；运行期间不更新 | `FrozenForAlpha` | S | XPU-03/05 |
| D2 anchor/assignment 载体 | Alpha 从已绑定 Pod 的 `spec.nodeName` 与受控 XPU assignment annotation 重建 anchor；直接扫描成员 Pod，不引入独立持久化记录或摘要状态 | `FrozenForAlpha` | S | XPU-11 |
| D4 首个 Provider/identity | Alpha 要求 Provider/Device Plugin 能消费或确认 Pod assignment annotation | `ProbePending` | R | XPU-01 |
| D5 API/schema | V4 `DeviceTopologySpec`；class 为 DNS-1123 label；严格 JSON；只保留实现所需的最小校验；若曾有 served V3 才保留 reject-only 兼容字段 | `FrozenForAlpha` | A | XPU-03/04 |
| D6 authoring/activity | anchor 建立后禁止 topology policy semantic mutation；复用 PodGroup spec 的现有 resourceVersion/generation 校验，不新增 `ActivityFence` | `FrozenForAlpha` | A/S | XPU-04/11 |
| D7 激活/action | gate/plugin 组合和 scheduler 本地 catalog 必须有效；能产生 assignment/Bind 的路径有 hard final guard，其他 action 只做旁路审计 | `FrozenForAlpha` | S | XPU-02/13/14 |
| D8 组合/状态归属 | 一个 winning Statement 一个最终 placement context；其中可含多个 GroupRef/资源，但每个 Task 只有一份最终 placement，全部约束取交集 | `FrozenForAlpha` | S/R | XPU-08/11/13 |

## 3. D1：catalog 权威载体

### 3.1 选择

Alpha 不新增 `ResourceTopologyDescriptor` CRD，也不实现 catalog 动态更新。`ResourceTopologyDescriptor` 只作为规范/内存模型，
首个 catalog 随 scheduler/plugin 的静态配置或安装包交付，由 scheduler 在启动和配置重载时加载一次。

catalog 不是 scheduler、controller、webhook、Provider 之间的共享控制面协议：

1. scheduler/plugin 是 Alpha catalog 的运行时权威消费者；
2. controller/webhook 只校验 `scope + domainClass` 的字段形状和 canonical policy，不 watch 或转发 scheduler catalog；
3. Provider 只在已加载 catalog 的 scheduler 侧把事实规范化到已知 class；
4. catalog 缺失或非法时 scheduler 对相关 xPU workload fail closed；运行期间不更新，新增/改变 class 另开兼容性评审。

### 3.2 交付和失败

- catalog 的具体文件、ConfigMap 或 Helm value 形状由实现阶段决定，不作为跨组件 API 冻结；
- catalog 必须是严格、只读、可重复加载的单一输入，resource/scope/class 不匹配时整版不可用；
- scheduler/plugin 不能以另一份 class 定义继续规划；未知 class 对 hard/soft 都是 authoring error 或 Pending；
- controller/webhook 不可读 scheduler catalog 时不承担“猜测 class”职责，只保留 policy；scheduler 仍须 fail closed。

### 3.3 为什么不是当前 HyperNode/Node annotation

catalog 表达管理员定义的稳定 class；HyperNode 描述网络层次，Node annotation 是 Provider observation。二者都不能同时作为
workload selector 的权威字典，否则同一个 `domainClass` 会被不同进程、不同 Session 或不同 Node 重解释。

## 4. D2：Pod-derived Group anchor 与 XPU assignment annotation

### 4.1 Alpha 选择

Alpha 不新增 Group durable record 或 PodGroup activity status。anchor 复用现有
HyperNode 的恢复思路：scheduler session 从已经绑定/运行中的 Group 成员 Pod 重建它。

每个已绑定成员提供两类事实：

```text
Pod.spec.nodeName
  -> Kubernetes 已确认的 Node placement

volcano.sh/xpu-assignment
  -> 该 Pod 的 scheduler/provider assignment（resource + canonical DeviceKeys）
```

建议的最小 annotation payload：

```json
{
  "version": 1,
  "resourceName": "nvidia.com/gpu",
  "provider": "nvidia-nvml-v1",
  "deviceKeys": ["<node-uid>/GPU-aaaaaaaa"]
}
```

`NodeName` 不重复写入 annotation；`deviceKeys` 必须包含能区分 Node replacement 的 canonical identity，不能只写 GPU index。
单容器整卡 Alpha 不要求把 `ContainerName` 或完整 plan 写入 annotation。
annotation 只保留上述四个字段；Domain/Fabric、GroupRef 和 plan digest 等派生信息由当前 snapshot 和 PodGroup 成员重建，
不进入 Alpha assignment contract。

Group anchor 是本次 Session 中从这些已绑定成员推导出的内存对象，而不是新的 API 对象：

```go
type PodDerivedGroupAnchor struct {
    GroupRef        TopologyGroupRef
    ResourceName   corev1.ResourceName
    Scope          DeviceTopologyDomainScope
    Class          DomainClassKey
    LocalDomainKey *LocalDomainKey
    FabricKey      *FabricKey
}
```

Alpha 不写入 `PodGroup` anchor 摘要；恢复时直接扫描成员 Pod，避免引入可过期的重复状态源。

恢复算法固定为：

1. 从 scheduler cache 找到该 Group 已绑定/运行的成员，使用现有 `AllocatedStatus` 与非空 `NodeName` 过滤；
2. 解析每个成员的 `xpu-assignment`，并用当前 immutable topology snapshot 将 `Node + DeviceKeys` 映射到 Domain/Fabric；
3. `Group + Node` 要求所有成员落在同一个 LocalDomain，`Group + Fabric` 要求所有成员存在共同 Fabric；
4. 结果一致时写入本次 Session 的 `JobInfo/SubJobInfo` anchor；缺失、冲突或无法映射时，hard Group Pending，不选择第二个实例；
5. 没有已绑定成员时不建立 Group anchor，首个 admission wave 使用本次 Session 的 plan；Bind 后的下一个 Session 再从 Pod 恢复。

同一 Session 内尚未 Bind 的成员只使用 Statement/JobInfo 中的试算 plan，不要求提前写入 PodGroup。Alpha 只允许一个 active
scheduler leader 负责该调度路径，不设计两个 scheduler 同时更新同一 Group 的 owner 语义。

### 4.2 权威边界与非目标

- 未绑定 Pod 上的同名 annotation 只是用户输入或计划，不能建立 Group anchor；
- 已绑定 Pod 的 assignment annotation 只有在 scheduler/provider 明确拥有该 key，且 assignment 能通过当前 Node/topology
  facts 校验时，才可作为 Alpha 的恢复事实；
- metrics 和 log 都只能作为可重建的诊断信息；
- Alpha 不负责外部设备 reservation、release 或跨系统故障对账；
- 共享/分数设备、MIG/vGPU、DRA Claim 和多 Container 的 Alpha assignment 语义延期，不通过 annotation 猜测；XPU-01 可以复用已有
  Volcano vGPU Adapter 做独立 L1 exact UUID 证据，但不因此冻结或承诺 Alpha 的 vGPU workload API。

如果未来需要在 Pod 消失后恢复外部设备状态或增加多 Pod 提交屏障，应另立需求和合同；它们不属于 Alpha，也不作为本合同的实现前置。

## 5. D4：首个 Provider identity 与 XPU-01 输入

首个 Provider 的**设备厂商和资源目标冻结为 NVIDIA**；首版 trusted identity contract 建议固定为：

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
| Volcano vGPU/HAMi Adapter | 基于 `volcano-vgpu-device-plugin` 和 deviceshare 验证 UUID allowlist、`vgpu-ids-new`、`devices-to-allocate` 与 `NVIDIA_VISIBLE_DEVICES` 的一致性；验证配置为 `deviceSplitCount=1`、`volcano.sh/vgpu-number=1` | generic scheduler-owned `DeviceKey` 已被 stock Device Plugin 消费；Alpha 已支持 vGPU/MIG 或真实硬件隔离 |
| NVIDIA Provider/Device Plugin | Alpha 中验证能消费或确认 Pod 上的 scheduler-selected GPU UUID | Kubernetes 多 Pod Bind 原子性或 exact-ID enforcement |
| 真实 NVIDIA 节点 | 在 M4 核验实际容器可见 UUID 等于 plan，并补真实重启/释放证据 | 未实际覆盖的 Fabric、MIG/vGPU 能力 |

HAMi 可以作为 NVIDIA 分配实现或 companion 方案的参考，但它不是底层设备厂商。不能用 HAMi 替换 `Vendor=NVIDIA`，也不能把
`ProviderID`、`nvidia.com/gpu` 和 GPU UUID 三层身份混成一个字段。

XPU-01 使用独立 harness，不依赖尚未实现的 scheduler plugin，并按 stock、vGPU Adapter、真实 runtime 三条轨道输出：

1. **stock nvml-mock conformance**：
   - 枚举稳定 GPU UUID/topology/health，并与 Node `nvidia.com/gpu` 数量对账；
   - 观察原生 NVIDIA Device Plugin 是否存在接收、确认和强制 Volcano selected UUID 的通道；不存在时返回 `XPUAssignmentNotEnforceable`，不把 kubelet 自选 ID 解释成 exact-ID；
   - 同一 Pod 的 assignment annotation 重读后得到稳定的 canonical DeviceKey；
   - 注入缺失、格式非法、Node replacement 和 topology 映射冲突，验证 scheduler 不会错误建立 Group anchor；
2. **Volcano vGPU Adapter conformance（独立 L1 轨道）**：
   - 使用非默认 UUID 的单值 `volcano.sh/vgpu-use-gpuuuid`，并将 Device Plugin/node config 的虚拟化槽数统一设置为 `1`；
   - 验证 `volcano.sh/vgpu-ids-new`、`volcano.sh/devices-to-allocate` 和 `NVIDIA_VISIBLE_DEVICES` 均保留同一个 UUID；
   - 验证目标 UUID 不存在、槽位已占用或注入 UUID 被替换时 fail closed；
   - 明确 allowlist 是现有 vGPU 案例的用户输入，不把它写成 generic scheduler-owned `DeviceKey` 的实现证据；
3. **真实 NVIDIA runtime probe（是否纳入本阶段由实现评审确认）**：
   - 从实际容器/NVIDIA runtime 读回 Device UUID，与 PodUID、ContainerName、ResourceName、NodeUID、plan DeviceIDs 对照；
   - 记录真实运行时设备身份与 assignment 的对应关系；
   - 若声明 Fabric，再补真实 NVLink/NVSwitch membership 与跨 Node/模块身份核验。

`nvml-mock` 可以完成 XPU-01 的大部分身份、annotation 和拓扑映射测试，但 Mock 通过不自动升级为“真实硬件已验收”。如果 Provider/Device
Plugin 无法消费或确认 scheduler-selected UUID，则结论为 `XPUAssignmentNotEnforceable`；相关 hard workload 保持 Pending，未验证的运行时
行为不写入 Alpha 能力声明。

## 6. D5：Public API、schema 与 canonicalization

### 6.1 冻结字段和限制

沿用 V4 `DeviceTopologySpec`，冻结以下 Alpha 规则：

| 项目 | Alpha 规则 |
| --- | --- |
| `mode` | `hard/soft`；缺省 `hard`；其他值拒绝 |
| `applyTo` | `Pod/Group`；缺省 `Pod` |
| `scope` | 必填，且只能是 `Node/Fabric` |
| `domainClass` | DNS-1123 label，1～63 字节，小写；不是具体 Domain ID |
| `resourceName` | Kubernetes qualified resource name；必须是扩展资源，不能是 `cpu/memory` |
| `policies` | 只要求可解析、可 canonicalize、无冲突；具体数量上限由实现阶段按 API server 与对象大小确定，不冻结为跨组件合同 |
| `podSelector` | 只允许 PodGroup 级 `applyTo=Pod`；Group/SubGroup policy 禁止 |
| annotation | Alpha canonical policy 只来自 PodGroup typed spec；workload annotation ergonomic 路径延期；assignment annotation 另有最小四字段合同 |
| catalog | 严格、只读、可加载；大小/descriptor 数量只设实现阶段的最小防护，不冻结具体数字 |

同一作用单元中，`resourceName + applyTo + scope + normalized selector` 相同而 `domainClass` 不同是冲突；Alpha 不计算 class 交集，
也不提供 acceptable-classes 列表。多条不同 scope/resource 的 policy 使用 AND。

### 6.2 strict 与 legacy

- annotation decoder 拒绝未知 apiVersion/字段、重复 JSON key、尾随 token、非 canonical 类型和超限；
- internal/versioned Go type 均不暴露 `tier/tierName`；
- 若确认某 served version 曾暴露 V3 `tier/tierName`，该 version 才保留 reject-only legacy tombstone，并用 CEL 拒绝；若没有实际 served 版本，不新增兼容字段；
- 只有启用 legacy tombstone 的 served version 需要用真实 API server 测试证明旧字段不是 prune 后被接受；
- Alpha canonicalization 先服务 direct PodGroup typed spec；VCJob、owner/template annotation 等 ergonomic source 延期，不冻结多源统一优先级；
- `PolicyFingerprint` 只覆盖默认化后的用户 intent；固定 catalog 只提供 class 定义，不参与 workload 版本判断。

共享纯函数的最小边界冻结为：

```go
// Contract draft only. It must not read scheduler cache or live allocation state.
func CanonicalizeDeviceTopology(
    spec *DeviceTopologySpec,
    source AuthoringSource,
    catalog CatalogView,
) (CanonicalDeviceTopologySpec, PolicyFingerprint, field.ErrorList)
```

`CatalogView` 是已加载的固定 catalog，调用期间不能变化。unknown class 是 field error；class 已知但 Provider 暂不可执行时，由运行时返回
明确 reason，不能由 canonicalizer 把 policy 删除或改成其他 class。

## 7. D6：authoring 与 Pod-derived anchor activity

### 7.1 Alpha 选择：不新增 ActivityFence

Alpha 不新增 `DeviceTopologyActivityStatus`，也不把 anchor 写入 PodGroup status。Group 是否已经建立 anchor，直接由已绑定成员
Pod 的 NodeName 和 `xpu-assignment` annotation 判断；anchor 在 scheduler 的 `JobInfo/SubJobInfo` 中按需重建。

第一轮尚无已绑定成员时，anchor 只存在于本次 Session 的试算状态。第一轮 Bind 成功后，后续 Session 从 Pod cache 恢复 anchor。
这是与现有 HyperNode recovery 相同的生命周期，不要求在 Bind 前增加一条 PodGroup 持久化写入。

Alpha 的 policy mutation 规则为：

- 没有已绑定成员时，可以按现有 webhook/controller 规则更新 policy；
- 已存在有效的 Group anchor 时，拒绝 `resource/scope/domainClass/applyTo/selector` 等 semantic mutation；
- 只允许同一 policy 的无语义变化更新；
- 由单 active scheduler leader 读取和更新缓存，普通 Kubernetes `resourceVersion` 只用于已有对象更新冲突；
- policy 或成员信息读取不一致时，丢弃本轮 plan 并重新读取，不创建第二个 anchor。

Pod 删除后的外部设备生命周期不属于 Alpha；本阶段只根据 Pod/cache/provider 的现有事实更新下一轮可用资源。

## 8. D7：激活、reload 和 action 支持矩阵

### 8.1 激活与 fail-closed

Alpha 只冻结本进程的最小组合校验：feature gate 与 `xpu-topology-aware` plugin 必须同时有效，scheduler 本地 catalog 可加载，
Provider identity/assignment contract 可用；任一条件不满足，scheduler 不调度相关 xPU workload。admission/controller 不读取 scheduler
运行时配置，不参与跨进程 activation digest 或共享配置同步。

feature gate 仍是进程启动参数；Alpha 不定义热更新、统一 drain、跨 replica digest 或 activation 状态机。关闭/切换由发布流程在
后续验收阶段决定，不能借此把非空 xPU policy 当普通 workload 调度。

### 8.2 action 支持矩阵

| action/入口 | 当前源码行为 | soft Alpha | hard Topology Alpha |
| --- | --- | --- | --- |
| `allocate` normal branch | winning `Statement.Commit()` | 允许；Predicate 后评分 | 允许；在 BindContext 写入 assignment annotation，anchor 只保存在 Session 或由已绑定 Pod 恢复 |
| `allocate` hard-network/SubGroup branch | trial/Save/Recover 后 `Statement.Commit()` | 允许，保持现有 network gradient | 允许；merged/recovered winning Statement 必须重新生成并校验 DeviceKeys |
| nomination fast path | 回到 allocate 的 Statement | 允许 | 允许；最终 placement 必须重新校验，不能沿用失效的 DeviceKeys |
| `backfill` | 直接 `Session.Allocate()` 并可能 dispatch | 允许 preference | **阻断无法写入 assignment annotation 或无法恢复 Group anchor 的 hard Task** |
| `preempt/reclaim` | Statement 主要提交 Evict/Pipeline | 允许现有 victim 选择 | 只审计不产生新 assignment；后续 allocate 重新完整计划 |
| `gangpreempt/gangreclaim` | domain nomination + Evict/Pipeline Commit | 允许现有网络行为 | 只审计不产生新 assignment；nomination 不是 xPU anchor |
| `shuffle` | 直接 `Session.Evict()` | 允许 | 只审计 Pod 生命周期；Pod 删除后由现有 cache/provider 反映下一轮可用性 |
| bind worker | 单项队列、逐 Pod PreBind/Bind | 普通路径不变 | 复用现有逐 Pod PreBind/Bind；每个成功绑定 Pod 必须保留 assignment annotation |

core final guard 必须覆盖会产生新 assignment 或 Bind 的 `allocate/backfill/nomination` 路径：如果无法生成并写入可校验的
assignment annotation，拒绝 dispatch。preempt/reclaim/gang*/shuffle 不创建 assignment，只保留“不改变 assignment”的审计测试。

## 9. D8：多个 Group、SubGroup 和 resource 的组合

Alpha 一个 winning Statement 仍只产生一份最终 placement context，可以包含多个逻辑 Group 和 resource；不创建跨 backend 的
全局 owner。

组合规则：

1. Job-level 和 SubGroup-level policy 先按每个 Task 展开，再求约束交集；同一 Task 只有一个 Node 和一组最终 DeviceKeys；
2. 一个 Task 可被多个 GroupRef 约束，但不能产生多份互相覆盖的 placement；
3. `Group + Node`、`Group + Fabric` 各自为所属稳定 Group 保存 class-bearing selection；Pod policy 不建立共享 anchor；
4. 同 resource 的每个 Pod 只产生一份 assignment annotation；Alpha 不建立跨 backend 的全局 owner；
5. 同一 Group 的已绑定成员必须得到一致的 Domain/Fabric anchor；缺失或冲突时 Group Pending；
6. Statement 中的普通 Task/Node operation 仍由现有 Statement 管理；Alpha 不增加新的提交事务层；
7. `SaveOperations/RecoverOperations` 只复制可重建的 Task/Node 试算结果，Group anchor 在新 Session 中从已绑定 Pod 重新推导。

这保留了 Group 拓扑语义，但不把 Alpha 扩展成跨 backend 的分布式事务。

## 10. Alpha 提交边界与失败责任

```text
Alpha:
  -> compile Group/Pod topology policy in the current Session
  -> Statement.Allocate selects Node and canonical DeviceKeys
  -> attach scheduler-owned volcano.sh/xpu-assignment to each BindContext
  -> existing per-Pod PreBind/Bind
  -> next Session derives Group anchor from bound Pods

本 Alpha 不提供跨系统 reservation、完整 batch 或多 Pod Bind 原子回滚；这些能力如有需要，必须另立需求。
```

| 失败点 | 必须动作 | 禁止推断 |
| --- | --- | --- |
| assignment annotation 缺失或格式非法 | 不建立该 Pod/Group 的恢复 anchor；hard policy 保持 Pending | 不得从 GPU index 猜测 DeviceKey |
| 已绑定成员的 Node/DeviceKeys 无法映射到当前 topology | Group 保持 Pending，等待事实恢复或人工处理 | 不得选择第二个 Domain/Fabric 规避冲突 |
| 同一 Group 的已绑定成员 anchor 冲突 | Group 保持 Pending，并记录可重建诊断信息 | 不得覆盖旧成员或把冲突当成新 Group |
| Node 被同名替换且 NodeUID 不一致 | 拒绝恢复旧 assignment，重新走调度 | 不得仅凭 NodeName 或 GPU index 复用旧 anchor |
| 普通 Statement/单 Pod Bind 失败 | 沿用现有 scheduler/cache 错误路径；未绑定成员不构成 anchor | 不得宣称 Kubernetes 多 Pod Bind 原子回滚 |

## 11. 源码触点与所有权

| 合同 | 当前触点 | 后续主责 |
| --- | --- | --- |
| gate/plugin activation guard | `pkg/features/volcano_features.go`、`pkg/scheduler/{util,scheduler}.go` | XPU-02/S |
| catalog/API/canonicalization | `staging/src/volcano.sh/apis/pkg/apis/{scheduling,batch}/`、生成链 | XPU-03/A+S |
| authoring/activity | `pkg/controllers/podgroup/pg_controller_handler.go`、job controller、admission、PodGroup status | XPU-04/A |
| Provider/cache/snapshot | `pkg/scheduler/cache/{cache,event_handlers}.go`、`api/cluster_info.go`、`framework/session.go` | XPU-05/06/S |
| Group planning | `framework/session_plugins.go`、`actions/allocate/allocate.go`、Job/SubJob | XPU-08/S |
| Session-local Group plan/Statement | `actions/allocate/{allocate,recorder}.go`、`framework/statement.go` | XPU-09/S |
| Pod-derived anchor/assignment annotation | `pkg/scheduler/framework/session.go`、`pkg/scheduler/cache/event_handlers.go`、Pod Bind annotation path | XPU-11/S |
| hard-policy bypass/Pod recovery | allocate/backfill/nomination 的 assignment guard；preempt/reclaim/gang*/shuffle 的不变性审计；Pod/Node events | XPU-13/14/S |

当前事实不能与提议混写：`Statement.Commit()` 仍无 error；`backfill` 直接调用 `Session.Allocate()`；`AddBindTask()` 仍单项入队；
`executePreBinds()` 会保留成功成员。本文没有改变这些实现。

### 11.1 当前调用链与插入点

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

Alpha 不增加独立 checkpoint 或新的提交事务层。Group plan 在 `allocateResourcesForQueues` 选出 winning Statement
后随现有 Statement 提交；不能在单 Task 试算结束时提前宣布 Group anchor，因为此时还看不到完整 Group、多 SubGroup 和 Recover 后的最终
operations。

当前 backfill 是独立旁路：

```text
backfill.Action.Execute
  -> PredicateNodes / BatchNodeOrderFn
  -> Session.Allocate
  -> Session.dispatch
  -> SchedulerCache.AddBindTask
```

因此仅保护 `Statement` 提交不能覆盖 backfill。XPU-02/13 的 core guard 必须在 `Session.Allocate/dispatch` 或更靠近 cache 提交入口再次识别
hard policy；如果该入口无法产出并保留 assignment annotation，就保持 Pending。preempt/reclaim/gang*/shuffle 不进入同一 guard，
只验证它们不创建 assignment、不伪造释放事实。

preempt/reclaim/gangpreempt/gangreclaim 的 Statement 主要产生 Evict/Pipeline operations；shuffle 直接 `Session.Evict()`。这些路径只改变
资源未来可用性。Alpha 不从 eviction、`FutureIdle` 或 Pipeline 推断外部设备释放；Pod 删除和 provider/cache 的正常更新负责反映下一轮可用资源，
XPU-14 只检查 anchor 恢复与旁路行为。

## 12. Fixture 规范与可追踪验收

以下是 XPU-00 的稳定 fixture case ID；实现阶段可将其映射到测试文件，但本合同不冻结 fixture 文件路径或具体序列化格式。
后续工作包不得复制后改名，应直接引用 case ID，并在测试层补充运行环境和断言。

最低映射：

| fixture | 最先落地 |
| --- | --- |
| `canonical-equivalent-order-defaults`、`legacy-tier-rejected-if-served`、`unknown-class-authoring-error` | XPU-03/04 |
| `node-domain-fragmented-6-plus-2`、四种 `applyTo × scope`、`node-and-fabric-and` | XPU-08 |
| `statement-multiple-subgroups-resources`、`backfill-hard-bypass-blocked` | XPU-09/13 |
| `bound-pod-anchor-recovery`、`xpu-assignment-annotation-persisted` | XPU-11/13 |
| `xpu-assignment-missing-pending`、`xpu-assignment-conflict-pending` | XPU-11/13 |
| `xpu-node-replacement-rejected` | XPU-11/14 |
| `nvidia-nvml-mock-selected-uuid-idempotent` | XPU-01/11 Mock conformance |
| `nvidia-real-runtime-id-gate` | XPU-01 真实硬件 identity probe |

## 13. 评审出口与剩余阻塞

XPU-00 可以在以下条件满足后从“实现基线草案”转为“已冻结”：

- API/scheduler reviewer 接受 D1/D5/D6 的最小 catalog、schema 和 policy mutation 规则；
- scheduler/framework reviewer 接受 D2 的 Pod-derived anchor、assignment annotation 与单 leader 边界，并确认 Alpha 不新增事务层；
- runtime reviewer 接受 D4 的 scheduler-owned assignment identity contract，并确认 XPU-01 的 NVIDIA Provider、nvml-mock harness 与后续真实硬件补验环境；
- allocate/bind 责任人逐项签核 action 矩阵、Pod annotation 持久路径和旁路行为；
- 所有 `FrozenForAlpha` 决策均有 owner，所有 `ProbePending` 决策均有对应 XPU-01 证据链接。

以下不阻塞 XPU-00/XPU-01：multiple acceptable classes、真实 Fabric 发布、topology-aware victim selection、DRA、MIG/vGPU workload API、
multi-container 和 workload start barrier。现有 Volcano vGPU Adapter 的 L1 证据可以作为 D4 的旁证，但不能混入首个 Alpha 的 vGPU 能力声明。
