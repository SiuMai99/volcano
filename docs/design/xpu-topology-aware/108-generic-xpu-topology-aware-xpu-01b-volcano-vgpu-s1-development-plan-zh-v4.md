# XPU-01B：以现有 Volcano vGPU 路径执行 scheduler-selected GPU UUID

> 决策日期：2026-10-01；探针状态更新：2026-10-10。本文是 [M3 计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)的 XPU-01B 执行后端补充，合同与代码顺序仍属待实施设计，hard Bind 未放行。
>
> [XPU-01 证据](./103-generic-xpu-topology-aware-xpu-01-l1-evidence-report-zh-v4.md)中的 stock NVIDIA Device Plugin 负例保持原样。现有 vGPU 路径的[单槽位与无锁并发基线](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/README.md)已运行：单 Pod GPU-B 注入通过，并发 A/B 实际交叉；[NodeLock 首轮](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/nodelock-enabled/README.md)及[后续三轮与槽位复用](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/nodelock-enabled/followup-20261010/README.md)只证明 mock 环境中的缓解和回收。尚未接入 xPU planner/API Pod assignment，也未解决 Allocate RPC 的 Pod 身份缺口。

## 1. 选择和适用范围

XPU-01B 的**首个执行 profile** 定为 `volcano-vgpu-s1`：NVIDIA 物理 GPU UUID 由 topology Provider 发现，xPU hard planner 为指定 Pod/ContainerRef 选择 `NodeUID + GPU UUID`，Volcano deviceshare 按这个选择记物理卡占用，现有 `volcano-vgpu-device-plugin` 的 `Allocate()` 路径把相同物理 UUID 注入容器。当前不以 fork stock `k8s-device-plugin` 作为首个实现步骤；其 `nvidia.com/gpu` exact-ID 能力保持独立的后续研究。

该 profile 使用 `volcano.sh/vgpu-number` 作为 Pod 目标扩展资源与 `DeviceTopologyPolicy.resourceName`，每个受支持的物理 GPU 在插件配置与 node config 中均设 `deviceSplitCount=1`，目标容器申请正整数 `volcano.sh/vgpu-number`。首轮只验证一节点两卡、请求 1 卡且明确选择非默认 GPU-B；再扩展多 Pod、跨 wave 和故障场景。catalog、topology facts、Provider identity、assignment envelope 中的 `resourceName` 必须一致。`provider` 仍表示发现物理 UUID 的受信 NVIDIA Provider，不能把资源名、ProviderID 或 kubelet 虚拟槽位 ID 当作同一个字段。

`deviceSplitCount=1` 是单槽位必要配置，不自动证明独占整卡、显存/算力不共享或 GPU runtime 隔离。XPU-01B 必须核验 vGPU memory/core 配置、普通 NVIDIA 插件并存、同一物理卡的其他资源池/分配路径，并在无法证明独占时保持 profile 不可用。此处复用 vGPU **执行后端**，不开放任意 split、fractional、MIG 或通用 vGPU workload API。普通 `nvidia.com/gpu` Pod 不得与此 profile 对同一物理卡形成不可见的混用。

当前 `ExtractResourceRequest` 在未请求 cores 时取 `Coresreq=0`，vGPU scheduler 的 `Coresreq=100` 才有显式独占分支；
因此单槽位不能凭 `vgpu-number: 1` 宣称算力独占。首个 profile 的 `vgpu-cores`/memory 请求与插件侧限制须在实现时冻结并验证；
在此之前只把“单物理 UUID 注入”作为待验证目标，不声明整卡隔离。

## 2. 两套 ID、两套账

| 对象 | 含义和权威来源 | XPU-01B 校验 |
| --- | --- | --- |
| xPU `DeviceKey` | `NodeUID/物理 GPU UUID`，planner 的选卡结果 | NodeUID、topology、health、目标资源与可用性 |
| API Pod `volcano.sh/xpu-assignment` | scheduler 持久化的 Pod/ContainerRef/ResourceName/Provider/DeviceKeys；跨 Session 的 anchor 输入 | Bind 前 API Pod 重读，绑定后按相同 envelope 恢复 |
| vGPU `vgpu-ids-new` / `devices-to-allocate` | deviceshare 物理卡占用和插件请求接缝；后者是消费过程中的临时状态 | 必须与所选 UUID 对应，不能作为 xPU 唯一恢复权威 |
| kubelet `DevicesIds` / PodResources | `volcano.sh/vgpu-number` 的虚拟槽位 ID，例如 `GPU-B-0`；kubelet 负责其分配/checkpoint | 记录实际 ID、数量、Pod/Container/资源和生命周期；不要求字符串等于物理 UUID |
| 插件注入与 runtime | `Allocate()` 给出的 `NVIDIA_VISIBLE_DEVICES` 等物理 UUID；真实 runtime 读回属 L2 | L1 比对注入 UUID；L2 比对实际可见 UUID |

因此废止对本 profile 使用 `selected DeviceKey == kubelet DevicesIds` 的字面等式。物理身份的不变量是：

```text
planner selected physical UUID
  == API Pod xpu-assignment physical UUID
  == deviceshare committed physical UUID
  == vGPU Allocate injected physical UUID
  [L2] == container runtime visible physical UUID
```

数量不变量另行核对：**每个 PodUID + ContainerRef + `volcano.sh/vgpu-number`** 的 selected/持久化物理卡数、deviceshare 已确认分配数、kubelet 实际虚拟槽位数一致；按 Node/资源汇总的占用与健康槽位容量也必须一致。试分配、Bind 失败、释放与重启期间要定义各自状态和收敛时点，不以瞬时计数相等代替生命周期证明。计数相等只能证明容量账的必要条件，不能证明同一 Pod 获得了 GPU-B；同一 Pod 的注入 UUID 必须单独核对。一个物理 UUID 在该独占 profile 下不得同时被两个活跃分配占用。

在没有 in-flight 分配的稳态，节点对账应满足 `deviceshare 剩余物理卡数 == kubelet 健康虚拟槽位数 - kubelet 已分配虚拟槽位数`；
两侧必须限定到同一受管 GPU 集合，并显式排除/报告健康变化和 pending 释放。kubelet 的 `Allocatable` 总数本身不是剩余数。

## 3. 已知代码接缝与待证明事项

- `pkg/scheduler/api/devices/nvidia/vgpu/device_info.go` 的 `GPUDevices.Allocate` 已选择物理 UUID，生成 `volcano.sh/vgpu-ids-new` 和 `volcano.sh/devices-to-allocate`，并维护 `PodMap`；`AddResource`/`Release` 负责恢复和释放。当前选卡依赖 vGPU 规则/`volcano.sh/vgpu-use-gpuuuid`，还没有消费 xPU winner 的 `DeviceKey`。
- `pkg/scheduler/plugins/predicates/predicates.go` 在 speculative `Statement.Allocate` 期间触发 deviceshare 分配；XPU-09 的 winning Statement 重算 xPU plan 发生在 trial/winner 阶段之后。必须解决顺序与回滚，不能让 trial 的 vGPU 选择抢先成为最终物理卡，也不能在 trial 写 API Pod。
- `pkg/scheduler/api/devices/util.go` 的 `ExtractResourceRequest`、vGPU `checkVGPUResourcesInPod` 和 `resourcereqs` 目前只遍历普通 `spec.containers[]`；即使 xPU codec 接受 init/restartable-init，现有 deviceshare 分配路径也不能据此宣称支持它们。
- `volcano-vgpu-device-plugin/pkg/plugin/server.go` 对外通告 `GPU-UUID-slot` 虚拟 ID；`Allocate()` 当前主要核对 `DevicesIds` 数量，读取 vGPU 私有 annotation 后用物理 UUID 生成注入结果。`GetPreferredAllocation` 不能替代 scheduler 到插件的确定性交接。
- `volcano-vgpu-device-plugin/pkg/util/util.go` 当前按 Node/请求数量寻找 pending Pod，且 `GetNextDeviceRequest` 只遍历普通 `spec.containers[]`。标准 Device Plugin `Allocate` RPC 不带 PodUID；并发、同数量 Pod、多容器、init/restartable-init 的对应关系必须另有可证明机制。无法唯一匹配时 fail closed，不能猜最早 Pod；**即使当前仅有一个 pending Pod，也不能由此证明该 RPC 属于它**。NodeLock 超时/释放后仍可能收到旧 Pod 的迟到或重试 RPC。
- 当前 `pkg/scheduler/topology/provider/annotation.go` 将首个 Provider 的 `ResourceName` 固定为 `nvidia.com/gpu`；XPU-01B 需要让可信 facts/catalog 对应 `volcano.sh/vgpu-number`，同时保留 NVIDIA 物理身份命名空间 `nvidia.com`，不能仅改 Pod 示例资源名。
- PR9 已有 canonical assignment codec、capability contract 和 API Pod writer PoC；PR11 已有纯 hard planner 与 winning Statement 可行性检查，尚无 production assignment preparation、vGPU selected-key handoff 或 hard Bind 放行。现有 `ConfirmKubeletDeviceIDs`/`ExactReady` 假设需要按本 profile 的物理身份与虚拟槽位分账修订。

## 4. 建议代码修改顺序（本次不实施）

| 顺序 | 代码落点和修改 | 完成证据 |
| --- | --- | --- |
| 1. profile 与身份 | `pkg/scheduler/topology/provider/annotation.go`、catalog/Provider/activation 与 assignment 校验：登记 `volcano-vgpu-s1`、`volcano.sh/vgpu-number`、物理 UUID DeviceKey 和单槽位/独占前提；保持 Provider 身份命名空间 `nvidia.com`，将 facts 的目标资源映射到 vGPU 扩展资源。capability 区分物理选卡、Pod 持久化、虚拟槽位计数、注入与 L2 runtime。原 `ConfirmKubeletDeviceIDs` 不再用于字面相等放行。 | gate 默认关闭；配置不一致、普通 GPU 混用或能力缺失时 `AssignmentContractReady=false` |
| 2. winner 到 deviceshare | `pkg/scheduler/actions/allocate`、`pkg/scheduler/plugins/predicates`、`pkg/scheduler/api/devices/{util.go,nvidia/vgpu}`：在 winner final plan 后，把选定的 `NodeUID/UUID + PodUID + ContainerRef + resource/count` 交给 deviceshare 精确校验/占用；避免 trial 分配泄漏或与 winner 改选冲突；冻结 cores/memory 独占形状，补齐受支持的 ContainerRef 或暂时 fail closed；保留现有 vGPU 物理卡账和回滚语义。 | 一节点两卡强制 GPU-B；trial、Discard/Recover、竞争占用都不能改成 GPU-A |
| 3. 持久化和恢复 | `pkg/scheduler/cache/xpu_assignment_writer.go`、Session final preparation/recovery：在 Bind 入队前按 UID/resourceVersion 写入 API Pod canonical assignment，并使 vGPU 私有分配信息与其一致；重启只从已绑定 Pod assignment 建 anchor，私有 `devices-to-allocate` 不能代替它。 | API Pod GET、Bind 后重读、跨 Session/wave、写入冲突和部分 Bind 失败均可复现 |
| 4. Pod-aware 接缝与注入 | 先确定能被 kubelet/容器运行时验证的 PodUID + ContainerRef 来源，再在 `volcano-vgpu-device-plugin/pkg/plugin/server.go`、`pkg/util/util.go` 或替代 node/runtime backend 中逐项核对 canonical assignment、私有 annotation、资源数量和目标 UUID；覆盖合同允许的 regular/init/restartable-init，或在支持前使这些形状保持 Pending。当前标准 `Allocate` RPC 无 PodUID；只改 oldest 匹配或打开 NodeLock 不满足此项。 | 同数量并发、迟到 RPC/重试、锁超时、插件/调度器重启、多容器/init、伪造/丢失/冲突 annotation 均 fail closed；容器获得 GPU-B |
| 5. 最终放行与回归 | xPU final guard、`Statement`/`Session`/backfill/nomination/Bind 入口：仅对已通过 profile 能力和当前分配确认的 hard Task 放行；释放/Node replacement/健康变化重新对账；普通 workload 与 M2 soft 保持原行为。 | 无 assignment、账目不一致或 bridge 不可确认时 `XPUAssignmentNotEnforceable` 且无 Bind |

这些是可拆成小 PR 的建议顺序，不改动 PR9/10/11 的历史完成结论。第 2～4 步可用 Fake 和 mock 做开发，但真实 profile 的 `AssignmentContractReady` 必须等待以下 L1 证据。

## 5. L1/L2 探针与放行门

首个 L1 场景固定为一节点两张可区分的健康物理 GPU，显式选非默认 GPU-B，`deviceSplitCount=1` 在插件和 node config 两处一致，且确认目标资源只由 vGPU 插件注册。每次运行记录：

1. PodUID、ContainerRef、NodeName/NodeUID、目标 ResourceName、ProviderID、GPU-A/GPU-B inventory、profile 配置和组件版本；
2. planner/winner assignment 与 API Pod GET 的 canonical `assignments[]`；
3. deviceshare 的物理 UUID 选择、`vgpu-ids-new`、插件实际消费的 `devices-to-allocate`；
4. kubelet PodResources/checkpoint 中虚拟槽位的 Pod/Container/资源归属及数量；能采到时另记原始 `Allocate.DevicesIds` 用于排查调用时序，它与物理 UUID 的字面相等及原始 RPC 日志本身都不是放行条件；
5. 插件 `Allocate` 响应注入的 `NVIDIA_VISIBLE_DEVICES` 和失败/重试日志；
6. GPU-B 占用、GPU-A 空闲、多 Pod 竞争、同数量并发、重启、删除/释放、NodeUID replacement 和歧义匹配的负例。

L1 只有在**同一 Pod/Container 的物理 UUID 链相等、虚拟槽位数量账成立、错误路径 fail closed、API Pod 持久化与恢复成立**时通过；不能凭 kubelet 槽位数量或已有 vGPU 用户 allowlist 案例单独通过。L2 再在真实 NVIDIA runtime 中读取可见 UUID 并与 GPU-B 对照；L1 mock 通过不等于 L2 已运行，也不证明 Fabric/性能。

在上述代码和证据完成前，M3 hard 继续 `XPUAssignmentNotEnforceable` / no-Bind。若标准 Device Plugin RPC 缺少 Pod 身份导致无法证明同数量并发 Pod 的正确关联，则该 profile 不能标记 exact-ready；需要收紧可运行并发范围或重新设计关联接缝并补证据，而不是用数量相等放行。

## 6. NodeLock 后续探针与 Pod 身份门（2026-10-10）

[无锁基线](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/README.md)的 A/B 同数量并发实际交叉。打开 `deviceshare.NodeLockEnable=true` 后，[首轮](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/nodelock-enabled/README.md)与[后续三轮](../../../evidence/xpu-01b/l1-20261009-kind-vgpu-s1/nodelock-enabled/followup-20261010/README.md)均未复现：每轮两 Pod 最终 Running，A/B 各自的 deviceshare assignment、插件 `Allocate Response` 注入 UUID、响应 mount 中 PodUID、kubelet checkpoint `AllocResp` 所属 PodUID 和容器 `NVIDIA_VISIBLE_DEVICES` 一致。每轮出现一次节点锁冲突及约一秒后重试。删除后的 checkpoint 旧 UID 在下一次正常分配时消失，单 Pod 可以复用槽位；因此前次因残留而停止第二轮是过于保守的判断。该结论只覆盖这四轮 mock 运行，不覆盖锁超时、迟到 RPC、重启或真卡。

物理卡与虚拟槽位分账已实测：指定物理 GPU-B 的单 Pod 正常得到 GPU-B 注入，kubelet PodResources 记为 `GPU-A-0`；后续三轮 A/B 的槽位前缀也与各自物理 assignment 相反。这是本 profile **允许的正常结果，不是身份门失败**。槽位只核对 Pod/Container 归属、数量与生命周期，不以其 GPU UUID 前缀反查目标 Pod。原始 `AllocateRequest.DevicesIds` 未直接采到；它可用于排查 RPC 时序，但不是物理卡字面一致或 exact-ready 的独立放行条件。

**真正的身份门是：插件为选中物理 UUID 生成的响应必须交给 API assignment 指定的 PodUID + ContainerRef。** 标准 Device Plugin [v1beta1 `AllocateRequest`](https://github.com/kubernetes/kubelet/blob/master/pkg/apis/deviceplugin/v1beta1/api.proto)只包含各容器 `devices_ids`，不带 PodUID/ContainerRef；本插件 [`GetPendingPod`](https://github.com/Project-HAMi/volcano-vgpu-device-plugin/blob/main/pkg/util/util.go)又以 Node、数量和最早时间选 Pod，`Allocate()` 按数量核对后使用该 Pod 注解的物理 UUID。Volcano 的 [NodeLock](../../../pkg/scheduler/plugins/util/nodelock/nodelock.go)是 Node 注解，有五分钟过期逻辑；插件成功/失败会释放锁。**以下是协议与源码推导的风险，尚未做迟到 RPC 故障注入：**即使锁让任一时刻只有一个 pending 注解，旧 Pod 的 RPC 仍可能在锁过期或释放后抵达，此时插件看见的是新 Pod 的唯一注解。两个同数量请求以及允许跨物理 UUID 的虚拟槽位无法在 RPC 处提供目标 Pod 的可靠区分。把“候选数超过一”改为拒绝是有用的局部防护；它不能证明“候选数等于一”一定正确。插件/调度器重启后的候选重建、RPC 重试和多容器也需同样处理。

| 工程路线 | 现在能承诺的语义 | 达到 exact UUID 所需的最小新增证据 |
| --- | --- | --- |
| 保留当前 vGPU Device Plugin + NodeLock | 作为现有 vGPU 路径的并发缓解，供独立的 best-effort/advisory 工作负载或 XPU-01B 开发探针使用；**不能**据此把 hard `AssignmentContractReady` 置 true | 需要一个可信的 PodUID + ContainerRef 到 kubelet 请求/容器的接缝；只加锁、按数量猜 Pod、按虚拟 ID 前缀匹配、事后计数对账均不足 |
| DRA ResourceClaim + node driver，作为未来执行后端 | 更换资源申请与 node prepare 接缝，不是现有 vGPU Device Plugin 原地具备的能力。[Kubernetes v1.33 DRA 文档](https://v1-33.docs.kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)描述 Pod/Claim 的分配与调度；[DRA KEP](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/4381-dra-structured-parameters/README.md)中的 `NodePrepareResources` 请求携带 Claim namespace/name/UID | 用每 Pod 独立 Claim 或验证 Claim `reservedFor`/PodUID，按 claim request 映射 ContainerRef 与选中物理 UUID；node driver 以 Claim UID 幂等 prepare/unprepare，返回 CDI 设备；验证两个同数量 Pod、重试、迟到 prepare、Claim 删除/重建、driver 重启及 PodResources/运行时 UUID |
| 可信 runtime/NRI 拦截层，保留现有标量资源申请 | 可能在容器创建时通过受信 Pod/Container 元数据核对最终注入并拒绝错配；这是**候选方案**，本仓库未实现/验证 | 必须证明拦截点确实拿到不可伪造的 PodUID/ContainerRef、能读取 API Pod canonical assignment 与真实注入/设备访问，并在不一致时阻止容器启动；还要处理插件已消费注解、锁已释放后的回滚/重试和重启恢复，不能只查环境变量 |
| kubelet 联动的 Pod-aware Allocate 协议 | 理论上可把 PodUID + ContainerRef 随调用传给受信插件，但涉及 kubelet/协议或等价的受信桥，维护成本和部署边界更大 | 同一 RPC 的调用者身份、PodUID、ContainerRef、虚拟 IDs 和 canonical assignment 可校验；重试幂等、旧 UID/epoch 拒绝、锁超时和 kubelet/plugin 重启 fail closed；明确版本兼容及升级路径 |

身份接缝一旦选定，验证对象应为 `(NodeUID, PodUID, ContainerRef, ResourceName, assignment digest/epoch)`：节点侧从**受信调用上下文**取得目标身份，重读 API Pod 或 Claim 的持久 assignment，核对 selected UUID、资源/数量和当前 PodUID/NodeUID；同一 epoch 的重试只能重放同一结果，旧 epoch、消失 Pod、冲突/歧义一律拒绝，并对准备、消费、回滚和释放做幂等账。scheduler 的 NodeLock 可继续减少正常争用，但不充当身份凭据。直到这一条可测试的链存在，hard Bind 保持 no-Bind。

下一份小型实验建议只在隔离环境做诊断，不改变生产插件语义：以 `volcano-vgpu-device-plugin/pkg/plugin/server.go:Allocate`、`pkg/util/util.go:GetPendingPod` 为观测点，在临时诊断镜像记录请求虚拟 IDs、进入/退出时间、候选 PodUID/ContainerRef、响应 mount UID/物理 UUID；通过可控延迟制造旧 RPC 跨过 NodeLock 释放/超时后与新同数量 Pod 交叠。**验收**是原始 RPC 与 kubelet PodResources/响应归属能逐次对照，出现无法证明归属时容器不能获得另一 Pod 的 UUID，并覆盖插件重启与重试。若诊断确认仅靠当前 RPC 无法 fail closed，就转向上表的 Pod-aware 接缝；不要通过增加重复通过轮数打开 `AssignmentContractReady`。可独立实施的 Volcano 小 PR 是 profile 校验与能力 gate：检查 `deviceSplitCount=1` 两处、受管 GPU 集合、请求形状和 NodeLock 配置，默认仍返回 `XPUAssignmentNotEnforceable`；单元/集成测试证明缺配置时拒绝 hard Bind，配置齐备但 Pod-aware 接缝缺失时同样拒绝。
