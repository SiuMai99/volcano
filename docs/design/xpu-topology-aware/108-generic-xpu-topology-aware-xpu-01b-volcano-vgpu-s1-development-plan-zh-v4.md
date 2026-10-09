# XPU-01B：以现有 Volcano vGPU 路径执行 scheduler-selected GPU UUID

> 决策日期：2026-10-01。本文是 [M3 计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)的 XPU-01B 执行后端补充。本文只冻结拟采用的合同和代码修改顺序，不报告新探针结果，也不表示 hard Bind 已放行。
>
> [XPU-01 证据](./103-generic-xpu-topology-aware-xpu-01-l1-evidence-report-zh-v4.md)中的 stock NVIDIA Device Plugin 负例和既有 vGPU Adapter 结果保持原样；单槽位 `volcano-vgpu-s1` 尚未按该报告的配置完成运行。

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
- `volcano-vgpu-device-plugin/pkg/util/util.go` 当前按 Node/请求数量寻找 pending Pod，且 `GetNextDeviceRequest` 只遍历普通 `spec.containers[]`。标准 Device Plugin `Allocate` RPC 不带 PodUID；并发、同数量 Pod、多容器、init/restartable-init 的对应关系必须另有可证明机制。无法唯一匹配时 fail closed，不能猜最早 Pod。
- 当前 `pkg/scheduler/topology/provider/annotation.go` 将首个 Provider 的 `ResourceName` 固定为 `nvidia.com/gpu`；XPU-01B 需要让可信 facts/catalog 对应 `volcano.sh/vgpu-number`，同时保留 NVIDIA 物理身份命名空间 `nvidia.com`，不能仅改 Pod 示例资源名。
- PR9 已有 canonical assignment codec、capability contract 和 API Pod writer PoC；PR11 已有纯 hard planner 与 winning Statement 可行性检查，尚无 production assignment preparation、vGPU selected-key handoff 或 hard Bind 放行。现有 `ConfirmKubeletDeviceIDs`/`ExactReady` 假设需要按本 profile 的物理身份与虚拟槽位分账修订。

## 4. 建议代码修改顺序（本次不实施）

| 顺序 | 代码落点和修改 | 完成证据 |
| --- | --- | --- |
| 1. profile 与身份 | `pkg/scheduler/topology/provider/annotation.go`、catalog/Provider/activation 与 assignment 校验：登记 `volcano-vgpu-s1`、`volcano.sh/vgpu-number`、物理 UUID DeviceKey 和单槽位/独占前提；保持 Provider 身份命名空间 `nvidia.com`，将 facts 的目标资源映射到 vGPU 扩展资源。capability 区分物理选卡、Pod 持久化、虚拟槽位计数、注入与 L2 runtime。原 `ConfirmKubeletDeviceIDs` 不再用于字面相等放行。 | gate 默认关闭；配置不一致、普通 GPU 混用或能力缺失时 `AssignmentContractReady=false` |
| 2. winner 到 deviceshare | `pkg/scheduler/actions/allocate`、`pkg/scheduler/plugins/predicates`、`pkg/scheduler/api/devices/{util.go,nvidia/vgpu}`：在 winner final plan 后，把选定的 `NodeUID/UUID + PodUID + ContainerRef + resource/count` 交给 deviceshare 精确校验/占用；避免 trial 分配泄漏或与 winner 改选冲突；冻结 cores/memory 独占形状，补齐受支持的 ContainerRef 或暂时 fail closed；保留现有 vGPU 物理卡账和回滚语义。 | 一节点两卡强制 GPU-B；trial、Discard/Recover、竞争占用都不能改成 GPU-A |
| 3. 持久化和恢复 | `pkg/scheduler/cache/xpu_assignment_writer.go`、Session final preparation/recovery：在 Bind 入队前按 UID/resourceVersion 写入 API Pod canonical assignment，并使 vGPU 私有分配信息与其一致；重启只从已绑定 Pod assignment 建 anchor，私有 `devices-to-allocate` 不能代替它。 | API Pod GET、Bind 后重读、跨 Session/wave、写入冲突和部分 Bind 失败均可复现 |
| 4. 插件关联与注入 | `volcano-vgpu-device-plugin/pkg/plugin/server.go`、`pkg/util/util.go`：建立可验证的 Pod/Container 请求关联，逐项校验 canonical assignment、私有 annotation、资源数量和目标 UUID；覆盖合同允许的 regular/init/restartable-init，或在支持前使这些形状保持 Pending。RPC 无 PodUID 的限制必须用明确、可测试的关联协议处理；歧义立即报错。 | 同数量并发 Pod、多容器/init、伪造/丢失/冲突 annotation、插件重启负例；注入 UUID 为 GPU-B |
| 5. 最终放行与回归 | xPU final guard、`Statement`/`Session`/backfill/nomination/Bind 入口：仅对已通过 profile 能力和当前分配确认的 hard Task 放行；释放/Node replacement/健康变化重新对账；普通 workload 与 M2 soft 保持原行为。 | 无 assignment、账目不一致或 bridge 不可确认时 `XPUAssignmentNotEnforceable` 且无 Bind |

这些是可拆成小 PR 的建议顺序，不改动 PR9/10/11 的历史完成结论。第 2～4 步可用 Fake 和 mock 做开发，但真实 profile 的 `AssignmentContractReady` 必须等待以下 L1 证据。

## 5. L1/L2 探针与放行门

首个 L1 场景固定为一节点两张可区分的健康物理 GPU，显式选非默认 GPU-B，`deviceSplitCount=1` 在插件和 node config 两处一致，且确认目标资源只由 vGPU 插件注册。每次运行记录：

1. PodUID、ContainerRef、NodeName/NodeUID、目标 ResourceName、ProviderID、GPU-A/GPU-B inventory、profile 配置和组件版本；
2. planner/winner assignment 与 API Pod GET 的 canonical `assignments[]`；
3. deviceshare 的物理 UUID 选择、`vgpu-ids-new`、插件实际消费的 `devices-to-allocate`；
4. kubelet `Allocate.DevicesIds` 与 PodResources 的虚拟槽位 ID、Pod/Container/资源归属及数量；
5. 插件 `Allocate` 响应注入的 `NVIDIA_VISIBLE_DEVICES` 和失败/重试日志；
6. GPU-B 占用、GPU-A 空闲、多 Pod 竞争、同数量并发、重启、删除/释放、NodeUID replacement 和歧义匹配的负例。

L1 只有在**同一 Pod/Container 的物理 UUID 链相等、虚拟槽位数量账成立、错误路径 fail closed、API Pod 持久化与恢复成立**时通过；不能凭 kubelet 槽位数量或已有 vGPU 用户 allowlist 案例单独通过。L2 再在真实 NVIDIA runtime 中读取可见 UUID 并与 GPU-B 对照；L1 mock 通过不等于 L2 已运行，也不证明 Fabric/性能。

在上述代码和证据完成前，M3 hard 继续 `XPUAssignmentNotEnforceable` / no-Bind。若标准 Device Plugin RPC 缺少 Pod 身份导致无法证明同数量并发 Pod 的正确关联，则该 profile 不能标记 exact-ready；需要收紧可运行并发范围或重新设计关联接缝并补证据，而不是用数量相等放行。
