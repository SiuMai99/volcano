# Volcano 通用 xPU 拓扑感知调度 V4：XPU-01 NVIDIA Provider identity 探针实施计划

> 关联合同：[XPU-00 合同冻结记录](./101-generic-xpu-topology-aware-contract-review-zh-v4.md)。
>
> 关联计划：[V4 开发计划](./100-generic-xpu-topology-aware-development-plan-zh-v4.md)。
>
> 状态：**实施计划，尚未表示 XPU-01 已完成**。
> 本文只规划探针、证据和能力差距输出，不新增 scheduler plugin、CRD、跨系统 allocation owner 或多 Pod 事务层。
> 计划基线：2026-09-17；当前仓库尚无 V4 Provider、topology manager 或 `xpu-assignment` Go 实现。
>
> 范围修订（2026-09-20）：保留 stock NVIDIA Device Plugin 的兼容性负例，同时增加现有 Volcano vGPU/HAMi Adapter 的独立 L1 验证轨道。vGPU 轨道使用 `deviceSplitCount=1` 的单槽位实验配置，不改变 XPU-00 对 Alpha 不支持 GPU 虚拟化请求的原始边界。

## 1. 目标与完成边界

### 1.1 目标

XPU-01 是对 XPU-00 D4 的分层运行证据工作包，当前回答两个必须分开记录的问题：

> 1. stock NVIDIA Device Plugin 是否能消费或确认 scheduler-selected GPU UUID，并以 NodeUID-safe 的 canonical `DeviceKey` 保留到已绑定 Pod 的 `volcano.sh/xpu-assignment` 中？
> 2. 现有 Volcano vGPU/HAMi Adapter 是否能在单槽位配置下，把指定 UUID 从 vGPU allowlist 传递到 assignment annotation 和容器运行时注入？

问题 1 是 generic XPU contract 的 stock compatibility probe；问题 2 是现有 Adapter 的 L1 exact UUID realization probe。问题 2 不能反向证明问题 1 已经满足。

XPU-01 需要产出：

1. NVIDIA identity contract 的可执行输入、输出和失败语义；
2. `nvml-mock` 层的 inventory/topology/health conformance 证据；
3. selected UUID、canonical `DeviceKey`、assignment annotation 的传递和幂等性证据；
4. annotation 缺失、非法、重复、Node replacement 和 topology 冲突的失败证据；
5. 原生 NVIDIA Device Plugin 能力与缺口清单，以及 Volcano vGPU/HAMi Adapter 的独立能力结果；
6. 可选真实 NVIDIA runtime identity gate 的环境要求、日志和判定结果；
7. 对 D4、XPU-11、XPU-13/14 的影响和下一步建议。

### 1.2 不属于 XPU-01

- 不实现 `xpu-topology-aware` scheduler plugin、`GroupTopologyPlanFn` 或 `TopologyReserveFn`；
- 不修改 `Statement.Commit()`、`AddBindTask()`、PreBind 或 Kubernetes Bind 语义；
- 不把 `AdmissionSet`、Group anchor 或 assignment annotation 变成新的 CRD 或独立持久化状态；
- 不实现外部设备 reservation、release、reconciliation、active-active fencing 或多 Pod Bind 原子性；
- 不以 mock 通过证明 CUDA/NCCL 性能、真实 GPU 故障隔离或真实 Fabric 能力；
- 不把 HAMi 作为 NVIDIA 厂商身份，也不把 HAMi 的分配实现当成 NVIDIA Device Plugin exact-ID 证据；
- 不把 Volcano vGPU/HAMi Adapter 的 exact UUID 结果写成 stock NVIDIA Device Plugin 已支持；
- 不把 `deviceSplitCount=1` 或 `volcano.sh/vgpu-number=1` 写成 generic scheduler-selected UUID enforcement；
- 不在没有真实 NVIDIA 节点时伪造真实 runtime 结果。

XPU-01 可以在尚未实现 scheduler plugin 的情况下启动。scheduler-selected assignment 由 harness 生成或由后续适配层注入；这种输入只用于验证 Provider/Device Plugin 的消费、确认和保留能力，不代表 scheduler 已经完成选卡。

## 2. 冻结的测试合同

### 2.1 NVIDIA identity contract

XPU-01 默认使用以下身份组合。若探针发现该组合无法落地，必须记录为能力差距，不得自行换成另一种身份。

| 字段 | 固定值 | 验证含义 |
| --- | --- | --- |
| `Vendor` | `NVIDIA` | 设备厂商，不是调度器或上层分配器名称 |
| `ProviderID` | `nvidia-nvml-v1` | 本次事实/assignment contract 的 Provider 身份 |
| `Namespace` | `nvidia.com` | Kubernetes 资源命名空间 |
| `ResourceName` | `nvidia.com/gpu` | 目标扩展资源 |
| `DeviceID` | NVIDIA GPU UUID | Provider 归一化后的厂商设备身份 |
| `Discovery API` | NVML | mock 层的模拟接口和真实层的发现接口 |

三层身份必须分开记录：

```text
Vendor identity       = NVIDIA
Provider identity     = nvidia-nvml-v1
Scheduler DeviceKey   = OwnerNodeUID + normalized DeviceID
```

两个不同 Node 上出现同一个 `DeviceID` 可以是合法的；同一个 NodeUID 下出现两个相同的设备身份必须失败。`DeviceKey` 不能只使用 GPU index，也不能丢失 Owner NodeUID。

XPU-01 使用以下两个执行 profile。它们共享 NVIDIA UUID/NodeUID identity 规则，但不共享 exact-ID 能力结论：

| Profile | 资源路径 | 选择/约束输入 | 主要结果 | 结论边界 |
| --- | --- | --- | --- | --- |
| `stock-nvidia` | `nvidia.com/gpu` + NVIDIA Device Plugin | harness 生成的 scheduler-selected `DeviceKey`；原生接口只观察 kubelet 最终 `DevicesIds` | ListAndWatch、Allocate、PodResources 和 mismatch 证据 | 不能把 kubelet 自选 ID解释成 Volcano selected UUID；缺通道时为 `XPUAssignmentNotEnforceable` |
| `volcano-vgpu-s1` | `volcano.sh/vgpu-number`/`memory`/`cores` + `volcano-vgpu-device-plugin` | 单值 `volcano.sh/vgpu-use-gpuuuid` allowlist；Device Plugin/node config 的 `deviceSplitCount=1` | `vgpu-ids-new`、`devices-to-allocate`、`NVIDIA_VISIBLE_DEVICES` 三层 UUID 对账 | 证明现有 vGPU Adapter 的 exact UUID realization；allowlist 是用户输入，不等于 generic scheduler-owned `DeviceKey` |

两条 profile 都保持 `Vendor=NVIDIA`、`Discovery API=NVML` 和物理 GPU UUID 作为设备身份。`volcano-vgpu-device-plugin`/HAMi 是执行 Adapter，不是新的设备厂商。

### 2.2 最小 assignment payload

探针使用 XPU-00 冻结的最小字段，不增加未被消费者使用的 Domain、Fabric、Group 或 plan digest 字段：

```json
{
  "version": 1,
  "resourceName": "nvidia.com/gpu",
  "provider": "nvidia-nvml-v1",
  "deviceKeys": ["<node-uid>/GPU-aaaaaaaa"]
}
```

合同要求：

- `version`、`resourceName`、`provider`、`deviceKeys` 必须存在且类型正确；
- `deviceKeys` 必须 canonical、稳定、可校验，并包含 NodeUID-safe identity；
- 同一 Pod 的 assignment 重读和重新序列化不应改变语义或产生不同的 canonical key；
- Provider 只能校验或消费 scheduler-selected DeviceKeys，不能在 callback 中重新选卡或替换 plan；
- assignment annotation 在成功 Bind 后必须仍可从 Pod 读取；
- vGPU 私有的 `volcano.sh/vgpu-use-gpuuuid`、`volcano.sh/vgpu-ids-new` 和 `volcano.sh/devices-to-allocate` 可以作为 Adapter 证据保存，但不能替代 generic `volcano.sh/xpu-assignment`；
- Provider 无法消费或确认 selected DeviceKey 时，hard 路径的结论必须是 `XPUAssignmentNotEnforceable`，不能静默改为 soft。

### 2.3 证据等级

| 等级 | 环境 | 能证明 | 不能证明 |
| --- | --- | --- | --- |
| L0 | 纯 parser/canonicalizer/Fake | payload 形状、DeviceKey 规则、幂等性和失败语义 | Provider 或 kubelet 真实消费 |
| L1 | `nvml-mock + NVIDIA Device Plugin` | mock inventory、NVML 调用、ListAndWatch/Allocate 和注入链的可重复行为 | 原生 Device Plugin 已接受 scheduler-selected UUID；真实硬件 exact-ID |
| L1-vGPU | `nvml-mock + volcano-vgpu-device-plugin + Volcano deviceshare`，`deviceSplitCount=1` | vGPU UUID allowlist、`vgpu-ids-new`、`devices-to-allocate` 与 `NVIDIA_VISIBLE_DEVICES` 的可重复一致性 | stock NVIDIA Device Plugin exact-ID、generic scheduler-owned `DeviceKey` transport、真实硬件隔离/性能 |
| L2 | 真实 NVIDIA 节点、驱动、runtime、Device Plugin | 实际容器可见 UUID 与计划身份的对应关系 | 未运行的 Fabric、MIG/vGPU、性能和多 Pod 原子性 |

任何报告必须同时写明证据等级和未覆盖范围。L0/L1 通过不能标记为 L2 通过。

## 3. 实施路线

### 3.1 W0：建立可复现输入和环境清单

**目标**：先固定每次探针使用的身份、NodeUID、GPU UUID、拓扑和预期结果，避免测试过程中临时改写合同。

**工作**：

- 复用 XPU-00 fixture `nvidia-nvml-mock-selected-uuid-idempotent` 和 `nvidia-real-runtime-id-gate`；需要负例时直接引用已有 assignment/recovery fixture，不复制后改名；
- 为每次运行生成一个不可变 input manifest，至少包含 NodeName、NodeUID、ProviderID、ResourceName、mock GPU UUID、DeviceKey、topology generation 和 health；
- 固定一组至少两个 GPU 的 mock inventory，并选择一个非默认 UUID 作为 selected UUID；
- 记录测试环境版本：Volcano checkout、Go/toolchain、mock 实现、NVIDIA Device Plugin、driver/runtime、kernel/OS（若适用）；
- 记录哪些步骤是纯 harness、哪些步骤真正经过 Device Plugin、哪些步骤读取了容器 runtime；
- 如果缺少 L1 或 L2 环境，标记 `NotRun`，不以空日志或静态配置代替运行证据。
- 对 `volcano-vgpu-s1` profile，固定并记录：`deviceSplitCount=1`、node config 的 `devicesplitcount=1`、Pod `volcano.sh/vgpu-number=1`，以及所用的 `volcano.sh/vgpu-memory`/`volcano.sh/vgpu-cores`；这只是单槽位验证配置，不是 Alpha 的 vGPU 支持承诺。
- 启用 `volcano-vgpu-device-plugin` 前停止同一 device-plugin socket 上的普通 NVIDIA Device Plugin；记录 `volcano.sh/node-vgpu-register` 和 `volcano.sh/vgpu-*` allocatable 是否就绪，并保留 restore 入口。

**产物**：环境 README、input manifest、依赖版本清单、运行入口说明和证据目录约定。

**完成条件**：同一 manifest 可在同一环境重复运行；每个后续 case 都能回指唯一输入和证据目录。

### 3.2 W1：NVML mock inventory/topology/health conformance

**目标**：证明 mock 层提供的是可解释、稳定的 NVIDIA facts，而不是只返回一个数量。

**工作**：

- 枚举稳定 GPU UUID、型号、显存、PCI/NUMA 信息以及需要覆盖的 NVLink/NVSwitch 关系；
- 将 mock inventory 与 Node 的 `nvidia.com/gpu` 数量对账；数量不一致时不得继续进入 selected-ID 结论；
- 验证 NodeUID 变化会生成新的 DeviceKey owner，不会把旧 Node 的同名 GPU index 继承到新 Node；
- 验证 health 状态能区分 Healthy、不可调度或未知，不把未知状态当成可用；
- 验证 Node-local Domain 和显式 Fabric 的 membership 输入可以被稳定读取，但不宣称 mock Fabric 等于真实硬件 Fabric；
- 对跨 Node 相同 DeviceID、同 Node 重复 DeviceID、缺失 UUID、非法 UUID 做正反例。

**核心断言**：

```text
same NodeUID + same DeviceID      -> one canonical DeviceKey
different NodeUID + same DeviceID -> different canonical DeviceKeys
same NodeUID + duplicate DeviceID -> reject
missing/invalid health or UUID     -> not schedulable / explicit failure
```

**完成条件**：inventory、topology、health 和 Node resource count 的原始输出可保存；所有 ID 冲突和非法输入都有明确 reason。

### 3.3 W2：selected UUID 消费/确认探针

**目标**：直接验证 D4 的核心，而不是只验证 Provider 自己会枚举设备。

**工作分三条轨道**。

#### Track A：Provider/Adapter contract harness

- 由 harness 构造一个已确定的 selected UUID 和对应 canonical DeviceKey；
- 调用拟定的 Provider identity/assignment 校验边界，验证 ProviderID、Namespace、ResourceName 完全匹配；
- 将 selected DeviceKey 编码为最小 assignment payload，再解析、校验和重新编码；
- 验证 Provider 不能把 selected key 替换成默认 GPU、GPU index 或自己重新选择的 UUID；
- 模拟成功确认、未知设备、错误 NodeUID、错误 resource、错误 provider 和健康不可用；
- 对每次确认保存“输入 selected key、Provider 返回、最终 payload、reason”的完整记录。

Track A 只能证明 contract harness 和测试 double 的行为；它不能单独证明原生 NVIDIA Device Plugin 或真实 runtime 的执行。

#### Track B：NVIDIA Device Plugin compatibility probe

- 使用 `nvml-mock` 提供 driver root/inventory，验证 NVIDIA Device Plugin 的 ListAndWatch、Allocate 和 kubelet 注入链；
- 观察原生接口是否存在“接收指定 UUID、确认指定 UUID、并将其绑定到目标 Pod”的可验证入口；
- 如果只能得到可用设备列表、数量或 `GetPreferredAllocation` 结果，而无法确认 scheduler-selected UUID，必须记录 `XPUAssignmentNotEnforceable`；
- 不通过修改测试断言、选择默认 UUID 或把 Device Plugin 的数量分配结果解释成 exact-ID，来掩盖该协议缺口；
- 若需要自定义 Provider/Adapter bridge，先记录最小输入输出协议和责任边界，另立实现任务；XPU-01 只验证其可行性，不把桥接实现偷偷纳入探针。

**完成条件**：能力表明确区分“能枚举”“能分配”“能消费 selected UUID”“能确认 selected UUID”“能保留 Pod assignment”五种能力。

#### Track C：Volcano vGPU/HAMi Adapter exact UUID probe

该轨道验证已有 Volcano vGPU 路径，不修改 NVIDIA Device Plugin，也不把 vGPU 请求形状加入 Alpha API。测试配置固定为 `volcano-vgpu-device-plugin`
的 HAMI-core 模式、`deviceSplitCount=1`、Pod `volcano.sh/vgpu-number=1`，并使用至少两个物理 GPU、一个非默认目标 UUID。

执行链路和断言：

1. 在 Pod 上设置单值 `volcano.sh/vgpu-use-gpuuuid=<target-uuid>`；该值是当前 vGPU 案例的 allowlist 输入，不伪装成 generic scheduler-owned assignment；
2. 验证 Volcano deviceshare 只在目标 UUID 上完成 Filter/Allocate，不因槽位不足静默改选其他 UUID；
3. 验证 Pod 的 `volcano.sh/vgpu-ids-new` 和 `volcano.sh/devices-to-allocate` 均包含目标 UUID；
4. 验证 `volcano-vgpu-device-plugin` 的 Allocate/注入结果中 `NVIDIA_VISIBLE_DEVICES` 等于目标 UUID；
5. 验证目标 UUID 不存在、目标槽位已占用、annotation 被替换或注入 UUID 不一致时，结果为失败/Pending，不生成成功 exact 结论；
6. 单独记录 vGPU 私有 annotation 与 generic `volcano.sh/xpu-assignment` 的关系：前者是 Adapter 事实，后者只有在 scheduler-owned assignment 真正接入并持久化后才能作为 XPU recovery 事实。

`deviceSplitCount=1` 只消除同一物理卡的多槽位共享变量；它不能单独证明 exact UUID。Track C 的正向结果应命名为
`VolcanoVGPUEndpointExactUUID` 或等价的 Adapter capability，不命名为 `stock Device Plugin scheduler-selected UUID exact enforcement`。

**完成条件**：单槽位配置在 Device Plugin/node config 两处一致；目标 UUID 在 `vgpu-ids-new`、`devices-to-allocate` 和
`NVIDIA_VISIBLE_DEVICES` 三处一致；目标不存在、槽位占用和 UUID 替换负例均不允许静默成功。

### 3.4 W3：assignment annotation canonicalization、持久化与幂等性

**目标**：证明 assignment 是可恢复的 Pod 事实，而不是一次性日志或临时内存值。

**工作**：

- 用非默认 selected UUID 生成 `volcano.sh/xpu-assignment` payload；
- 验证 annotation key、JSON 字段、数组内容和 NodeUID-safe DeviceKey；
- 用不同字段顺序、DeviceKey 输入顺序和重复读取路径测试 canonicalization；
- 验证同一 Pod 多次读取、解析和重序列化得到相同语义；
- 在 Fake/API-server 层验证 annotation 随 Pod metadata 写入并可在 Bind 后读取；
- 对 vGPU profile 额外验证 `vgpu-ids-new` 等私有 annotation 的 Bind 后持久化；若只通过 Binding metadata 观察到而 API Pod 重读不到，记录为持久化缺口，不能升级为 generic assignment recovery；
- 记录 scheduler-owned/provider-owned/user-owned 的边界；用户输入的同名 annotation 不能被当成已绑定 assignment 事实；
- 不将 Domain/Fabric/Group 派生摘要写入最小 payload，也不以 PodGroup status 或日志替代 Pod assignment。

**完成条件**：`xpu-assignment-annotation-persisted` 与 `nvidia-nvml-mock-selected-uuid-idempotent` 的断言均有可追踪证据；任何无法持久化或重读不一致都标记为失败。

### 3.5 W4：recovery 和负例故障注入

**目标**：验证探针在错误数据下 fail closed，不制造错误 anchor。

| 场景 | 预期结果 | 禁止行为 |
| --- | --- | --- |
| assignment 缺失 | 不建立恢复 anchor；hard Pending | 从 GPU index、数量或默认 UUID 猜测 |
| JSON 格式非法 | 拒绝解析；记录明确 reason | 忽略字段错误继续 Bind |
| `provider` 或 `resourceName` 不匹配 | identity contract mismatch | 按资源数量继续确认 |
| `deviceKeys` 为空、重复或非法 | assignment invalid | 去重后静默通过 |
| DeviceKey 不属于当前 NodeUID | recovery Pending | 仅凭 NodeName 复用 |
| Node 同名替换、NodeUID 改变 | 旧 assignment 不恢复 | 继承旧 Node 的 anchor |
| 当前 topology 无法映射 Domain/Fabric | Group Pending | 选择第二个 Domain/Fabric |
| 已绑定成员之间 anchor 冲突 | Group Pending | 覆盖旧成员或新建第二个 anchor |
| Provider 无法确认 selected UUID | `XPUAssignmentNotEnforceable` | hard 静默降级为 soft |

每个负例都必须保存：输入 Pod/Node/topology、原始 annotation、解析错误或 Provider reason、是否生成 anchor、是否允许 Bind。

**完成条件**：负例不会产生错误 assignment、错误恢复 anchor 或成功 Bind；reason 能区分 identity 不可执行、数据非法和 topology 不可恢复。

### 3.6 W5：真实 NVIDIA runtime identity gate（可选）

**目标**：在真实 NVIDIA 节点上验证实际容器可见设备身份，而不是把 mock 结论升级为硬件结论。

**前置条件**：

- 可用的 NVIDIA GPU 节点、驱动、NVML、container runtime 和 NVIDIA Device Plugin；
- 能记录 NodeUID、PodUID、ContainerName、ResourceName、plan DeviceIDs 和 Pod assignment；
- 有权限从目标容器内通过 NVML 或等价的 NVIDIA runtime 查询实际 GPU UUID；
- 运行环境和版本可复现，并能保存脱敏后的原始结果。

**工作**：

- 使用非默认 selected UUID，部署目标 workload，并保存 Pod assignment；
- 从目标容器实际读取可见 GPU UUID，不以 `CUDA_VISIBLE_DEVICES` 字符串或 host inventory 单独代替 runtime 证据；
- 比对 `PodUID + ContainerName + ResourceName + NodeUID + DeviceIDs` 与实际可见 UUID；
- 若声明 Fabric，再单独比对真实 NVLink/NVSwitch membership；不能从普通网络连通性推导 Fabric；
- 将“assignment annotation 与 runtime 一致”“Device Plugin 注入成功”“真实 exact-ID 被执行”分别记录，不能合并成一个通过项；
- 真实环境未运行时把该 gate 标记为 `NotRun`，不阻塞 mock conformance，但不能写入“真实硬件已验收”。

**完成条件**：真实 UUID 对账结果可追溯；任一 selected UUID 无法与 runtime 可见 UUID 对齐时，真实 gate 失败，D4 不得标记为 exact-ID 已满足。

### 3.7 W6：报告、D4 反馈和评审

**目标**：把探针结果转成 XPU-00 可以引用的评审证据，而不是只提交一组日志。

**工作**：

- 生成按 L0/L1/L2 分层的 Provider capability matrix；
- capability matrix 必须分列 `stock-nvidia` 与 `volcano-vgpu-s1`：前者的 exact-ID 缺口不能被后者的 Adapter 通过结果覆盖；
- 列出每个 case 的输入、环境、结果、reason、证据路径和复现入口；
- 单独列出原生 Device Plugin 的协议缺口和是否需要 Provider/Adapter bridge；
- 更新 D4 的证据链接：可以单独报告 `D4-vGPU Adapter evidence`，但 generic scheduler-owned assignment 和 native Device Plugin exact-ID 仍按各自结果处理；
- 把原生 NVIDIA Device Plugin 的定制修改列为后续 `XPU-01B` bridge 计划，不在当前 vGPU 轨道中实现；
- 明确哪些结果可以进入 Alpha 能力声明，哪些只能保留为 mock 或后续发布证据；
- 提交 API/scheduler/framework/runtime reviewer 评审，不在报告中自行宣布 XPU-00 已冻结。

**完成条件**：reviewer 能仅凭报告回答 selected UUID 是否可消费/确认、annotation 是否可恢复、真实 runtime 是否已验证，以及失败时 hard workload 应如何处理。

## 4. Case 与断言矩阵

XPU-00 已定义的稳定 fixture ID 继续作为跨工作包引用。以下是 XPU-01 的执行映射；`XPU01-*` 是本计划的执行编号，不替换既有 fixture ID。

| 执行编号 | 引用 fixture | 层级 | 主要断言 | 结果影响 |
| --- | --- | --- | --- | --- |
| `XPU01-M01` | `nvidia-nvml-mock-selected-uuid-idempotent` | L0/L1 | 非默认 UUID、DeviceKey 和 assignment 可稳定生成和重读 | 核心 conformance |
| `XPU01-M02` | `nvidia-nvml-mock-selected-uuid-idempotent` | L1 | Device Plugin inventory/ListAndWatch/Allocate 链路可运行 | 只证明 plugin 基础链路 |
| `XPU01-V01` | `volcano-vgpu-device-plugin` 既有案例 | L1-vGPU | `deviceSplitCount=1`、`vgpu-number=1` 下，UUID allowlist 到 `vgpu-ids-new`/`devices-to-allocate`/`NVIDIA_VISIBLE_DEVICES` 一致 | Volcano vGPU Adapter exact UUID evidence |
| `XPU01-V02` | `volcano-vgpu-device-plugin` 既有案例 | L1-vGPU | 目标 UUID 不存在、槽位已占用或注入 UUID 改变时 fail closed | 不允许静默改选 |
| `XPU01-M03` | `xpu-assignment-annotation-persisted` | L0/L1 | annotation 字段、canonicalization、Bind 后可读 | D4/XPU-11 |
| `XPU01-M04` | `bound-pod-anchor-recovery` | L0/L1 | NodeName + assignment 能映射回相同 identity | D4/XPU-11 |
| `XPU01-N01` | `xpu-assignment-missing-pending` | L0/L1 | 缺失 assignment 不建立 anchor，hard Pending | fail closed |
| `XPU01-N02` | `xpu-assignment-conflict-pending` | L0/L1 | 冲突 assignment 不覆盖事实，不选第二实例 | fail closed |
| `XPU01-N03` | `xpu-node-replacement-rejected` | L0/L1 | NodeUID 变化拒绝旧 assignment 恢复 | identity safety |
| `XPU01-N04` | `node-domain-fragmented-6-plus-2` | L0/L1 | topology 映射/容量失败不伪造完整 Domain | topology safety |
| `XPU01-R01` | `nvidia-real-runtime-id-gate` | L2 | 容器实际 UUID 与 plan/assignment 完全对齐 | 真实 exact-ID gate |
| `XPU01-R02` | `nvidia-real-runtime-id-gate` | L2，可选 | 声明的真实 Fabric membership 可对账 | 只能进入发布验收证据 |

`XPU01-V01/V02` 的 `Pass` 只表示 Volcano vGPU Adapter 的 L1 证据通过；不改变 `XPU01-M02` 对 stock NVIDIA Device Plugin
selected UUID exact enforcement 的 `Blocked`/`XPUAssignmentNotEnforceable` 结论。

### 4.1 统一结果枚举

每个 case 只能使用以下结果之一，并附带证据路径：

| 结果 | 含义 |
| --- | --- |
| `Pass` | 当前证据等级下所有断言通过 |
| `Fail` | 断言被反例或运行结果明确违反 |
| `NotRun` | 环境、权限或依赖缺失，不能推断通过或失败 |
| `NotApplicable` | 经评审确认该 case 不属于当前 provider/环境 |
| `Blocked` | 探针发现前置协议缺口，无法继续验证后续链路 |

`NotRun` 不得写成 `Pass`；`Blocked` 必须写出阻塞的接口、组件和最小修复方向。

## 5. 产物、建议落点与审计要求

以下是建议落点，不改变 XPU-00 对 fixture 文件路径“由实现阶段决定”的约束：

```text
docs/design/xpu-topology-aware/
  102-generic-xpu-topology-aware-xpu-01-provider-identity-probe-plan-zh-v4.md
  testdata/xpu-00-contract-fixtures.yaml       # 继续复用稳定 fixture ID
  testdata/xpu-01-nvidia/                      # 建议：输入和最小 mock 场景

tools/ 或 hack/                                # 建议：可重放 harness，不提前冻结包名
evidence/xpu-01/                               # 建议：运行 manifest、日志、能力表

外部 vGPU Adapter 复现入口（当前不纳入本仓库实现）：

```text
/Users/oujunwei/Documents/hami/volcano-hami-vgpu/
  README.md
  scripts/setup.sh
  scripts/run-case.sh
  scripts/restore.sh
  manifests/volcano-vgpu-device-plugin-nvml-mock.yaml
  manifests/workload-specific-h100.yaml
```

若执行 `volcano-vgpu-s1` profile，必须在 Device Plugin ConfigMap 与 node config 两处把槽数统一为 `1`，并在证据中记录普通
NVIDIA Device Plugin 已停止、vGPU socket 已接管以及 restore 结果。
```

实施提交至少应包含：

1. 可重放的 harness 或明确的外部环境入口；
2. 不依赖 wall-clock、随机 GPU index 或未记录默认值的输入 manifest；
3. 原始结果与结构化断言结果；
4. case ID 到日志/测试/报告的映射；
5. Provider capability matrix 和 capability gap list；
6. 真实硬件未运行时的 `NotRun` 证据；
7. 失败时的 `XPUAssignmentNotEnforceable`、identity mismatch、topology not ready 等 reason 记录。

日志与 metrics 的审计边界：

- 可以在受控证据文件中记录完整 DeviceKey/UUID，以便精确对账；
- 对外 metrics/event 不使用 raw DeviceID、PodUID 等高基数或敏感身份；
- 日志不能成为恢复事实源，Pod assignment 才是后续 Alpha 恢复输入；
- 删除或脱敏原始日志时，必须保留结构化断言和 hash/引用关系，确保报告仍可审计。

## 6. 时间、角色与依赖

### 6.1 角色分工

| 角色 | 责任 | 不负责 |
| --- | --- | --- |
| R：Provider/runtime | harness、NVIDIA identity、stock/vGPU Device Plugin runtime 探针、能力表 | scheduler 选卡、Group planner、Statement 提交 |
| S：scheduler/framework | 确认 selected DeviceKey、Bind annotation 和 recovery 输入的合同边界 | 在 XPU-01 中实现完整 scheduler plugin |
| Q：测试/发布 | 环境矩阵、证据归档、重复运行和报告校验 | 把 mock 结果升级成真实硬件结论 |
| A：API/controller | 只在 annotation/API 边界需要时评审字段和 mutation 影响 | 改动 XPU-01 的 Provider identity |

### 6.2 参考排期

在已有 mock/runtime 环境可用的前提下：

| 阶段 | 参考耗时 | 主要出口 |
| --- | --- | --- |
| W0 输入和环境 | 0.5 天 | manifest、版本和复现入口 |
| W1 mock facts | 0.5～1 天 | inventory/topology/health conformance |
| W2 selected UUID | 1 天 | Provider/Device Plugin 能力差距 |
| W3 annotation/recovery | 0.5～1 天 | canonical、持久化和负例证据 |
| W5 真实 runtime | 0～1 天 | 可选 L2 identity gate |
| W6 报告评审 | 0.5 天 | capability matrix、D4 反馈 |

mock conformance 参考为 3～4 工程日；包含真实 runtime gate 时参考为 4～5 工程日。硬件申请、驱动安装、厂商协议开发和等待 reviewer 不计入该估算；环境不可用时不得压缩负例和报告工作来追赶时间。

### 6.3 依赖和阻塞处理

| 依赖/风险 | 先取得的证据 | 处理方式 |
| --- | --- | --- |
| 原生 Device Plugin 无 selected UUID 输入通道 | Track B compatibility probe | 记录 `XPUAssignmentNotEnforceable`，另立 bridge 设计；不伪造 exact-ID 通过 |
| vGPU profile 与普通 Device Plugin 共用 socket | Track C 环境清单 | 先停止普通 NVIDIA Device Plugin，记录切换和 restore；不允许两个插件同时注册同一资源/socket |
| vGPU `deviceSplitCount` 与 node config 不一致 | Track C W0 配置检查 | 阻断 VGPU case，统一两处为 `1` 后再运行 |
| vGPU annotation 只有 Binding metadata、API Pod 重读不到 | Track C/W3 持久化检查 | 只保留为 Adapter 注入证据，不能作为 generic `xpu-assignment` recovery 事实 |
| mock inventory 与 Node 资源数量不一致 | W1 对账结果 | 阻断后续 selected-ID 结论，修正 facts 或输入 |
| annotation 无法随 Bind 保留 | W3 API/Fake 结果 | 标记 D4/XPU-11 缺口，相关 hard workload 保持 Pending |
| NodeUID 或 DeviceKey 不能稳定恢复 | W3/W4 recovery 结果 | 禁止使用 NodeName/index 猜测，补 identity contract |
| 真实 NVIDIA 节点不可用 | W5 环境清单 | L2 标记 `NotRun`；不阻塞 L0/L1，但不声明真实硬件验收 |
| 真实 Fabric 不可用 | W5 Fabric 结果 | 只标注 Fabric Mock 或未验证，不能写入 Alpha 能力声明 |
| Provider 需要重新选卡才能工作 | W2 selected UUID 结果 | 违反 scheduler-owned selection 边界，D4 fail；不把 chooser 放入 Provider |

## 7. 通过、失败与 XPU-00 关系

### 7.1 XPU-01 通过条件

XPU-01 的 mock conformance 可以在 L0/L1 下通过，必须同时满足：

- NVIDIA identity contract 完整匹配；
- selected UUID 能被 Provider/适配层消费或明确确认；
- canonical DeviceKey 包含正确 NodeUID，且重读幂等；
- 最小 assignment annotation 能生成、解析并在 Bind 后读取；
- 缺失、非法、冲突、Node replacement 和 topology 映射负例均 fail closed；
- 原生 Device Plugin 只能证明数量/注入时，报告明确写成能力不足，而不是 exact-ID 通过。
- Volcano vGPU Adapter 轨道通过时，必须同时满足单槽位配置、单值 UUID allowlist、私有 assignment annotation 和容器注入 UUID 对账；该通过只进入 Adapter capability matrix。

真实 runtime gate 不是 mock conformance 的必要条件，但若执行，必须单独通过 UUID 对账，不能以 L1 结果替代。

### 7.2 失败条件

出现以下任一情况，XPU-01 不能给出 generic “D4 满足”的结论：

- Provider/Device Plugin 无法消费或确认 scheduler-selected UUID；
- Provider 可以枚举设备，但会重新选择或替换 selected DeviceKey；
- assignment annotation 的 resource/provider/DeviceKey 与输入不一致；
- Node replacement 后仍错误恢复旧 DeviceKey；
- 缺失或冲突 assignment 被猜测、覆盖或降级为普通可调度；
- 真实 runtime probe 与 plan identity 不一致。

如果只有 Track C 通过，结论只能是 `D4-vGPU Adapter evidence=Pass`；stock/native D4 仍为 `ProbePending` 或
`XPUAssignmentNotEnforceable`，取决于 Track B 的结果。

这种情况下的统一能力结论是：相关 hard workload 返回 `XPUAssignmentNotEnforceable` 并保持 Pending；mock 层已经通过的部分仍可作为有限证据保留。

### 7.3 对 XPU-00 的影响

XPU-01 完成并不自动等于 XPU-00 完成。它只能补齐 D4 的 backend evidence。当前可以分别报告：

```text
D4-vGPU Adapter evidence       = existing Volcano vGPU path, L1 bounded result
D4-stock/native exact-ID       = remains blocked/pending until XPU-01B
Alpha workload vGPU support    = not introduced by this probe
```

XPU-00 仍需满足合同评审出口：

1. runtime reviewer 接受 NVIDIA assignment identity contract；
2. allocate/bind 责任人接受 annotation 持久路径和旁路行为；
3. 其他 `FrozenForAlpha` 决策完成相应签核；
4. 所有 `ProbePending` 决策有 XPU-01 证据链接。

## 8. 首轮执行清单

第一轮只做以下工作即可形成可评审增量：

1. 建立 L0/L1 环境 manifest 和两个固定 mock GPU；
2. 运行 `nvidia-nvml-mock-selected-uuid-idempotent`，选择非默认 UUID；
3. 运行 `volcano-vgpu-s1` profile，统一两处槽数配置为 `1`，并记录 allowlist、私有 annotation 和 `NVIDIA_VISIBLE_DEVICES`；
4. 记录 stock Provider/Device Plugin 是否能消费或确认 generic scheduler-selected UUID；
5. 生成并重读最小 `xpu-assignment`，验证 NodeUID-safe DeviceKey 和幂等性；
6. 运行缺失、非法、冲突、Node replacement 以及 vGPU 目标不存在/槽位占用四类负例；
7. 输出分轨 capability matrix、gap list 和 D4 结论草案；
8. 若 L2 环境已具备，再追加真实 runtime UUID gate；否则明确 `NotRun`。

首轮禁止提交一个只有空回调的 scheduler plugin PR，也禁止在没有 selected UUID 消费/确认证据时开始声明 hard exact Alpha 已可用。

## 9. 与现有文档的验证边界

本文是 XPU-01 的实施计划，不是运行结果。实施完成后应把真实 evidence path、运行日期、环境版本、case 结果和 reviewer 结论回填到独立报告或后续评审记录中；不要直接把本计划中的预期结果改写成已通过事实。

XPU-00 的 assignment、Pod-derived recovery、现有逐 Pod Bind 和 `XPUAssignmentNotEnforceable` 语义以合同文档为准；XPU-01 不能扩大这些合同的能力范围。
`volcano-vgpu-device-plugin` 只作为 XPU-01 的现有 Adapter 验证后端；原生 NVIDIA Device Plugin 的 exact allocation bridge 另由 XPU-01B 规划。
