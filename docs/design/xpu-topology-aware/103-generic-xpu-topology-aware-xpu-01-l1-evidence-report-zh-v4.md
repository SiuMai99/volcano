# Volcano 通用 xPU 拓扑感知调度 V4：XPU-01 L1 分层证据报告

> M3 对齐说明（2026-09-22）：本报告保存的单 assignment payload 是 XPU-01 探针历史证据，不是 M3 production serialization。
> M3 使用包含 ContainerRef 的 Pod 级 `assignments[]` canonical envelope，且必须另行证明 API Pod 持久化、selected DeviceKey 与 kubelet
> DeviceID 对账；见 [M3 开发计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)。

> 关联合同：[XPU-00 合同冻结记录](./101-generic-xpu-topology-aware-contract-review-zh-v4.md)。
>
> 实施计划：[XPU-01 Provider identity 探针实施计划](./102-generic-xpu-topology-aware-xpu-01-provider-identity-probe-plan-zh-v4.md)。
>
> 采集时间：2026-09-17（UTC）。
>
> 报告修订：2026-09-20；本次修订增加现有 Volcano vGPU/HAMi Adapter 证据边界和单槽位验证计划。
>
> 结论：**stock NVIDIA Device Plugin 的 exact-ID 仍未满足；现有 Volcano vGPU Adapter 可以作为独立 L1 exact UUID 轨道；`deviceSplitCount=1` 的单槽位 profile 尚待按新配置运行。**

## 1. 结论摘要

| 层级/能力 | 结果 | 说明 |
| --- | --- | --- |
| L0 identity contract harness | `Pass` | NVIDIA/NVML/resource/provider、NodeUID-safe `DeviceKey`、最小 assignment 的解析、排序、幂等和负例通过 |
| L1 mock inventory | `Pass` | H100 8 卡、L40S 8 卡、T4 4 卡；mock UUID、NodeUID 和 Node allocatable 数量已对账 |
| L1 NVIDIA Device Plugin 基础链路 | `Pass` | 3 个 Device Plugin Pod Ready，`nvidia.com/gpu` 已注册，Volcano 基础调度链可运行 |
| L1 Provider-Adapter bridge contract | `Pass`（harness） | scheduler-owned selected `DeviceKey` 被原样传给 mock Provider，精确校验后生成最小 assignment |
| L1 kubelet 实际 DeviceID 观察 | `Pass`（观察能力） | PodResources 读取到 `single-h100` 实际分配的 1 个 GPU UUID，且属于该 NodeUID 的 mock inventory |
| L1 stock Device Plugin scheduler-selected UUID exact enforcement | `Blocked` | stock NVIDIA Device Plugin 路径没有 scheduler-selected UUID 的确认/强制通道；实际 UUID 与输入 selected UUID 不相等 |
| L1 Volcano vGPU/HAMi UUID realization | `Pass`（既有 mock 案例，边界限定） | `volcano.sh/vgpu-use-gpuuuid` → `volcano.sh/vgpu-ids-new` → `NVIDIA_VISIBLE_DEVICES` 链路已有可复现入口和断言；不是 stock Device Plugin 证据 |
| L1 Volcano vGPU single-slot profile (`deviceSplitCount=1`) | `NotRun` | 计划将 Device Plugin ConfigMap、node config 和 Pod request 统一为单槽位；现有外部案例需要按该 profile 重新运行 |
| L1 `volcano.sh/xpu-assignment` 随真实 Pod Bind 保留 | `NotRun`/缺口 | bridge 只生成 annotation value，不写 API Pod；当前 MVP Pod 没有该 annotation |
| L2 真实 NVIDIA runtime identity gate | `NotRun` | 当前环境是 arm64 kind + nvml-mock，不是真实 NVIDIA 驱动和 GPU 节点 |

因此，本轮不能把 generic XPU-01 或 XPU-00 D4 标记为完成。可以单独记录 `D4-vGPU Adapter evidence`，但不能用它覆盖
stock/native Device Plugin 的缺口。可确认的准确能力边界是：

```text
mock inventory + native Device Plugin registration + kubelet DeviceID observation = available
Provider-Adapter exact-key validation + minimum assignment generation = available in harness
stock Device Plugin scheduler-selected UUID exact enforcement = not enforceable
Volcano vGPU/HAMi UUID realization = available as a separate Adapter path
Volcano vGPU single-slot profile = planned, not run
real NVIDIA runtime identity = not run
```

## 2. 实际环境

环境快照见 [`environment.txt`](../../../evidence/xpu-01/l1-20260917-kind/environment.txt)。本轮使用：

- kind 集群 `volcano-gpu-mvp`，1 个 control-plane、3 个 worker，Kubernetes server v1.33.12；
- `nvml-mock` 三种 profile：H100 8 卡、L40S 8 卡、T4 4 卡；
- NVIDIA Device Plugin mock DaemonSet，3 个 Pod 全部 Ready；
- Volcano scheduler、controllers、admission 全部 Running；
- workload `gpu-mvp/single-h100`：`Running`，请求 `nvidia.com/gpu: 1`，绑定到 `volcano-gpu-mvp-worker`。

本次环境是对外部 `volcano-device-plugin-mvp` 的可复现实验输入，但没有修改该外部工程。其原始 `make run` 受到 nvml chart SHA 与当前 OCI 内容不一致、旧 profile 参数与当前 chart/node-agent 参数不兼容的影响；本轮使用当前 chart/image 手工启动并保存了实际环境快照。该差异属于环境复现边界，不应隐藏在 XPU-01 结论中。

### 2.1 Volcano vGPU Adapter 复现环境

现有 vGPU 证据来自外部案例目录：

```text
/Users/oujunwei/Documents/hami/volcano-hami-vgpu/
```

该案例切换到 `volcano-vgpu-device-plugin`，启用 Volcano deviceshare，并使用 `nvml-mock` H100 worker。案例链路为：

```text
nvml-mock
  -> volcano-vgpu-device-plugin
  -> volcano.sh/node-vgpu-register
  -> Volcano UUID allowlist/filter
  -> volcano.sh/vgpu-ids-new
  -> kubelet Allocate
  -> NVIDIA_VISIBLE_DEVICES
```

既有案例的目标 UUID 为 `GPU-01000100-0000-0000-0000-000000000001`，入口和断言见：

- `/Users/oujunwei/Documents/hami/volcano-hami-vgpu/README.md`；
- `/Users/oujunwei/Documents/hami/volcano-hami-vgpu/scripts/run-case.sh`；
- `/Users/oujunwei/Documents/hami/volcano-hami-vgpu/manifests/workload-specific-h100.yaml`。

该案例当前 manifest 的 `deviceSplitCount` 为 `10`，因此只能作为已有 vGPU UUID realization 证据。新增的
`volcano-vgpu-s1` profile 需要把 Device Plugin ConfigMap 和 node config 的槽数统一改为 `1`，并重新运行后才能报告单槽位结果。
`deviceSplitCount=1` 只约束每张卡的逻辑槽位数量，不单独证明 exact UUID。

## 3. 核心证据

### 3.1 固定输入和 mock inventory

输入 manifest：[`input-manifest.json`](../../../evidence/xpu-01/l1-20260917-kind/input-manifest.json)。

- 固定 contract：`Vendor=NVIDIA`、`ProviderID=nvidia-nvml-v1`、`ResourceName=nvidia.com/gpu`、`DiscoveryAPI=NVML`；
- 记录 20 个 mock GPU UUID 及其 NodeUID；
- 选定 H100 非默认 UUID：`GPU-01000100-0000-0000-0000-000000000001`；
- `providerCanConfirmSelected=false` 是故意输入，用于验证 stock Device Plugin 的能力缺口，而不是 mock inventory 失败。

原始 inventory 和基础环境检查：

- [`mvp-inventory.log`](../../../evidence/xpu-01/l1-20260917-kind/mvp-inventory.log)：UUID、型号、显存、PCI、拓扑矩阵和数量对账；
- [`mvp-verify.log`](../../../evidence/xpu-01/l1-20260917-kind/mvp-verify.log)：节点、Device Plugin、Volcano API/控制器和调度链检查。

inventory 输出中的 `libpcisysfs.so.1` preload warning 来自 mock profile 的运行环境，未影响 UUID、拓扑和资源数量采集；它不能被解释成真实 PCI/Fabric 证据。

### 3.2 L0 contract harness

实现和测试位于：

- [`tools/xpu-01/probe.go`](../../../tools/xpu-01/probe.go)；
- [`tools/xpu-01/probe_test.go`](../../../tools/xpu-01/probe_test.go)；
- [`tools/xpu-01/bridge.go`](../../../tools/xpu-01/bridge.go)；
- [`tools/xpu-01/bridge_test.go`](../../../tools/xpu-01/bridge_test.go)；
- [`tools/xpu-01/testdata/mock-selected-uuid.json`](../../../tools/xpu-01/testdata/mock-selected-uuid.json)。

已覆盖：

- 同一 NodeUID 下重复 DeviceID 拒绝；不同 NodeUID 下同名 DeviceID 可区分；
- NodeUID 与 DeviceID 组成 canonical key；
- assignment 的未知字段、错误 provider/resource、空值、重复 key、非法 key 拒绝；
- DeviceID 不属于目标 NodeUID、设备 unhealthy、Provider 无法确认 selected ID 时 fail closed；
- assignment 排序、序列化和重读保持幂等。

该 harness 是 L0 测试 double，不是 scheduler plugin、Provider production implementation 或 NVIDIA Device Plugin 修改。

### 3.3 最小 Provider-Adapter bridge

实现入口：[`tools/xpu-01/bridge.go`](../../../tools/xpu-01/bridge.go)。它固定三条边界：

1. scheduler 负责提供最终 `NodeUID/DeviceKey`；
2. Provider 只按 exact NodeUID、UUID 和 health 校验，不重新选择设备；
3. Adapter 生成最小 `volcano.sh/xpu-assignment` JSON value，并做严格 round-trip 校验，但不写 Pod、不调用 kubelet。

同一份 kind manifest 的正向结果见 [`bridge-result.json`](../../../evidence/xpu-01/l1-20260917-kind/bridge-result.json)。本次确认得到：

```text
selected DeviceKey = a9de0849-181a-45e5-aa17-b08aef62c817/GPU-01000100-0000-0000-0000-000000000001
Provider confirmation = Pass
assignment annotation key = volcano.sh/xpu-assignment
assignment deviceKeys = the same selected DeviceKey
```

这是 bridge contract feasibility 的正向证据，不是 kubelet exact-ID enforcement 证据；后者仍由下一节的 stock path 对比证明尚未具备。

### 3.4 kubelet 实际 DeviceID

采集入口：[`probe-podresources-kind.sh`](../../../tools/xpu-01/probe-podresources-kind.sh)；客户端：[`podresources/main.go`](../../../tools/xpu-01/podresources/main.go)。

目标 Pod 快照：[`single-h100.pod.json`](../../../evidence/xpu-01/l1-20260917-kind/single-h100.pod.json)。

PodResources 原始结果：[`podresources-single-h100.json`](../../../evidence/xpu-01/l1-20260917-kind/podresources-single-h100.json)。

结构化对比：[`podresources-single-h100-comparison.json`](../../../evidence/xpu-01/l1-20260917-kind/podresources-single-h100-comparison.json)。关键结果：

```text
expected selected UUID = GPU-01000100-0000-0000-0000-000000000001
actual kubelet DeviceID = GPU-01000100-0000-0000-0000-000000000000
actual ID belongs to node inventory = true
actual ID equals expected selected UUID = false
```

这证明 kubelet 确实为 Pod 记录并分配了一张 mock GPU，也证明该 ID 属于目标节点库存；它同时证明 stock `nvidia.com/gpu: 1` 请求没有把 scheduler-selected UUID 强制传递到实际分配结果。

### 3.5 契约判定

stock Device Plugin 结构化结果：[`probe-result.json`](../../../evidence/xpu-01/l1-20260917-kind/probe-result.json)。

```json
{
  "status": "Fail",
  "reason": "XPUAssignmentNotEnforceable"
}
```

这里的 `Fail` 是 selected-ID 能力探针的失败，不是整个 mock 集群或 inventory 的失败。能力说明见 [`capability-note.txt`](../../../evidence/xpu-01/l1-20260917-kind/capability-note.txt)。

相对地，Provider-Adapter bridge 的结构化结果为 [`bridge-result.json`](../../../evidence/xpu-01/l1-20260917-kind/bridge-result.json)，其 `status=Pass` 只表示 mock Provider 已按合同确认 selected key，并生成了 assignment value。

### 3.6 Volcano vGPU/HAMi Adapter 证据

既有案例的正向断言是：

```text
Pod schedulerName                  = volcano
volcano.sh/vgpu-use-gpuuuid        = GPU-...0001
volcano.sh/vgpu-ids-new            contains GPU-...0001
NVIDIA_VISIBLE_DEVICES             = GPU-...0001
Pod phase                          = Running
```

这证明了现有 Volcano vGPU Adapter 在 `nvml-mock` 环境下能够把 UUID allowlist 传递到 vGPU assignment 和插件注入结果。
它不证明 stock NVIDIA Device Plugin 能消费 generic scheduler-selected UUID，也不证明真实硬件 CUDA、隔离、性能或释放。
另外，`volcano.sh/vgpu-use-gpuuuid` 是当前案例的用户 allowlist 输入，不是 `volcano.sh/xpu-assignment` 中的 scheduler-owned
canonical `DeviceKey`。generic XPU recovery 仍需单独完成 assignment 持久化和 NodeUID 校验。

## 4. 验证命令与结果

在当前仓库内通过：

```text
GOCACHE=/private/tmp/xpu01-gocache go test ./tools/xpu-01
GOCACHE=/private/tmp/xpu01-gocache go test -race ./tools/xpu-01
GOCACHE=/private/tmp/xpu01-gocache go vet ./tools/xpu-01
bash -n tools/xpu-01/collect-kind-l1.sh tools/xpu-01/probe-podresources-kind.sh
git diff --check
```

PodResources 客户端也已在隔离的临时 Go module 中使用本机缓存的 Kubernetes/gRPC 源码完成 `go test`、`go vet` 和 Linux/arm64 build；该临时 module 不改变仓库的 `go.mod`/`go.sum`。当前根模块的完整 cross-build 仍受本机 module cache 写权限和指定 gRPC 版本未完全缓存影响，这是验证环境限制，不是 L0 harness 的功能失败。

## 5. 对后续工作的影响

当前形成两条后续路径：

1. **当前可用的 Volcano vGPU Adapter 路径**：按 `volcano-vgpu-s1` profile 重新运行单槽位证据，保留 vGPU 私有 annotation、restore 流程和容器 UUID 断言；该路径不修改原始 Alpha 的 vGPU API 范围。
2. **后续 native NVIDIA Device Plugin 路径**：另立 `XPU-01B` exact allocation bridge 计划，不把工作简化成只修改 `Allocate()`。

native bridge 仍需明确：

1. scheduler 如何把 selected `DeviceKey` 交给 Provider/适配层；
2. Provider 如何只确认/消费该 key，而不是重新选卡；
3. 哪个组件负责把 assignment 写入 Pod，并保证 Bind 后可恢复；
4. Provider 无法确认时，hard workload 如何保持 `Pending` 并返回 `XPUAssignmentNotEnforceable`；
5. 如何用 PodUID、NodeUID、ContainerName、ResourceName 和实际 DeviceIDs 完成端到端对账。

当前 bridge 仍是可执行合同 harness，不是 production Provider、scheduler plugin 或 Pod mutation 实现。在它接入真实 API/组件前，不应把 native NVIDIA Device Plugin 的整数资源分配结果解释成 exact-ID 支持，也不应开始声明真实 NVIDIA runtime 或 Fabric 已验收。

最终分层结论：

```text
Volcano vGPU/HAMi Adapter UUID realization = 当前可复用的 L1 后端证据
Volcano vGPU single-slot (deviceSplitCount=1) = 计划验证配置，当前 NotRun
stock NVIDIA Device Plugin exact-ID       = Blocked，后续 XPU-01B
Alpha workload GPU virtualization support = 未引入
```
