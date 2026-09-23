# Volcano 通用 xPU 拓扑感知调度 V4：PR9 assignment contract 与 exact bridge 开发记录

> 上位计划：[M3 Pod-derived Topology Alpha 开发计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)。
>
> 历史探针：[XPU-01 NVIDIA Provider identity 探针计划](./102-generic-xpu-topology-aware-xpu-01-provider-identity-probe-plan-zh-v4.md)与
> [XPU-01 L1 分层证据](./103-generic-xpu-topology-aware-xpu-01-l1-evidence-report-zh-v4.md)。
>
> 状态：**PR9 开发中；L0/单元合同已落地，L1 exact profile 仍 Blocked**。记录日期：2026-09-23。
> 本文记录代码、静态/Fake 验证和剩余门槛，不表示 production hard Bind、真实 API server 持久化、kubelet selected-ID
> enforcement 或 runtime UUID 已完成。

## 1. 本轮结论

PR9 已将 `volcano.sh/xpu-assignment` 从 `tools/xpu-01` 内的 NVIDIA 单 resource 历史 struct，迁移为 scheduler API 包中的
Pod 级 canonical envelope；M2 compiler 同时开始保留 ContainerRef。assignment Provider 的能力被拆成 validate、consume、kubelet
confirm 和 runtime reconcile，validation-only Fake 无法得到 `ExactReady=true`。

本轮还增加了 resourceVersion-aware API Pod writer PoC，但没有把它接到 `Session.dispatch()`、`Statement.Commit()` 或
`AddBindTask()`。因此：

```text
assignment codec / ContainerRef / Provider validation / writer PoC = Implemented + unit/Fake verified
production final-path persistence                                  = NotIntegrated
stock NVIDIA selected UUID enforcement                            = Blocked
named L1 exact-ready profile                                      = NotAvailable
AssignmentContractReady                                           = false
hard xPU topology Bind                                             = forbidden
```

## 2. 已落地代码

| 区域 | 落点 | 当前能力 | 当前边界 |
| --- | --- | --- | --- |
| production assignment contract | `pkg/scheduler/api/device_topology_assignment.go` | `assignments[]`、ContainerRef、strict decode、duplicate-key/unknown-field 拒绝、canonical sort、NodeUID 校验 | 不建立 anchor，不选择 DeviceKey |
| Pod request shape | `XPUResourceRequestForPod`、`compiler.go` | regular/init/restartable-init 唯一消费者，保留 `ContainerRef + ResourceName + Count` | DRA/MIG/vGPU/shared 继续拒绝 |
| assignment Provider contract | `pkg/scheduler/topology/assignment/provider.go` | process-scoped identity、capability、selected-key validation；接口无选卡方法 | 不提供 reservation/release/ledger |
| validation-only mock | `StaticInventoryProvider` | inventory、NodeUID、Provider identity、health、source generation 校验 | `ConsumeSelectedDeviceKeys=false`、`ExactReady=false` |
| API Pod writer PoC | `pkg/scheduler/cache/xpu_assignment_writer.go` | UID/resourceVersion JSON Patch、canonical value、幂等覆盖、返回对象确认 | 未接 final Bind path；无自动冲突重试 |
| replayable probe | `tools/xpu-01` | 复用 production codec，输入 PodUID/ContainerRef/generation，输出 capability matrix | 不写 Pod、不调用 kubelet |

## 3. 冻结的 assignment envelope

```json
{
  "version": 1,
  "assignments": [
    {
      "container": {
        "kind": "regular",
        "name": "worker"
      },
      "resourceName": "nvidia.com/gpu",
      "provider": "nvidia-nvml-v1",
      "deviceKeys": [
        "<node-uid>/GPU-aaaaaaaa"
      ]
    }
  ]
}
```

已实现规则：

1. `container.kind` 只接受 `regular/init/restartable-init`；
2. 每条 `resourceName` 只能出现一次；
3. DeviceKeys 必须是 `NodeUID/ProviderNormalizedDeviceID`，排序稳定且不允许重复；
4. entry 的 ContainerRef 必须匹配 Pod 中该 resource 的唯一消费者；
5. DeviceKey 数量必须等于正整数 limit；request 可省略，存在时必须等于 limit；
6. NodeName、PodUID、Domain/Fabric、GroupRef 和 plan digest 不复制进 annotation；
7. Provider namespace 由受信任的运行时 Provider identity 补全，不能从 resource prefix 猜测；
8. 同名 Node 换 UID 后，旧 DeviceKey 校验失败。

## 4. Provider 合同与 capability

Provider 只接受 scheduler-selected keys：

```text
SelectedAssignment
  = PodUID
  + NodeName/NodeUID
  + ContainerRef
  + ResourceName
  + selected DeviceKeys
  + SourceGeneration

Provider.ValidateSelected(SelectedAssignment)
  -> accepted DeviceKeys must equal selected DeviceKeys
```

接口没有 `SelectDevice()`。若 Provider 返回不同 DeviceKey，caller 必须将其视为 identity mismatch，不能接受 Provider 重选结果。

当前 capability matrix：

| Profile/组件 | enumerate | selected input | validate | consume | API Pod persist | kubelet confirm | runtime reconcile | exact ready |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `stock-nvidia` | yes | harness only | no | no | no | no | no | no |
| `mock-nvidia-nvml-validation` | yes | yes | yes | no | no | no | no | no |
| API Pod writer PoC | n/a | canonical envelope | n/a | n/a | Fake yes / real API server NotRun | n/a | n/a | no |
| 2026-09-17 `volcano-vgpu-s1` 历史轨道 | yes | private allowlist | private path | private path | generic assignment no | private path evidence | mock injection evidence | generic no |
| 首个 generic exact profile | TBD | required | required | required | required | required | optional L2 | blocked |

`StaticInventoryProvider` 即使 validation Pass，也不能驱动 `AssignmentContractReady`；stock Device Plugin 的数量、
`GetPreferredAllocation` 或 kubelet 任意 DeviceID 也不能补齐 consume/confirm。

## 5. API Pod writer 失败语义

writer 接受 informer/cache 中的 Pod UID 和 resourceVersion，构造带两条 `test` precondition 的 JSON Patch：

```text
test /metadata/uid
test /metadata/resourceVersion
add  /metadata/annotations/volcano.sh~1xpu-assignment
```

固定语义：

- 写入前执行 assignment structural + Pod request validation；
- 保留其他 annotation，覆盖用户预置的同名 key；
- 相同 canonical value 重复写幂等；
- UID/resourceVersion 冲突立即返回错误，不做 blind retry；
- 后续 production caller 必须丢弃 stale plan，从新 snapshot 重新规划；
- writer RPC 不允许在 SchedulerCache 主锁内执行；
- 写成功但 Bind 失败的未绑定 Pod 不建立 recovery anchor。

## 6. 本轮验证

已运行并通过：

```bash
go test ./pkg/scheduler/api \
  ./pkg/scheduler/topology/... \
  ./pkg/scheduler/plugins/xpu-topology-aware \
  ./tools/xpu-01 \
  ./pkg/scheduler/cache

go test -race ./pkg/scheduler/api \
  ./pkg/scheduler/topology/... \
  ./pkg/scheduler/plugins/xpu-topology-aware \
  ./tools/xpu-01 \
  ./pkg/scheduler/cache

go run ./tools/xpu-01 \
  -mode bridge \
  -input ./tools/xpu-01/testdata/mock-selected-uuid.json
```

最后一个命令的关键输出为：

```text
profile                         = mock-nvidia-nvml-validation
validateSelectedKey             = true
consumeSelectedKey              = false
persistAPIPodAssignment         = false
confirmKubeletDeviceID          = false
reconcileRuntimeDeviceID        = false
exactReady                      = false
```

上述结果只证明 L0 contract 与 validation-only Fake；没有运行真实 API server/kind writer round-trip，也没有产生新的 L1/L2 证据。

## 7. PR9 剩余工作与 Stop gate

### 7.1 PR9 内继续完成

1. 在 API server/kind 上执行 writer round-trip，保存 Patch 前后的 Pod GET、UID、resourceVersion 和 canonical annotation；
2. 为候选 exact bridge 明确 Pod/Container 与 kubelet Allocate 的可验证关联机制；
3. 运行非默认 UUID 正例，保存 selected key、persisted assignment、bridge accepted key、kubelet DeviceID；
4. 运行 unknown/unhealthy/busy device、NodeUID replacement、generation change、writer conflict 和 UUID mismatch 负例；
5. 输出明确命名的 L1 profile capability matrix，不能用 mock Provider 或 vGPU 私有 allowlist 覆盖 generic 缺口。

### 7.2 Stop gate

若候选机制只能做到以下任一项，PR9 必须保持 `Blocked`：

- 只能枚举设备或返回 preference；
- 只能在 `Allocate()` 看到 kubelet 已经选好的 `DevicesIds`；
- 无法把 Allocate 调用可靠关联到 assignment 中的 PodUID/ContainerRef；
- API Pod 重读不到 assignment；
- bridge 允许重选另一个 UUID；
- kubelet accounting ID 与 runtime 注入 UUID 不同；
- 实现需要偷偷引入 DRA、kubelet fork、外部 reservation/ledger 或 vGPU/shared 请求形状。

在以上 Stop gate 关闭前，PR9 不修改 `AssignmentContractReady` 的运行时来源，不接 production Bind，不开始 PR12/13 的 hard 放行。

