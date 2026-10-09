# XPU-01B NodeLock 后续探针：槽位复用、三轮并发与身份接缝（2026-10-10）

## 结论

在 `kind-xpu01b-lock-1009` 隔离集群，先前删除 Pod 后保留在 kubelet checkpoint 的两个旧 `volcano.sh/vgpu-number` PodUID **没有阻碍复用**。新建一个指定物理 GPU-B 的 Pod 后，它正常 Running，原两个 UID 从 checkpoint 消失，checkpoint 改为该新 Pod 的虚拟槽位。随后在 `deviceshare.NodeLockEnable=true` 下又运行三轮同数量 Pod A/B，连同[前次首轮](../README.md)共四轮：每轮两个 Pod 最终 Running，A 的 deviceshare 物理 UUID、插件响应、checkpoint 中归属于 A 的 Allocate 响应及容器 `NVIDIA_VISIBLE_DEVICES` 均为 GPU-A；B 对应 GPU-B，未再观察到物理 UUID 交叉。

每轮 A 第一次分配遇到节点锁，在约 1.05～1.07 秒后重试成功；NodeLock 注解在两次插件成功响应后清除。每轮 kubelet PodResources 的**虚拟槽位**可能与物理 UUID 前缀相反。例如本次三轮 A 都得到 `GPU-B-0`、B 得 `GPU-A-0`，但插件响应及容器注入分别是 GPU-A/GPU-B。这符合[设计文档](../../../../../docs/design/xpu-topology-aware/108-generic-xpu-topology-aware-xpu-01b-volcano-vgpu-s1-development-plan-zh-v4.md)定义的分账，不能把 PodResources 的槽位前缀当成注入物理卡，也不能以它反查 Pod 身份。

**结论仅限 mock 环境中 NodeLock 的缓解与槽位复用。** 虚拟槽位与注入物理 UUID 字面不同是允许的正常结果，不构成身份失败。标准 Device Plugin `AllocateRequest` 仍无 PodUID/ContainerRef；本次没有采到原始 RPC `DevicesIds`（仅影响调用时序诊断），也没有注入迟到 RPC、锁超时、组件重启，没有接入 xPU planner 或 API Pod `xpu-assignment`，没有真实 GPU runtime 读回。四轮正确归属不构成所有时序下都能正确关联目标 Pod 的证明；`AssignmentContractReady=false`、M3 hard no-Bind 不变。

## 槽位复用

GPU-A = `GPU-01000100-0000-0000-0000-000000000000`，GPU-B = `GPU-01000100-0000-0000-0000-000000000001`。环境沿用[首轮报告](../README.md)的单节点两张 mock H100、插件和 Node 两处 `deviceSplitCount=1`、正式 vGPU 插件 v1.12.0、同一 NodeLock 配置。测试只操作 `kind-xpu01b-lock-1009`；未触碰 `kind-volcano-gpu-mvp`，未手工删除或修改 checkpoint。

| 顺序 | checkpoint 中 `vgpu-number` PodUID | API/注入/PodResources 结果 |
| --- | --- | --- |
| 首轮 A/B 删除后 | 旧 A/B 两 UID | API 已无 A/B；此前因此停止复跑 |
| 新单 Pod `reuse-gpub-1010` 分配后 | 仅新 UID `eca20e73-a206-45ba-a9b9-c4fe39473fb8` | Running；assignment 与注入 GPU-B；PodResources `GPU-A-0` |
| 本次第 2/3/4 轮各自新分配后 | 仅当前轮两个 UID，前一轮 UID 消失 | 每轮两个槽位可继续分配，PodResources/容器与新 PodUID 归属一致 |
| 第 4 轮删除后再建 GPU-B 单 Pod | 仅新 UID `d779af7f-e80a-459e-9e6c-09ce71323eda` | Running；assignment 与注入 GPU-B；PodResources `GPU-A-0` |

详细前后快照及两次单 Pod 结果见 [checkpoint-reuse.json](./checkpoint-reuse.json)，每轮的 `preCheckpointVGPU` 和 `afterAllocationCheckpointVGPU` 在对应 [第 2 轮](./round-02/allocation-chain.json)、[第 3 轮](./round-03/allocation-chain.json)、[第 4 轮](./round-04/allocation-chain.json)。这证明本配置下旧记录在后续正常分配中被清掉且槽位可复用；它没有证明删除瞬间已经持久化回收，也不能据此推断所有重启/故障路径均可回收。

## NodeLock 多轮并发结果

三轮都复用原双 Pod YAML：A allowlist GPU-A、B allowlist GPU-B，`vgpu-number=1`、`vgpu-cores=100`、`vgpu-memory=1`、`CUDA_DISABLE_CONTROL=true`。第 2 轮 A/B 按原文档顺序一起提交，第 3 轮交换 YAML 文档顺序，第 4 轮 A 先提交、命令侧延迟 200 ms 后提交 B。Pod 创建时间在第 4 轮仍落在同一秒；实际 scheduler 处理顺序由日志判定，而非从 YAML 顺序推断。下表中的“响应 UID”取插件 `Allocate Response` 的 `/tmp/vgpu` mount host_path，并与 kubelet checkpoint 同一 PodUID 的 `AllocResp` 解码结果交叉核对。

| 轮次与提交 | A/B 结果 | 锁冲突与重试 | kubelet 槽位 |
| --- | --- | --- | --- |
| 第 2 轮，A/B 原 YAML | 两 Pod Running；A 响应 UID=A 且注入 GPU-A，B 响应 UID=B 且注入 GPU-B | A 于 `17:58:46.837Z` 遇锁，`17:58:47.886Z` 再设锁，约 1.05 s | A `GPU-B-0`；B `GPU-A-0` |
| 第 3 轮，B/A 倒序 YAML | 同上 | A 于 `18:00:38.528Z` 遇锁，`18:00:39.592Z` 再设锁，约 1.06 s | A `GPU-B-0`；B `GPU-A-0` |
| 第 4 轮，A 后延迟 200 ms 提交 B | 同上 | A 于 `18:01:34.977Z` 遇锁，`18:01:36.044Z` 再设锁，约 1.07 s | A `GPU-B-0`；B `GPU-A-0` |

本表时间均为 2026-10-09 UTC（对应 2026-10-10 Asia/Shanghai）。各轮证据见 [第 2 轮分配链](./round-02/allocation-chain.json)、[第 3 轮分配链](./round-03/allocation-chain.json)、[第 4 轮分配链](./round-04/allocation-chain.json)；每轮目录还含 `scheduler-lock.log`、`plugin-response-and-release.log`、`node-lock-transitions.json` 和 `pod-watch-summary.json`。`Allocate Response` 的 mount UID、注入 UUID 与 kubelet checkpoint 的 PodUID/`AllocResp` 一致，容器实测 `NVIDIA_VISIBLE_DEVICES` 也一致。`pod-watch-summary.json` 只记录注解是否非空，不收录完整 Pod 或 ServiceAccount token。

删除最后一个复用 Pod 后，隔离命名空间无工作负载、Node `hamivgpu` 锁为空、`vgpu-number` allocatable=2。最后一次删除后的 checkpoint 是否立即去掉该 UID 未作为回收判据；本次回收结论来自后续正常分配的前后对照。原始采集（完整 Pod/Node watch、日志、checkpoint）和执行脚本保留在 `/private/tmp/xpu01b-lock-1009/repeat-20261010/` 与 `/private/tmp/xpu01b-lock-1009/reuse-20261010/`，本目录只保留必要摘要，不含 token/secret。证据尚未提交。

## 身份关联判断与下一步

当前插件在 [`GetPendingPod`](https://github.com/Project-HAMi/volcano-vgpu-device-plugin/blob/main/pkg/util/util.go) 中按节点、设备数量、最早时间猜 Pod；`Allocate()` 仅检查所猜 Pod 的设备数与 kubelet 请求数相等，随后从其注解取物理 UUID。NodeLock 约束 scheduler 同时生成的待分配注解，但锁有效期五分钟、插件成功后主动解锁；它没有把某一次 kubelet RPC 的 PodUID/ContainerRef 传给插件。本次 PodResources/checkpoint 后态已显示虚拟槽位可带另一物理 UUID 前缀；**原始 `AllocateRequest.DevicesIds` 仍未采到**，不能把后态直接充作 RPC 请求日志。

因此“唯一 pending Pod”与“按 `DevicesIds` 前缀反查”都不能成为 exact 关联证明。旧 Pod 的迟到 RPC、重试、节点锁超时后新 Pod 的请求、插件重启后的候选重建，是由当前 RPC 字段和源码推导出的风险，**本次未注入这些故障**。遇到两个候选时拒绝比取最早 Pod 安全，但单候选也不等于已验证 RPC 归属。设计决策与可选的 Pod-aware 接缝见 [XPU-01B 计划](../../../../../docs/design/xpu-topology-aware/108-generic-xpu-topology-aware-xpu-01b-volcano-vgpu-s1-development-plan-zh-v4.md)中的“NodeLock 后续探针与 Pod 身份门”。下一步先做原始 RPC 日志与迟到/超时故障注入的小型实验；在可靠身份接缝和故障收敛证据齐备前，NodeLock 只能作为现有 vGPU 路径的并发缓解，不能打开 hard Bind。
