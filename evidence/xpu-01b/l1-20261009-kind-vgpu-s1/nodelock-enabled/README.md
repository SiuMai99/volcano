# XPU-01B 现有 vGPU 路径 NodeLock 并发对照探针（2026-10-09）

> 后续复核（2026-10-10）：下文“槽位回收未证、未做第二轮”仅记录本轮当时的停止点。[后续探针](./followup-20261010/README.md)已通过新 Pod 正常分配证明旧 checkpoint UID 不阻碍槽位复用，并另完成三轮 NodeLock 并发复测；本轮原始记录保留不改。

## 结论与边界

在新建的隔离集群 `kind-xpu01b-lock-1009` 中，**仅把 scheduler 的 `deviceshare.NodeLockEnable` 从默认关闭改为 `true`**，其余并发 Pod YAML 与[无锁基线](../README.md)完全相同。一节点两张 mock H100、两处 `deviceSplitCount=1`，A/B 同时提交、各请求一个虚拟槽位且分别指定 GPU-A/GPU-B。首轮观察到 NodeLock 将两个待分配 Pod 串行化：B 首次遇锁被拒，约 1.05 秒后重试，两 Pod 最终 Running，assignment、插件响应/容器注入和 PodResources 分别与各自的 UUID 一致，**本轮未复现基线中的交叉**。

这只支持“NodeLock 在本轮缓解了同节点同数量 Pod 的并发关联歧义”。删除两 Pod 后，API 与 PodResources 的 Pod 已消失、NodeLock 清除、Node allocatable 仍为 2，但 kubelet checkpoint 持续保留这两个 UID 的虚拟槽位；因此按探针约束**未做第二轮**，稳定性和槽位回收未证。未接入 xPU planner/API Pod `xpu-assignment`，未采到原始 `AllocateRequest.DevicesIds` RPC，也未使用真实 NVIDIA GPU。`AssignmentContractReady=false`，M3 hard 继续 no-Bind。

| 检查项 | 状态 | 证据 |
| --- | --- | --- |
| NodeLock 配置已加载、节点锁实际 set/clear | Pass | [scheduler 配置](./round-01/scheduler-config.json)、[Node watch](./round-01/node-lock-watch.json)、[scheduler 锁日志](./round-01/scheduler-lock.log) |
| 两个同数量 Pod 首轮最终 Running，UUID 关联无交叉 | Pass（仅一轮） | [Pod 摘要](./round-01/pod-summaries.json)、[插件响应](./round-01/plugin-allocate-and-release.log)、[容器值 A](./round-01/concurrent-a-visible.txt)、[容器值 B](./round-01/concurrent-b-visible.txt)、[PodResources A](./round-01/concurrent-a-podresources.json)、[PodResources B](./round-01/concurrent-b-podresources.json) |
| B 首次锁冲突与后续重试 | Pass | [scheduler 锁日志](./round-01/scheduler-lock.log)、[Pod 事件](./round-01/events-summary.json) |
| 删除后 checkpoint 虚拟槽位回收、第二轮稳定性 | 未证 | [删除后 checkpoint](./round-01/post-delete-checkpoint-vgpu-number.json)、[次日复核](./round-01/post-delete-recheck-checkpoint-vgpu-number.json)仍包含首轮两个 UID；见下文 |
| 插件原始 `AllocateRequest.DevicesIds`、xPU planner/API assignment、L2 真卡 | NotRun | 正式插件未记录 RPC 请求 ID；PodResources/checkpoint 不能代替它；该场景只验证现有 vGPU 路径 |

GPU-A = `GPU-01000100-0000-0000-0000-000000000000`，GPU-B = `GPU-01000100-0000-0000-0000-000000000001`。PodResources ID 后缀 `-0` 是 kubelet 的**虚拟槽位**，不是另一张物理卡或原始 Allocate RPC 参数。首轮两 Pod 的完整四层对照：

| Pod / UID | UUID allowlist | deviceshare `vgpu-ids-new` | 插件响应 `NVIDIA_VISIBLE_DEVICES` / 容器实测 | PodResources `volcano.sh/vgpu-number` |
| --- | --- | --- | --- | --- |
| A / `3f46da77-021d-4555-989e-943752395934` | GPU-A | GPU-A | GPU-A / GPU-A | `GPU-A-0` |
| B / `64efd216-3a53-450a-99b8-217ee9acaea0` | GPU-B | GPU-B | GPU-B / GPU-B | `GPU-B-0` |

两 Pod 均创建于 `2026-10-09T10:13:56Z`，A 于 `10:13:58Z` Ready，B 于 `10:13:59Z` Ready（下述时间均为 UTC）。[Pod watch](./round-01/pod-watch.json)在 resourceVersion 955 记录 A 的 `devices-to-allocate` 非空，963 清空；B 直到 976 才非空，982 清空。最终 Pod 快照中的 `devices-to-allocate` 均为空，因为该注解在 Allocate 后被消费，不能据最终快照回推其原内容。

| 时间 | 可复核事件 |
| --- | --- |
| 10:13:57.933438 | scheduler 首次 `Node lock set`；[Node watch](./round-01/node-lock-watch.json) RV950 记录 `hamivgpu` 锁存在 |
| 10:13:57.938951 / .939150 | B `AllocateToPod` 因节点已锁失败，Statement callback 同次失败；[B 事件](./round-01/events-summary.json)记录 10:13:57 `FailedScheduling` |
| 10:13:58.017029 / .018374 | 插件首次 Allocate 响应的 mount 路径含 **A UID**、注入 GPU-A，随后 `AllDevicesAllocateSuccess releasing lock`；Node watch RV965 锁已清除 |
| 10:13:58.992212 | scheduler 再次 `Node lock set`，距 B 首次锁冲突约 **1.05 秒**；Node watch RV971 再次记录锁存在 |
| 10:13:59.071565 / .073352 | 插件第二次响应的 mount 路径含 **B UID**、注入 GPU-B，随后释放锁；Node watch RV984 锁已清除，B 在 10:13:59 获 `Scheduled` 事件并 Ready |

对照[无锁基线](../README.md)：相同 Pod 负例曾出现 A assignment GPU-A 但注入/PodResources GPU-B，B 反向交叉，且 watch 捕获两个 `devices-to-allocate` 同时非空。本轮两者不重叠，并出现真实的锁冲突、释放、重试日志。这个对照有独立集群的 NodeUID/启动时序差异，只能证明一次运行中的缓解效果，不能把偶然无交叉当作并发安全证明。

## 环境、清理与复现

- 隔离集群 `kind-xpu01b-lock-1009`，worker NodeUID `e20fa95a-d64e-437b-b44e-3aeb7db8fdcc`，Kubernetes v1.33.12/arm64，[Node 摘要](./round-01/node-summary.json)显示两张 H100 mock GPU 和 `volcano.sh/vgpu-number` allocatable=2。Volcano chart 1.15.2 SHA256 `84b13ab82dd73c95307131ac105566f1dcf96383eed14bf30c4a74b0020b3fdd`；nvml-mock chart 0.3.0 SHA256 `d61e6c1e04242935a4c74a7a0624242c71ddd27238bfbf7c437b933b67bddc44`，与基线相同；见 [chart SHA](./chart-sha256.txt)和 [Helm 发布](./helm-releases.json)。scheduler 使用本地 `volcano-vgpu-uuid/vc-scheduler:dev` 镜像。
- 正式 vGPU 插件 `projecthami/volcano-vgpu-device-plugin:v1.12.0`，运行时 imageID `sha256:1b81679a3ef3c8c526580929b6b0a04b40d218ed24290f16bdb46295099c3858`，见[插件运行镜像](./plugin-runtime-image.json)；`ghcr.io/nvidia/nvml-mock:latest` 的本次 [RepoDigest](./mock-repodigest.json) 为 `sha256:febb7bd3dc61119bdd05b9d96e9cde1db03c748ebc33dfdf62538e17f0149d1d`、[运行时 imageID](./mock-runtime-image.json) 为 `sha256:79aac489ca197be67499754a1f086d0dcab15f016d4a5af8bab1ad4145348098`，均与基线本次独立配置相同。workload 为 `vgpu-number=1`、`vgpu-cores=100`、`vgpu-memory=1`、`CUDA_DISABLE_CONTROL=true`。
- 最初在基线隔离集群 `kind-xpu01b-probe-1009` 把 NodeLock 改为 true 并重启 scheduler，随后删除原并发 A/B。其 kubelet checkpoint 的旧 UID 未随 Pod 删除而消失；即使**仅在该隔离 worker** 重启 kubelet并确认插件重新注册，旧 UID 仍在，故没有混用旧槽位。改用全新集群 `kind-xpu01b-lock-1009` 跑本轮；[原集群 checkpoint 摘要](./first-cluster-post-restart-checkpoint.json)保留这个前置障碍。两个隔离集群目前均存在、探针 Pod 已删除；基线集群现仍加载 NodeLock=true（并非原基线测量时的设置）。用户已有 `kind-volcano-gpu-mvp` 未操作。
- 本轮 Pod 删除后，[API 摘要](./round-01/post-delete-pods.json)无 Pod，[PodResources 复核](./round-01/post-delete-podresources.txt)找不到两者，[Node 摘要](./round-01/post-delete-node.json)无 `hamivgpu` 锁且 allocatable=2；[删除后 checkpoint](./round-01/post-delete-checkpoint-vgpu-number.json)仍列 A-0/B-0。于 `2026-10-10 01:14` 左右（Asia/Shanghai）再次只读核对，[checkpoint 复核](./round-01/post-delete-recheck-checkpoint-vgpu-number.json)仍列这两个 UID。**目前只确认 checkpoint 残留，未定位其生命周期原因或证明永久泄漏。** 按不混用旧槽位的约束停止复跑，也未手工删除 checkpoint。

配置和 workload 在 [kind](./kind.yaml)、[mock](./nvml-h100-two.yaml)、[Volcano NodeLock values](./volcano-vgpu-nodelock.yaml)、[scheduler Pod annotation RBAC](./scheduler-pod-annotation-rbac.yaml)、[vGPU 插件](./vgpu-plugin-s1.yaml)、[并发双 Pod](./concurrent-two.yaml)。[本地镜像预载脚本](./import-images.sh)仅用于这个命名隔离集群；换集群重放时须改其集群名与拓扑 node 名，避免操作现存集群。此轮执行顺序为：

```sh
kind create cluster --name xpu01b-lock-1009 --config kind.yaml
bash import-images.sh
helm --kube-context kind-xpu01b-lock-1009 upgrade --install nvml-h100 /private/tmp/xpu01b-probe-1009/nvml-mock-0.3.0.tgz -n nvml-mock-system --create-namespace -f nvml-h100-two.yaml
helm --kube-context kind-xpu01b-lock-1009 upgrade --install volcano /private/tmp/xpu01b-probe-1009/volcano-1.15.2.tgz -n volcano-system --create-namespace -f volcano-vgpu-nodelock.yaml
kubectl --context kind-xpu01b-lock-1009 apply -f scheduler-pod-annotation-rbac.yaml
kubectl --context kind-xpu01b-lock-1009 apply -f vgpu-plugin-s1.yaml
kubectl --context kind-xpu01b-lock-1009 create namespace xpu01b-probe
kubectl --context kind-xpu01b-lock-1009 apply -f concurrent-two.yaml
```

执行前确认唯一集群名、镜像、chart 可用且 scheduler、mock、插件已 Ready，Node 正好登记 GPU-A/GPU-B、allocatable=2、checkpoint 无旧 vGPU PodUID；提交时同时启动 Pod/Node watch。以上记录是本次顺序，**不能在当前残留 checkpoint 的集群直接重跑创建命令**。完整原始 Pod/Node watch、Pod JSON、日志、checkpoint、事件、PodResources 与采集脚本保存在 `/private/tmp/xpu01b-lock-1009/round-01/`；本目录只保留必要摘要和相关原始日志行，不含 Pod ServiceAccount token。未改 Volcano 或外部插件产品代码，证据尚未提交。
