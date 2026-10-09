# XPU-01B 现有 vGPU 路径 L1 基线探针（2026-10-09）

## 结果

**单 Pod 指定非默认 GPU-B 的选择与注入链通过；两个同数量 Pod 的并发负例实际发现 Pod 物理身份交叉，现有路径不能放行 XPU-01B exact-ready。** 两个 Pod 都 Running，deviceshare 分别占用 GPU-A、GPU-B，kubelet 也各记 1 个虚拟槽位，但容器获得了对方的物理 UUID。`AssignmentContractReady` 必须保持 `false`，M3 hard 继续 no-Bind。

| 项目 | 状态 | 证据和边界 |
| --- | --- | --- |
| 一节点两张可区分 H100 mock GPU、插件和 node config 两处单槽位 | Pass | [Node 摘要](./node-summary.json)、[实际插件清单](./vgpu-plugin-s1.yaml)；`volcano.sh/vgpu-number` allocatable=2 |
| 单 Pod GPU-B allowlist → deviceshare assignment → 插件响应/容器注入 | Pass | [Pod 摘要](./success-pod-summary.json)、[插件响应](./plugin-allocate-responses.log)、[容器值](./success-container-visible.txt)均为 GPU-B |
| 单 Pod kubelet PodResources 虚拟槽位 | Pass | [PodResources](./success-podresources.json) 为 `GPU-B-0`，不要求字面等于物理 UUID |
| 两个同数量 Pod 并发的正确 Pod/物理 UUID 关联 | **Fail，实际 fail-open** | [Pod watch](./concurrent-pod-watch.json)捕获两者 `devices-to-allocate` 同时非空；下表显示交叉 |
| 成功路径插件 `AllocateRequest.DevicesIds` 原始 RPC 输入 | **NotRun，未直接采到** | 正式 v1.12.0 镜像未打印该字段；PodResources 和 checkpoint 不能冒充 RPC 原始日志。临时诊断构建尝试见下文 |
| xPU planner selected key、API Pod `xpu-assignment`、L2 真实 GPU UUID | NotRun | 本探针仅验证**现有 UUID allowlist/vGPU 后端基线** |

GPU-A 为 `GPU-01000100-0000-0000-0000-000000000000`，GPU-B 为 `GPU-01000100-0000-0000-0000-000000000001`。两个并发 Pod 同秒创建，均请求 `volcano.sh/vgpu-number=1`、cores=100；A 指向 GPU-A，B 指向 GPU-B：

| Pod / UID | allowlist、`vgpu-ids-new` | 插件注入 `NVIDIA_VISIBLE_DEVICES` | kubelet 虚拟槽位 |
| --- | --- | --- | --- |
| `concurrent-a` / `2e2c14e7-bdb4-4421-bc84-9ac5ccb30c45` | **GPU-A** | **GPU-B** | `GPU-B-0` |
| `concurrent-b` / `84a4e7e1-80b4-4b31-b193-ed35b6b8e44b` | **GPU-B** | **GPU-A** | `GPU-A-0` |

对照 [并发 Pod 摘要](./concurrent-pod-summaries.json)、[A 容器值](./concurrent-a-visible.txt)、[B 容器值](./concurrent-b-visible.txt)、[A PodResources](./concurrent-a-podresources.json)、[B PodResources](./concurrent-b-podresources.json) 和 [kubelet checkpoint 精简摘录](./kubelet-slot-checkpoint-summary.json)。[插件响应日志](./plugin-allocate-responses.log)中，GPU-B 的首次响应 mount 路径含 B Pod UID，GPU-A 的第二次响应路径含 A Pod UID；kubelet 却把前者给了 A、后者给了 B。`devices-to-allocate` 是消费中的临时注解：正例完成后的 Pod 快照为空；并发 [watch](./concurrent-pod-watch.json)在 resourceVersion 1734/1735 捕获两 Pod 均非空，1743/1746 才先后清空。因此本次确实进入同节点、同数量候选 Pod 的歧义窗口。

插件源码 `GetPendingPod` 按 Node 与请求数量找 oldest pending Pod，`Allocate()` 只比较数量再用该 Pod 的物理 UUID 生成响应；这与观测到的交叉一致。此处是源码与运行结果共同支持的解释，尚无成功路径的原始 `AllocateRequest.DevicesIds` 日志可用于重建完整 RPC 时间线。**负例不是“通过”**；它已经暴露现有 vGPU 后端的 Pod 身份接缝问题。

## 隔离环境和工件

- 新集群 `kind-xpu01b-probe-1009`；worker `xpu01b-probe-1009-worker`，NodeUID `81dd9878-35fb-4967-87cf-4a7d5573914c`，Kubernetes v1.33.12，arm64。旧集群 `kind-volcano-gpu-mvp` 未改动。
- Volcano chart 1.15.2，scheduler 镜像 `docker.io/volcano-vgpu-uuid/vc-scheduler:dev`；正式插件 `projecthami/volcano-vgpu-device-plugin:v1.12.0`，运行时 imageID `sha256:1b81679a3ef3c8c526580929b6b0a04b40d218ed24290f16bdb46295099c3858`。[Helm 发布](./helm-releases.json)。Volcano checkout HEAD `233f5c4c5fce95327c1e57e4433079ba2aa1eff1`。
- nvml-mock chart 0.3.0 本次下载 SHA256 为 `d61e6c1e04242935a4c74a7a0624242c71ddd27238bfbf7c437b933b67bddc44`，与历史案例脚本锁定的 `900027ab...` 不同。历史 `sha-c8557c3` 镜像不识别本次 chart 的 `--topology` 参数，首次安装 CrashLoop；本次隔离集群改用本机缓存 `ghcr.io/nvidia/nvml-mock:latest`，digest `sha256:febb7bd3dc61119bdd05b9d96e9cde1db03c748ebc33dfdf62538e17f0149d1d`，运行时 imageID `sha256:79aac489ca197be67499754a1f086d0dcab15f016d4a5af8bab1ad4145348098` 后就绪。[chart SHA](./chart-sha256.txt)、[mock 镜像 digest](./nvml-image.txt)。本次是独立配置，不与历史工件混同。
- Workload 用 `vgpu-cores=100`、`vgpu-memory=1`（该 mock 配置为 32 MiB 单位）、`CUDA_DISABLE_CONTROL=true`。本实验不验证整卡显存隔离、HAMi-core preload 或真实 NVIDIA runtime。

可重放配置在 [kind](./kind.yaml)、[mock values](./nvml-h100-two.yaml)、[Volcano values](./volcano-vgpu.yaml)、[vGPU 插件](./vgpu-plugin-s1.yaml)、[单 Pod](./workload-gpub.yaml) 和 [并发双 Pod](./concurrent-two.yaml)。先建唯一命名的 kind 集群并预载镜像，再按 mock → Volcano → scheduler Pod annotation RBAC → vGPU 插件 → workload 顺序安装；单 Pod 采证后需删除并等 GPU 释放，再提交双 Pod。外部案例 `setup.sh`/`run-case.sh` 硬编码既有集群，不能直接用于本探针。

为采集原始 RPC，已将 vGPU 源码复制到 `/private/tmp/xpu01b-probe-1009/vgpu-diagnostic-src/`，仅在副本里增加请求 ID 与匹配 PodUID 的日志。macOS 到 Linux 的 `CGO_ENABLED=0` 编译因 NVIDIA NVML 依赖失败；随后拉取 Linux Go 构建镜像，但下载长时间未完成并已中止。因此**没有构建、部署或运行诊断插件**，该项保持 NotRun，不能把 PodResources/Checkpoint 追认为原始 RPC。

随后只读核对了隔离 worker 的现成追踪能力：kubelet `journalctl -u kubelet` 在 09:27 并发窗口没有 `Allocate`/`DevicesIds` 请求日志，现存日志只记录了 09:21 的 device plugin 注册；worker 和插件容器均无 `strace`、`socat`、`tcpdump`、`grpcurl`，主机也无 `strace`/`socat`/`grpcurl`。现有 UDS socket 与 kubelet checkpoint 可读，但 checkpoint 是调用后状态。要拦截原始 UDS RPC，需改变插件 socket/注册路径或重启组件，超出本轮低风险补证范围；因此未复跑 Pod，`AllocateRequest.DevicesIds` 原始输入继续标记 **NotRun**。

完整原始采集与临时构建材料保留在 `/private/tmp/xpu01b-probe-1009/`；本目录仅收录不含 token/secret 的必要摘要、配置和日志。2026-10-09 17:34（Asia/Shanghai）只读复核时，`kind-xpu01b-probe-1009`、两个并发 Pod、mock 与正式 vGPU 插件都仍 Running，留供复核，尚未清理；旧 `kind-volcano-gpu-mvp` 仍在。Volcano 工作树只新增本证据目录，原 vGPU 仓库未改。

后续启用 `deviceshare.NodeLockEnable=true` 的隔离对照、原基线 Pod 删除和当前集群状态，见 [NodeLock 探针报告](./nodelock-enabled/README.md)。上段状态仅是 2026-10-09 17:34 的快照。
