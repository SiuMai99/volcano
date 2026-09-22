# Volcano 通用 xPU 拓扑感知调度 V4：M2 安装与运行证据（PR8）

> 范围：[M1/M2 Advisory MVP 开发计划](./104-generic-xpu-topology-aware-m1-m2-advisory-mvp-development-plan-zh-v4.md)。
>
> 状态：**已在 kind/API server 验证**（2026-09-22）；本文件仍保留可重放的安装/运行验收与证据边界。

## 1. PR8 交付边界

PR8 将已实现的 M2 compiler、soft scorer、hard final guard 和 Annotation Provider 接入 Helm 示例与真实 API server。它不改变
M2 的调度语义：soft 仅在普通 Predicate 给出的候选上追加有界偏好；facts 缺失、删除或过期时得 0 分但不拒绝候选；hard 永远不能进入 Bind。

本 PR 不实现 scheduler-selected UUID、`volcano.sh/xpu-assignment`、Device Plugin bridge、DRA/MIG/vGPU、reservation/ledger 或通用
topology publisher。`MockProvider` 也不作为部署 runtime：当前 scheduler 仅为 `annotation` Provider 配置 Node event refresh bridge；Mock
仅用于单元和 fixture 场景。

## 2. Helm 安装合同

使用 [xpu-topology-aware-values.yaml](../../../installer/helm/chart/volcano/examples/xpu-topology-aware-values.yaml) 安装时，chart 必须：

- 在 scheduler 与 admission 同时设置 `XPUTopologyAwareScheduling=true`；
- 在 scheduler config 中显式注册 `xpu-topology-aware`，Provider 固定为 `annotation`；
- 创建只读 catalog ConfigMap，并挂载为 `/volcano.xpu/catalog.json`；catalog 改动需要重启 scheduler；
- 授予 scheduler 对 `podgroups` 的 `get,list,watch,update` 及对 `podgroups/status` 的 `update,patch`，供 `JobValidFn` 读取并写 runtime reason；
- 保持默认 values 完全关闭：不创建 catalog ConfigMap、不挂载 volume、不注入 plugin 或 gate。

`Node` annotation key 为 `volcano.sh/xpu-topology`。它必须由可信集群组件或管理员发布，workload identity 不应获得 Node patch 权限。当前
scheduler chart 因既有 node-lock 插件仍拥有 Node update 权限，故 PR8 **不把该现状表述为 annotation 写保护已完成**；收紧该权限需要独立
评估 node-lock 的部署合同，不能在本 PR 中删除既有权限并造成回归。

fixture 的 Pod 必须使用 `scheduling.k8s.io/group-name` 指向同名 PodGroup；它是当前 scheduler 的 Job/PodGroup 关联键。
`scheduling.volcano.sh/group-name` 不会让该 scheduler 关联到已创建的 PodGroup，可能被 PodGroup controller 改写为自动生成的 group，因而不能用于
本 PR 的 topology policy 验证。

静态安装合同可先运行：

```bash
bash hack/verify-xpu-topology-aware-helm.sh
```

## 3. kind 运行步骤

前提：集群的两个 worker 通过测试 Device Plugin/L1 fixture 暴露 `nvidia.com/gpu`，且普通 Predicate 能将一个 GPU Pod 放到两者之一。
这只提供普通资源 fit 输入；不构成 DeviceID 或 CUDA runtime 证据。

```bash
helm upgrade --install volcano installer/helm/chart/volcano \
  --namespace volcano-system --create-namespace \
  -f installer/helm/chart/volcano/examples/xpu-topology-aware-values.yaml

kubectl -n volcano-system rollout status deployment/volcano-scheduler
kubectl auth can-i get podgroups.scheduling.volcano.sh \
  --as=system:serviceaccount:volcano-system:volcano-scheduler
kubectl auth can-i update podgroups.scheduling.volcano.sh --subresource=status \
  --as=system:serviceaccount:volcano-system:volcano-scheduler
kubectl auth can-i patch podgroups.scheduling.volcano.sh --subresource=status \
  --as=system:serviceaccount:volcano-system:volcano-scheduler
```

将 fixture 发布到两个具有可用 GPU 资源的 worker；annotation 需是单行严格 JSON：

```bash
small_node=<one-gpu-worker>
large_node=<two-gpu-worker>
small_facts=$(jq -c . docs/design/xpu-topology-aware/testdata/pr8/facts-small.json)
large_facts=$(jq -c . docs/design/xpu-topology-aware/testdata/pr8/facts-large.json)
kubectl annotate node "$small_node" volcano.sh/xpu-topology="$small_facts" --overwrite
kubectl annotate node "$large_node" volcano.sh/xpu-topology="$large_facts" --overwrite

kubectl apply -f docs/design/xpu-topology-aware/testdata/pr8/soft-podgroup.yaml
kubectl apply -f docs/design/xpu-topology-aware/testdata/pr8/soft-pod.yaml
kubectl get pod pr8-soft -o wide
```

soft 场景的验收是：两个 Node 都经普通 Predicate 通过时，1-GPU local Domain 的 Node 获得可重放的 structural exact-fit preference。Pod 的
`spec.nodeName` 只能作为此 fixture 的最终选择记录；不能推导 scheduler-selected UUID。

删除两个候选节点的 annotation 后提交一个新的相同 soft Pod，普通调度仍可继续，xPU preference 为 0：

```bash
kubectl annotate node "$small_node" volcano.sh/xpu-topology-
kubectl annotate node "$large_node" volcano.sh/xpu-topology-
```

hard 场景必须不发生 Bind：

```bash
kubectl apply -f docs/design/xpu-topology-aware/testdata/pr8/hard-podgroup.yaml
kubectl apply -f docs/design/xpu-topology-aware/testdata/pr8/hard-pod.yaml
kubectl get pod pr8-hard -o jsonpath='{.spec.nodeName}{"\n"}'
kubectl get podgroup pr8-hard -o yaml
```

验收：Pod 的 `spec.nodeName` 为空，PodGroup `Unschedulable` condition 的 reason 为 `XPUAssignmentNotEnforceable` 或更早的明确
authoring/readiness blocker。不得以 Pod Pending 或 GPU 数量代替该 reason，也不得出现 `volcano.sh/xpu-assignment`。

## 4. 证据记录

每次 kind 验证保存以下最小、可审计输出：Helm render、scheduler/admission args、catalog ConfigMap、`kubectl auth can-i`、soft Pod 的
NodeName、facts 删除后的新 soft Pod 结果、hard Pod 的空 NodeName、hard PodGroup condition，以及 scheduler 的
`Published xPU topology annotation observation` 日志。该 V(3) 成功日志只记录 Node 名称、`ReplaceFacts`/`ClearFacts`、
`sourceGeneration` 与 `accepted=true`，用来直接证明 annotation observation 已进入 snapshot；不记录 GPU 或 domain ID。记录
Cluster/Kubernetes/Volcano image 版本和 fixture Node 名称；不记录或声称真实 GPU UUID、CUDA runtime 或隔离性能。

```bash
kubectl -n volcano-system logs deploy/volcano-scheduler --since=5m |
  rg 'Published xPU topology annotation observation'
```

### 2026-09-22 实测（kind `volcano-gpu-mvp`，Kubernetes v1.33.12）

- 未使用下载到本地但 revision 为 `d61e…bdc44` 的 OCI chart；`nvml-mock` 由本地模板源码渲染，镜像固定为
  `ghcr.io/nvidia/nvml-mock:sha-c8557c3`。NVIDIA Device Plugin v0.18.2 向 kubelet 注册的可分配资源为 H100 worker=8、L40S worker=8、T4 worker=4。
- 当前工作区 chart 用 scheduler/admission `XPUTopologyAwareScheduling=true`、`annotation` Provider、catalog ConfigMap `/volcano.xpu/catalog.json`
  安装成功；API server 对 scheduler ServiceAccount 的 `get podgroups`、`update podgroups/status`、`patch podgroups/status` 授权均返回 `yes`。
- 1-device facts（T4 worker）与 2-device facts（L40S worker）均已被 scheduler snapshot 吸收后，soft fixture 选择了 T4 worker；这是 Advisory
  structural exact-fit 的运行记录，不是指定 GPU UUID 的证明。
- 删除两个 facts annotation 后，新 soft fixture 仍在 H100 worker Running；facts 缺失没有阻止普通 GPU Predicate 调度。
- hard fixture 的 Pod 始终没有 `spec.nodeName` 或 `volcano.sh/xpu-assignment`；PodGroup 的 `Unschedulable=True` reason 为
  `XPUAssignmentNotEnforceable`，scheduler 日志同时确认跳过 allocate 和 backfill。普通 gang 状态不会再覆盖该 runtime condition。

## 5. 验收矩阵

| 场景 | 通过条件 | 不得宣称 |
| --- | --- | --- |
| chart opt-in | 双 gate、plugin、catalog mount、status RBAC 都被 render | 默认安装启用 xPU |
| soft facts | 普通候选不变且 fixture 选择稳定 | exact free UUID |
| facts delete/stale | 候选仍可调度，xPU 得分为 0 | topology 是 hard filter |
| hard policy | 无 Bind、reason 明确 | hard topology 已执行 |
| L1 resource fixture | Predicate 可见扩展资源 | NVIDIA runtime/L2 已验证 |
