# Volcano GPU scheduling examples

本目录是 [`Volcano GPU 调度设计与源码导读`](../../docs/design/gpu-scheduling-guide-zh.md) 的配套示例。

## 示例列表

| 文件 | 用途 |
| --- | --- |
| `00-vgpu-scheduler-configmap.yaml` | vGPU 的 scheduler 配置参考 |
| `01-full-gpu-job.yaml` | `nvidia.com/gpu` 整卡 Volcano Job |
| `02-vgpu-hami-core-job.yaml` | HAMI-core 软件 vGPU |
| `03-vgpu-dynamic-mig-job.yaml` | Dynamic MIG 硬件切片 |
| `04-dra-gpu-job.yaml` | DRA Queue、ResourceClaimTemplate 和 Volcano Job |
| `05-legacy-gpu-number-pod.yaml` | 已废弃的旧 GPU Number |
| `06-legacy-gpu-memory-sharing-pods.yaml` | 已废弃的旧 GPU Memory Sharing |

## 使用前提

- 所有示例都假设 Volcano 已安装。
- 整卡示例需要 NVIDIA device plugin 发布 `nvidia.com/gpu`。
- vGPU 示例需要 `volcano-vgpu-device-plugin`，并把 scheduler 配置合并到现有 `volcano-scheduler-configmap`。
- Dynamic MIG 还需要 MIG-capable GPU、节点已启用 MIG，并配置合适的 geometry。
- DRA 示例以本仓库 Kubernetes 1.36 依赖使用 `resource.k8s.io/v1`。运行前必须安装与 `gpu.nvidia.com` DeviceClass 对应的 DRA driver，并开启 DRA feature gate 和 CDI。
- 两个 legacy 示例需要旧 Volcano device plugin；只应启用其中一种旧模式。

## 应用示例

```bash
kubectl apply -f example/gpu-scheduling/01-full-gpu-job.yaml
kubectl get vcjob,pod -w
```

vGPU 配置文件是完整 ConfigMap 示例。已有集群通常包含定制 action 和插件，请先对比并合并，不要直接覆盖：

```bash
kubectl get cm -n volcano-system volcano-scheduler-configmap -o yaml
kubectl diff -f example/gpu-scheduling/00-vgpu-scheduler-configmap.yaml
```

DRA 还需要 scheduler 进程参数：

```text
--feature-gates=DynamicResourceAllocation=true
```

如果使用 consumable capacity，再按 Kubernetes 版本开启相应 feature gate。当前提交不能只依靠 `predicate.DynamicResourceAllocationEnable` ConfigMap 参数开启 DRA predicates。

## 互斥关系

以下 `deviceshare` 开关不能同时启用：

- `deviceshare.GPUSharingEnable` 与 `deviceshare.GPUNumberEnable`
- 任一旧 GPU 开关与 `deviceshare.VGPUEnable`

这些约束来自 scheduler 启动时的硬性检查。迁移时应先停用旧模式，再启用 vGPU。
