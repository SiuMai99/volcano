# 诊断 Pending Pod 的节点间整卡 GPU 碎片

Volcano 可以在证据充分时，为请求整卡 `nvidia.com/gpu` 的 Pending Pod 解释一种放置失败：符合其他放置约束的 GPU 节点合计空闲卡数足够，但没有单个节点能容纳该 Pod。诊断只更新原有的调度失败消息，不整理资源，也不改变调度结果。

## 启用

在调度器配置的 `allocate` action 中添加：

```yaml
configurations:
- name: allocate
  arguments:
    gpuFragmentationDiagnosisEnable: true
    gpuFragmentationDiagnosisMaxNodes: 512
```

诊断默认关闭。`gpuFragmentationDiagnosisMaxNodes` 是一次失败诊断允许扫描的最大节点数，默认 `512`；超过上限时保留原有失败原因。将 `gpuFragmentationDiagnosisEnable` 设为 `false` 即可关闭。配置位置与加载方式见[调度器配置指南](how_to_configure_scheduler.md)。

## 如何读消息

命中时，Pod 的 `PodScheduled=False` 条件和 `FailedScheduling` 事件的消息会包含类似内容：

```text
GPU fragmentation across nodes: pod requests 4 nvidia.com/gpu; 3 otherwise-eligible nodes have 4 GPUs free in total, but at most 2 on any one node
```

其中 `3` 是通过其他约束检查、且具备 GPU 资源的候选节点数；`4` 是这些节点的合计**当前空闲整卡数**；`2` 是单节点最大当前空闲整卡数。数值来自 Volcano 本轮调度会话的资源账本，不是物理 GPU 利用率。原有 Pod 条件 `Reason=Unschedulable` 和事件 `Reason=FailedScheduling` 保持不变。PodGroup 仍按原逻辑汇总成员状态，消息不表示整个 Gang 都因碎片无法启动。

## 何时不会显示

诊断只对实际进入 `allocate` 节点检查、请求正整数整卡 `nvidia.com/gpu` 且最终仍 Pending 的 Pod 生效。首版还要求：

- 调度器处于非分片模式，Job 没有显式网络拓扑约束，Pod 没有 nominated node；
- Pod 没有 DRA ResourceClaim，也没有除 `nvidia.com/gpu`、Pod 数量以外的其他扩展标量资源请求；
- 所有启用的 Predicate 都有可用的诊断检查路径；内置 `predicates` 插件需要关闭其 `predicate.CacheEnable`（默认关闭）；
- 相关节点集完整，检查结果没有插件错误，且节点数未超过上限。

若 Pod 未通过队列准入或调度门，或者仅因 Gang 就绪条件被阻塞，Volcano 沿用原有失败原因。总空闲量不足、某单节点已经有足够空闲 GPU、等待资源释放的 Pipelined Pod，以及 vGPU／MIG／GPU 共享等设备语义，也不会得到此诊断。CPU、内存、节点选择、亲和性或污点不满足的节点不计入候选；若无法可靠确认其他约束，也会回退到原有失败原因。**没有显示碎片消息不等于排除了碎片问题**；它表示本轮未达到确定结论所需的全部条件。

本功能不能证明设备插件最终选择的 GPU UUID，也不能保证迁移运行中 Pod 后一定能够放置该 Pod。需要设备级确认时，应另行核对 kubelet 的设备分配和容器内可见设备。
