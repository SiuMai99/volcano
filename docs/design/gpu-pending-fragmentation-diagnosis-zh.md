# Pending GPU 作业的节点间碎片化原因诊断

> 状态：需求草案（尚未实现）
>
> 范围：Volcano 调度器；首版仅覆盖整卡扩展资源 `nvidia.com/gpu`
>
> 文档基线：本仓库本地 `master`，`caf21f3d4`（与 fork 的 `origin/master` 一致）
>
> 上游关联工作核对日期：2026-10-17

## 1. 需求背景

GPU 集群可能仍有足够的空闲卡数，却无法为一个需要多张卡的 Pod 找到单个可容纳它的节点。例如三个 8 卡节点分别使用 7、6、7 张卡，空闲量为 1、2、1 张。集群合计空闲 4 张卡，但请求 4 张卡的单个 Pod 无法放到任何节点。

[社区 Issue #3948](https://github.com/volcano-sh/volcano/issues/3948) 已提出这类节点间碎片问题，期望通过迁移运行中的 Pod 整理资源。相关的[设计 PR #4148](https://github.com/volcano-sh/volcano/pull/4148)、[descheduler PR #5](https://github.com/volcano-sh/descheduler/pull/5)、[Reservation API PR #177](https://github.com/volcano-sh/apis/pull/177)和[调度器 PR #4887](https://github.com/volcano-sh/volcano/pull/4887)主要围绕节点使用率检测、迁移、预留和重新调度；核对时均未合入。它们没有定义“针对当前 Pending Pod，解释为何剩余 GPU 合计足够却仍无法放置”的用户可见诊断。本需求只处理这一解释能力，不承担资源整理。

Volcano 已有节点适配失败的汇总及 Pod／PodGroup 调度失败信息，例如 `JobInfo.FitError`、`TaskSchedulingReason` 和 `SchedulerCache.RecordJobStatusEvent`；[Scheduling Reason 设计](scheduling-reason.md)也明确要求保留 Pod `Unschedulable` 原因以兼容集群扩容组件。现有通道可以告诉用户调度失败，但没有为上述场景提供一个经过校验的“节点间 GPU 碎片”结论。

## 2. 需求价值

- **缩短排障时间**：让用户和平台运维直接区分“GPU 总量不足”与“总量足够、单节点放不下”，避免反复检查节点资源或误以为调度器故障。
- **支持容量决策**：提供请求量、候选节点合计空闲量和单节点最大空闲量，使运维能判断是否需要等待释放、人工整理负载或增加合适节点。诊断本身不承诺哪种措施一定有效。
- **提高反馈可信度**：仅在排除其他已知阻塞因素后给出碎片结论；证据不足时沿用现有通用调度失败信息，避免误导用户。
- **形成可独立交付的改动**：复用当前失败原因通道，不依赖正在推进的 Pod 迁移和 Reservation 工作。

## 3. 首版目标与判定口径

1. 对已进入 Volcano 调度流程、请求正整数 `nvidia.com/gpu` 且本轮未能放置的**单个 Pod**，在同一次调度快照中检查候选节点的 GPU 空闲量。
2. “候选节点”指已经满足该 Pod 的非 GPU 约束的节点，包括节点选择、亲和性／污点容忍、CPU／内存和现有调度策略。诊断必须依据调度器实际可用的判断结果；如果现有过滤路径提前返回，无法证明其他约束已满足，就不输出确定的碎片结论。
3. 设请求量为 `R`，候选节点的空闲 GPU 数为 `F1...Fn`。仅当候选节点非空、`sum(Fi) >= R` 且 `max(Fi) < R`，并且没有其他已知失败原因足以解释本轮未放置时，判定为“节点间 GPU 碎片”。统计口径必须使用同一轮调度快照，不混用实时 Node 状态和缓存值。
4. 在现有 Pod `PodScheduled` 条件／`FailedScheduling` 事件的**消息**中说明请求量、候选节点合计空闲量、单节点最大空闲量；Pod 条件的 `Reason` 保持既有的 `Unschedulable`，不新增外部 API 字段。PodGroup 可沿用现有汇总通道显示该 Pod 的原因，但首版不声称已诊断整个 Gang 的可调度性。
5. 无法可靠分类时保留现有失败原因；本功能不改变过滤、打分、抢占或绑定结果。

示例消息（措辞可在实现时与现有事件风格统一）：

```text
GPU fragmentation across nodes: pod requests 4 nvidia.com/gpu; 3 otherwise-eligible nodes have 4 GPUs free in total, but at most 2 on any one node.
```

## 4. 首版范围

### 包含

- Volcano 调度的原生整卡资源 `nvidia.com/gpu`；以单个 Pod 的 GPU 请求和节点可用量为单位。
- 调度失败后的只读诊断与现有 Pod 失败信息通道集成。
- 在诊断证据充分时输出具体数值；不充分时保持现有信息。
- 单元测试覆盖判定边界，并用模拟节点构造可复现的 1／2／1 空闲卡场景。

### 不包含

- 自动驱逐、迁移、预留、资源整理或新的 descheduler 策略。
- vGPU、MIG、DRA、xPU 设备拓扑、卡级 UUID 选择等非整卡标量资源场景。
- 对多 Pod Gang 的整体 GPU 碎片分析；单个 Pod 可放置但 Gang 无法同时启动，不属于首版命中条件。
- 将物理 GPU 使用率、显存使用率或跨节点 GPU 空闲量直接当作 Pod 可放置性的证明。

## 5. 验收场景

| 场景 | 预期 |
| --- | --- |
| 三个 8 卡候选节点分别空闲 1、2、1 张，单 Pod 请求 4 张 | 报告节点间碎片，`R=4`、合计 `4`、单节点最大 `2` |
| 候选节点合计空闲 3 张，Pod 请求 4 张 | 报告既有资源不足原因，不标记碎片 |
| 某候选节点空闲至少 4 张，但 Pod 因其他约束失败 | 不标记碎片；保留真正的失败原因 |
| 仅有 GPU 空闲的节点不满足 CPU、亲和性或污点容忍条件 | 不把这些节点计入候选集合，不误报碎片 |
| Pod 未通过队列准入，或因 Gang `minAvailable` 无法就绪而 Pending | 不以单 Pod 空闲 GPU 汇总解释整个 Job 的失败 |
| Pod 使用 vGPU 或 DRA 请求 | 继续走原有调度失败信息，不进入本诊断 |

验收时还需检查：正常调度结果与加入诊断前一致；重复调度周期不会产生新的无界事件噪声；现有依赖 `Unschedulable` 原因的行为不变。模拟节点测试只能证明调度器诊断逻辑，不能证明真实 GPU 设备分配或运行时 UUID。

## 6. 实现前需确认的边界

- 当前 `allocate` 资源过滤与插件谓词可能提前返回。需要确认如何在**不改变调度决策**的前提下获得足以排除非 GPU 阻塞的证据；若做不到，应收紧命中条件，而不是根据 GPU 数量单独猜测原因。
- GPU 空闲量应取调度会话中与本次放置判断一致的值，并明确 `Idle` 与可能的未来可用资源、抢占资源之间的边界。首版只解释当前可立即放置的容量。
- 输出接入现有 `FitErrors`／`TaskSchedulingReason`／状态更新路径时，需避免覆盖比碎片更直接的失败原因，并保持 PodGroup 汇总含义准确。

## 7. 参考

- [社区需求：GPU fragmentation across nodes，#3948](https://github.com/volcano-sh/volcano/issues/3948)
- [碎片整理设计 PR #4148](https://github.com/volcano-sh/volcano/pull/4148)
- [descheduler 碎片整理 PR #5](https://github.com/volcano-sh/descheduler/pull/5)
- [Reservation API PR #177](https://github.com/volcano-sh/apis/pull/177)
- [Volcano Reservation 调度器 PR #4887](https://github.com/volcano-sh/volcano/pull/4887)
- [Volcano Scheduling Reason 设计](scheduling-reason.md)
