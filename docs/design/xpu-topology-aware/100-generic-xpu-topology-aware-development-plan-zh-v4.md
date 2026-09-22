# Volcano 通用 xPU 拓扑感知调度 V4 开发计划

> 基线：[V4 设计](./99-generic-xpu-topology-aware-design-zh-v4.md)。本计划只细化其中的 Alpha 范围，不纳入独立的跨系统事务、外部设备生命周期或多 Pod 原子提交需求。
>
> 状态更新（2026-09-22）：本地分支已完成 M1/XPU-02～06 与 M2/XPU-07 Advisory MVP，并在 kind/API server 完成安装、soft
> preference、facts 删除退化和 hard no-Bind 验证；实现基线为 `5cd3f7b7e`。运行证据见
> [M2 安装与运行证据](./105-generic-xpu-topology-aware-m2-install-evidence-zh-v4.md)。本地完成不表示这些提交已经推送、合入上游或成为
> 发布能力；M3 仍未实现，实施计划见
> [M3 Pod-derived Topology Alpha 开发计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)。
>
> 原始计划基线为 2026-09-16、HEAD `7604cc7d3`。下文的“当前源码基线”和“拟新增”描述保留其历史规划语境；判断现状时以
> 上述状态更新、当前源码和对应运行证据为准。
>
> 范围修订（2026-09-20）：原始 Alpha 仍不把 GPU 虚拟化请求形状纳入产品能力；为提前验证 exact UUID 后端，XPU-01 增加一个复用现有 Volcano vGPU/HAMi Adapter 的 L1 证据轨道。该轨道是验证配置，不改变 Alpha 的 vGPU/MIG 支持范围，也不替代后续原生 NVIDIA Device Plugin exact-ID 方案。
>
> XPU-00 已启动；合同冻结草案与 fixture 规范见
> [101-generic-xpu-topology-aware-contract-review-zh-v4.md](./101-generic-xpu-topology-aware-contract-review-zh-v4.md)。

## 1. 交付目标与拆分原则

建议按 **合同冻结 → 数据与启用基础 → Advisory MVP → Pod-derived Topology Alpha → 发布验收** 推进。
V4 中的“PR 1”和“PR 3”各自包含多个跨组件改动，本计划将其拆成可建 Issue 的工作包；较大的工作包还可拆成多个 PR。

最先交付一个可安装、可配置、可解释调度结果的 Advisory MVP；随后以已绑定 Pod 的 NodeName 和 assignment annotation
完成拓扑 Alpha。跨系统事务、外部设备生命周期和多 Pod 原子提交不属于本计划范围。

### 1.1 发布能力分级

| 交付物 | 用户可验证的行为 | 放行条件 |
| --- | --- | --- |
| 数据基础 M1 | catalog、可信 Provider、NodeUID identity、同次 Session snapshot、canonical policy | 配置/API/并发一致性测试通过；尚不作为可用调度功能发布 |
| Advisory MVP M2 | `soft` topology preference、确定性 Compact、缺数据时可解释退化 | gate/plugin 同时有效；hard 仍 fail closed；不创建 per-device allocation owner |
| Pod-derived Topology Alpha M3 | 单 leader 下按 Group 规划 Node/Domain/Fabric；成功绑定的 Pod 保存 scheduler-owned assignment annotation；后续 wave/Session 从 Pod 恢复 anchor | Pod cache 恢复、NodeUID/DeviceKey 校验、缺失/冲突 Pending、普通逐 Pod Bind 回归通过 |
| 发布候选 M4 | 安装、升级、关闭、容量与性能边界可复现；明确声明 Alpha 能力边界 | 对应能力矩阵、E2E、性能报告及运维手册通过 |

贯穿所有工作包的约束：

- 复用 JobInfo/SubJobInfo、Queue/Gang/Predicate/HyperNode 和 Statement；不新建平行调度循环或 readiness 系统。
- `scope + domainClass` 必须贯通 API、catalog、Provider、索引、planner、assignment、anchor、恢复和迁移拒绝。
- hard 不降级为 soft；未知 class 对两种 mode 都是 authoring error。soft 数据不可用只能失去 preference。
- Alpha 复用现有 Statement 与逐 Pod PreBind/Bind；不新增独立跨 Session 提交状态、持久化证据库、ActivityFence 或第二套 commit loop。
- 成功绑定的 Pod 必须保留 `spec.nodeName` 与 scheduler-owned `volcano.sh/xpu-assignment`；后续 Session 由此重建 Group anchor。
- annotation 缺失、DeviceKey 非法、NodeUID 不匹配或 Group anchor 冲突时，hard workload 保持 Pending，不猜测或选择第二个实例。
- 单 active scheduler leader 负责同一调度路径；不设计两个 scheduler 同时更新同一 Group 的 owner/fencing 语义。
- 中间 PR 保持 feature 默认关闭；不新增跨系统 owner、事务层或多 Pod Bind 原子性承诺。

### 1.2 首期范围

包含：Annotation/Mock Provider、Node-local 和显式 Fabric、PodGroup/SubGroup、direct PodGroup canonical authoring、M2 Advisory 与后续
exact Alpha 共用的 multi-container/init/restartable-init device 请求形状、基于已绑定 Pod 的稳定跨 wave anchor、单 active leader，以及
一个 Provider 能否消费/确认 scheduler-selected DeviceKey 的探针。

延期：DRA claim/ResourceSlice、MIG/vGPU/共享几何作为 Alpha workload API、topology-aware victim selection、自动推导 Fabric、通信 ring
优化、外部设备生命周期、多 Pod Bind 原子性、workload 同时启动屏障。
首个 Provider 的厂商目标固定为 NVIDIA；XPU-01 主轨道仍使用 `nvml-mock + NVIDIA Device Plugin` 接入 `nvidia.com/gpu`，并保留 stock
Device Plugin 的 exact-ID 缺口结论。新增的 vGPU 轨道只复用现有 `volcano-vgpu-device-plugin` 作为 L1 Adapter 证据，使用
`deviceSplitCount=1` 的最小单槽位验证配置，不表示首期支持 vGPU 请求形状。HAMi 不是底层设备厂商。

## 2. 源码基线与实际改动面

| 区域 | 已核对的现状 | 对开发计划的影响 |
| --- | --- | --- |
| [配置解析](../../../pkg/scheduler/util.go)、[加载与重载](../../../pkg/scheduler/scheduler.go) | `UnmarshalSchedulerConf` 做部分校验；`loadSchedulerConf` 存在默认配置/上一配置路径 | Alpha 复用现有 plugin 的参数解析语义；xPU 参数在 `New` 中按默认值、告警和安全降级处理，gate/plugin activation 与 catalog/provider fail-closed 另行处理 |
| [Helm values](../../../installer/helm/chart/volcano/values.yaml) | 已有 `scheduler_feature_gates`、`admission_feature_gates`、`scheduler_config_override` | 复用现有参数，增加 xPU 示例和渲染测试，无需重复造开关 |
| [自动 PodGroup](../../../pkg/controllers/podgroup/pg_controller_handler.go) | 有 `buildPodGroupFromPod`、`parseNetworkTopologyFromPod`、`shouldUpdateExistingPodGroup` | 新增 strict xPU canonicalization，不照搬无效 network mode 的默认回退 |
| [VCJob controller](../../../pkg/controllers/job/job_controller_actions.go)、[更新校验](../../../pkg/webhooks/admission/jobs/validate/admit_job.go) | 已有 Job/partition 到 PodGroup/SubGroup 的 NetworkTopology 转换；`validateJobUpdate` 对其他 spec 变化严格限制 | Alpha 不新增 VCJob/partition topology source 转换；后续接入时再覆盖两级转换与 V4 quiescent 更新规则 |
| [JobInfo.SetPodGroup](../../../pkg/scheduler/api/job_info.go) | SubJobs 仅在初次设置或 SubGroupPolicy 变化时重建 | 新 policy 的缓存失效需要覆盖顶层 policy 变化，避免保留旧 SubJob 视图 |
| [ClusterInfo](../../../pkg/scheduler/api/cluster_info.go)、[Snapshot](../../../pkg/scheduler/cache/cache.go)、[openSession](../../../pkg/scheduler/framework/session.go) | 尚无 DeviceTopology；Snapshot 在主锁下取得集群视图 | XPU-06 增加同次捕获，且 Node replacement 与 topology publish 协调 |
| [allocate](../../../pkg/scheduler/actions/allocate/allocate.go)、[recorder](../../../pkg/scheduler/actions/allocate/recorder.go) | 有 worksheet、HyperNode trial、nomination、Save/RecoverOperations 和多处分支提交 | XPU-09 审计所有分支，确保 winning placement 和 assignment annotation 不来自试算副本 |
| [Statement](../../../pkg/scheduler/framework/statement.go) | `Commit()` 无 error；Merge 转移 operations；SaveOperations 克隆 Task operations | Alpha 直接复用现有 Commit，不新增提交事务层 |
| [Bind cache](../../../pkg/scheduler/cache/cache.go)、[PreBinder](../../../pkg/scheduler/cache/interface.go) | `AddBindTask` 单项入队；`executePreBinds` 保留成功项；rollback 无错误结果 | Alpha 保持现有逐 Pod Bind，并验证 assignment annotation 随 Bind 保留 |
| [Condition](../../../pkg/scheduler/framework/session.go)、[JobUpdater](../../../pkg/scheduler/framework/job_updater.go) | Condition 按 Type 替换，再经 JobUpdater 写回 | xPU 与 gang 需要统一 outcome 选择和 controller blocker 保护 |
| [backfill](../../../pkg/scheduler/actions/backfill/backfill.go)、[preempt](../../../pkg/scheduler/actions/preempt/preempt.go)、[reclaim](../../../pkg/scheduler/actions/reclaim/reclaim.go) | allocate 之外还有直接 `Session.Allocate` 和 Statement 提交路径；preempt/reclaim 主要提交 Evict/Pipeline | XPU-13/14 只在 backfill/nomination 等产生 assignment 的路径加 guard，其他 action 做不改变 assignment 的审计 |

上述核对是源码阅读，不是运行验证；本次未发现 V4 命名的 gate、plugin、snapshot 或 group anchor 的 Go 实现。

## 3. M0：先冻结哪些开发合同

M0 不是重写设计。输出短决策记录、接口草案与反例用例，解决会导致多个组件返工的问题。
“建议起点”用于安排评审，不表示最终 API 已确定。

| 决策 | 建议起点与必须产出的结论 | 阻塞的工作 |
| --- | --- | --- |
| D1：catalog 权威载体 | 固定 Alpha class 定义，使用 scheduler-side 静态配置/安装输入；不冻结跨组件共享读取协议，不实现运行期间更新 | XPU-03、05 |
| D2：Pod-derived anchor 与 assignment | 复用已绑定 Pod 的 `spec.nodeName` 和 scheduler-owned `volcano.sh/xpu-assignment` 重建 anchor；直接扫描成员 Pod，不引入独立持久化记录或摘要状态 | XPU-11、14 |
| D4：首个 Provider | 保持 generic contract：scheduler-selected DeviceKey 能被 Provider/Device Plugin 消费或确认，并能写入/保留 Pod assignment annotation；XPU-01 同时报告 stock 路径缺口和现有 Volcano vGPU Adapter 的 exact UUID 证据，两者不合并 | XPU-01 |
| D5：API 与 schema | 冻结 class 名语法、默认值、selector 冲突判定和 strict JSON；大小上限与 legacy tombstone 仅在实现/实际 served 版本需要时确定 | XPU-03、04 |
| D6：authoring 与 anchor activity | 已绑定成员存在时禁止 topology policy semantic mutation；复用 PodGroup 的现有 resourceVersion/generation 更新冲突，不新增 ActivityFence；单 leader 读取一致缓存 | XPU-04、11 |
| D7：激活与 action 兼容 | 冻结 gate/plugin/catalog fail-closed；只对能产生 assignment/Bind 的 allocate/backfill/nomination 做 guard，其他 action 做不变性审计 | XPU-02、13、14 |
| D8：组合与状态归属 | 多 SubGroup、多 resource policy 仍在同一 Statement/Session 中求约束交集；每个 Pod 一份 assignment，Group anchor 从已绑定成员重建，不建立共同外部 owner | XPU-08、11、13 |

两项需要写进决策记录的实现细节：

1. **gate 关闭时允许删除 policy** 仍受 V4 §3.5 的 quiescent 限制；已有绑定成员时不能借删除绕过保护。
2. `soft` 只做 preference，不建立 hard anchor。MVP 如何体现 Group soft preference 要给出可复现评分例子；不能让软约束排除普通可行 Node。

### XPU-01：NVIDIA Provider identity 探针

在大规模修改 Statement 前，针对 NVIDIA Provider 完成分层小实验。XPU-01 分成两个互不混淆的 L1 轨道：

1. **Stock 轨道**：`nvml-mock + NVIDIA Device Plugin + nvidia.com/gpu`，验证原生接口的能力边界；如果只能得到数量、`GetPreferredAllocation` 或 kubelet 最终 ID，selected UUID 结论必须是 `XPUAssignmentNotEnforceable`。
2. **VGPU Adapter 轨道**：`nvml-mock + volcano-vgpu-device-plugin + Volcano deviceshare`，验证已有 vGPU/HAMi 路径能否把 UUID allowlist 传到 `vgpu-ids-new` 和 `NVIDIA_VISIBLE_DEVICES`。该轨道使用 `deviceSplitCount=1`、`volcano.sh/vgpu-number=1` 的单槽位验证配置；它证明的是现有 Adapter 的 exact UUID realization，不是 stock Device Plugin，也不是 Alpha 的 vGPU 产品支持。

真实 NVIDIA 节点只作为后续发布验收的可选补充，不作为 Alpha 合同前置：

1. 从 nvml-mock 枚举稳定 NVIDIA GPU UUID，选择非默认 UUID，验证 Provider 能将 canonical DeviceKey 传递到 Pod assignment annotation。
2. mock 层核对 NVML/Device Plugin inventory、plan 与 annotation；真实硬件层再从实际容器/NVIDIA runtime 读出设备身份，与 Pod 的
   PodUID、ResourceName、NodeUID 和 DeviceKeys 对照。
3. 重读已绑定 Pod，验证 NodeName + assignment annotation 可以恢复同一 Node-local/Fabric anchor；NodeUID 变化必须 Pending。
4. 注入 annotation 缺失、非法、重复 DeviceKey、topology 映射冲突，验证不会猜测或建立第二个 anchor。
5. 明确 nvml-mock 能覆盖的 Fabric/topology/health 故障与真实 NVIDIA Fabric 验收边界。

6. 在 VGPU Adapter 轨道中同时记录 `volcano.sh/vgpu-use-gpuuuid`、`volcano.sh/vgpu-ids-new`、`volcano.sh/devices-to-allocate` 和
   容器内 `NVIDIA_VISIBLE_DEVICES`；明确该 allowlist 是当前 vGPU 案例的用户输入，不等同于 generic scheduler-owned `DeviceKey`。

产物是 NVIDIA Provider 能力表、stock/vGPU 分轨日志与断言、尚缺的协议和硬件清单。nvml-mock 能证明身份和拓扑映射行为，不能替代真实
容器 UUID 验收；真实层未运行时记录为未验证，不阻塞 Alpha。Provider 不能消费 scheduler-selected DeviceKey 时，Alpha 对相关 hard workload 保持 Pending。

## 4. 依赖、工作包和估算

### 4.1 依赖总览

图展示主线和可并行分支；具体依赖以 4.2 表为准。

```mermaid
flowchart TB
    C["M0 合同与 Provider identity 探针"]
    A["API 与 canonicalization"]
    B["激活与进程生命周期"]
    D["Provider 与同次 Snapshot"]
    E["M2 Advisory MVP"]
    P["Group planner"]
    T["Pod-derived anchor 与 assignment"]
    I["Alpha 集成与旁路审计"]
    R["Provider identity probe"]
    F["M3 Pod-derived Alpha"]
    V["M4 回归 性能 drain 与发布"]
    C --> A
    C --> B
    C --> R
    A --> D
    B --> D
    A --> E
    D --> E
    E --> P
    P --> T
    P --> I
    T --> I
    I --> F
    F --> V
    R --> V
    classDef base fill:#e7f5ff,stroke:#1971c2;
    classDef deliver fill:#d3f9d8,stroke:#2f9e44;
    class C,A,B,D base;
    class P,T,I,R base;
    class E,F,V deliver;
```

### 4.2 可直接建 Issue 的工作包

估算单位为工程人日，含实现、对应测试和一次正常代码评审修改；不含等待 maintainer、硬件采购、厂商协议开发及大规模基础设施搭建。
它是基于当前拆分的初估，M0 和 XPU-01 后必须重估；并行不会减少总人日。

角色：A = API/controller；S = scheduler/cache/framework；R = runtime Provider；Q = 测试/发布。角色可由同一人承担。
依赖列表示完成/合入所需条件；接口冻结后可先写纯逻辑、Fake 和测试。

| ID | 工作包 / 建议 PR 主题 | 依赖 | 主责 | 人日 |
| --- | --- | --- | --- | --- |
| XPU-00 | [冻结 API、Pod-derived anchor、assignment 与验收合同](./101-generic-xpu-topology-aware-contract-review-zh-v4.md) | 无 | A/S/R | 4～6 |
| XPU-01 | Provider identity/selected DeviceKey 探针与硬件验收方案；包含 stock negative case 和现有 vGPU Adapter L1 evidence | 00 的初版合同；结果反哺 D4 | R/Q | 3～5 |
| XPU-01B | 后续 native NVIDIA exact allocation bridge 设计：scheduler-owned assignment、Provider 校验、kubelet `DevicesIds` 对账和 runtime reconciliation | XPU-01 stock gap；不属于当前 vGPU 验证实施 | S/R | 后续单独估算 |
| XPU-02 | Feature/plugin activation guard、process manager 骨架 | 00/D7 | S | 4～6 |
| XPU-03 | PodGroup Public API、scheduler-side catalog、canonical types 与生成链 | 00/D1/D5 | A/S | 6～9 |
| XPU-04 | direct PodGroup canonicalization、mutation、Condition 聚合 | 02、03；D6 | A | 6～9 |
| XPU-05 | Annotation/Mock Provider、Normalizer、可信发布 | 02、03 | S | 6～9 |
| XPU-06 | topology live cache、NodeUID、immutable paired snapshot | 02、03、05 | S | 6～9 |
| XPU-07 | plugin/compiler、soft score 与 Advisory MVP | 04、06 | S | 4～6 |
| XPU-08 | allocate 私有 Group planner、四种语义与有限搜索 | 07；D8 | S | 7～11 |
| XPU-09 | Statement/Session Group plan 与提交分支审计 | 08 | S | 3～5 |
| XPU-11 | Pod-derived anchor、assignment annotation 与恢复 | 03、08；D2/D6 | S/R | 4～6 |
| XPU-13 | Alpha 提交 guard 与所有 action 入口防绕过 | 08、11；D7/D8 | S | 4～6 |
| XPU-14 | Alpha Pod/Node recovery 与 assignment-producing 旁路审计 | 11、13 | S | 4～7 |
| XPU-16 | 后续发布阶段的 Mock/真实 Group、Fabric、失败注入 E2E | 14 | Q/S/R | 6～10 |
| XPU-17 | 后续发布阶段的 metrics、性能与并发容量基线 | 07 可启动；Alpha 部分依赖 13/14 | Q/S | 4～6 |
| XPU-18 | 后续发布阶段的安装、升级/drain/回滚与发布清单 | 04、16、17 | A/Q | 3～5 |

Alpha 主线约 **61～94 工程人日**（XPU-00～09、11、13、14）；后续发布验收 XPU-16～18 另计约 **13～21 工程人日**。
M2 所需 XPU-00～07 约 **39～59 人日**，其中包含提前执行的 Provider identity 探针。
这些不是已投入工时，也不是交付承诺。

### 4.3 与 V4 原 PR 划分的对应

| V4 粗粒度阶段 | 本计划工作包 |
| --- | --- |
| PR 0：contract review | XPU-00、01 |
| PR 1：model/provider/identity | XPU-02、03、05、06 |
| PR 2：snapshot/Advisory | XPU-04、06、07 |
| PR 3：Topology Alpha 与旁路保护 | XPU-08、09、11、13、14 |
| PR 4：Fabric/Group E2E 与发布 | XPU-16～18；Fabric 模型与纯规划提前在 05/08 落地 |

## 5. M1/M2：先完成可运行的 Advisory 路径

详细实施顺序、PR 拆分和验收门槛见
[104-generic-xpu-topology-aware-m1-m2-advisory-mvp-development-plan-zh-v4.md](./104-generic-xpu-topology-aware-m1-m2-advisory-mvp-development-plan-zh-v4.md)。

> 实施状态：本地 M1/M2 已按 XPU-02～07 完成；M2 仍只提供 soft Advisory，所有 hard policy 保持 fail closed。安装与运行证据见
> [105-generic-xpu-topology-aware-m2-install-evidence-zh-v4.md](./105-generic-xpu-topology-aware-m2-install-evidence-zh-v4.md)。

### XPU-02：激活与进程生命周期

**落点**：现有 `pkg/features/volcano_features.go`、`pkg/scheduler/{util,scheduler}.go`、
`pkg/scheduler/plugins/factory.go`；新增 manager 与 activation guard，目录在 M0 冻结。

- 注册默认关闭的 `XPUTopologyAwareScheduling` 和 `xpu-topology-aware` builder；复用两个进程的 feature-gate 参数。
- `New` 按现有 plugin 规则解析参数：建立默认值，类型/格式/范围错误告警并回退；Provider 配置无法安全使用时标记不可用，相关 xPU policy 保持 Pending。
- activation guard 只覆盖 gate/plugin 组合、scheduler-side catalog、Provider identity 和 assignment annotation contract，不生成跨组件 digest；plugin arguments 继续由 `New` 按现有规则处理。
- gate/plugin 组合或 catalog/contract 无效时，相关 xPU policy 不进入普通调度；Alpha 不冻结热更新、旧配置保留或跨组件 drain 状态机。
- feature gate 是进程启动参数；关闭、升级和回滚的 drain 步骤属于后续发布验收。
- 将 catalog/config readiness、Provider readiness、单 Node freshness 区分；单 Node 未同步只影响相应候选。
- process manager 持有 Provider/cache/topology snapshot；per-Session plugin 仅持只读 view、Group anchor 和 plan overlay。
- 在 core 层保护存量 typed policy 与 authoring blocker，不能依赖被关闭 plugin 的 callback 才拒绝任务。
- Alpha 只检查已绑定 Pod 的 policy mutation 和 scheduler leader 生命周期；不引入独立 activity/recovery 接口。

**完成条件**：四种 gate/plugin 组合、参数解析的默认/告警/安全降级、catalog/Provider 不可读和 Session 重建均有测试；manager 不重复启动；
两个 gate 都关闭时普通 workload 行为一致。重载、升级和 drain 不作为 Alpha 前置。

### XPU-03：API、catalog 与 canonical model

**落点**：现有 `staging/src/volcano.sh/apis/pkg/apis/{scheduling,batch}/`、CRD 生成目录；
新增共享 canonicalization/catalog/model 包。共享逻辑不得依赖 scheduler 实例或运行时分配状态。

- 增加 PodGroup/SubGroup policy typed field；VCJob/partition ergonomic 字段延期，不作为 Alpha API 依赖。
- 实现 default/validate/stable sort/dedup/canonical equality；canonical selector 排序、重叠 selector 检测、多 policy 冲突使用同一实现。
- 定义 NodeUID-safe DeviceKey、LocalDomainKey、FabricKey、DomainClassKey、Node/Fabric class-bearing model。
- 实现 scheduler-side 固定 catalog 读取；不实现跨组件共享读取、历史 catalog 或运行期间切换。
- Go/canonical model 不包含 `tier/tierName`；只有实际 served V3 的 version 才按需增加 reject-only tombstone + CEL。
- 明确 `allocationStrategy` 等禁用字段在 typed schema 与 annotation 的拒绝路径，不能只依赖客户端 Strict。

**完成条件**：direct PodGroup typed input 得到 canonical spec；不同 resource/scope 的同名 class 不冲突；
通过真实 API server schema 测试证明旧字段被拒绝，而非先 prune 后接受；生成链重复运行无差异。

### XPU-04：controller/webhook 与可解释状态

**落点**：现有 `pkg/controllers/podgroup/pg_controller_handler.go`、`pkg/controllers/job/job_controller_actions.go`、
`pkg/webhooks/admission/{jobs,podgroups,pods}/`、`JobInfo.SetPodGroup`、Session/JobUpdater/status update 路径。

- 支持 direct PodGroup typed field；VCJob、Deployment/ReplicaSet、StatefulSet、bare Pod authoring 延期。
- typed canonicalization 拒绝重复 JSON key、未知字段和无效类型；不冻结多源优先级或 owner/template 冲突系统。
- 解析失败时保留 blocker；scheduler 不直接读 workload annotation。
- 实现 quiescent/no-op/活动 mutation 规则，复用已有对象的 resourceVersion/generation CAS。
- M2 scheduler 与后续 exact Alpha 共用请求形状校验：每条 resourceName 只由一个 regular/init/restartable-init container 声明，且 limit
  为正整数；request 可省略，若存在则必须与 limit 相等；exact Alpha 的 assignment handoff 额外持久化该容器引用。
- 统一 outcome：controller authoring blocker 优先，之后具体 xPU reason 优于泛化资源不足；解决后清除陈旧 True，保留其他 status 字段。

**完成条件**：direct PodGroup、活动期删除、合法 no-op、status conflict retry 和 scheduler blocker 均覆盖；scheduler catalog 不可读时 fail closed。
D6 的绑定成员与 semantic mutation 验收在 XPU-11/13 回补。

### XPU-05：可信 Provider 和 Normalizer

**落点**：新增 `pkg/scheduler/topology/provider/`（建议）与测试 fixtures；现有 Node informer/workqueue 入口。

- 实现 Annotation 与 Mock 两种输入；输出相同 ReplaceFacts/ClearFacts、Pending/Synced、generation/freshness 合同。
- Node RV 只做相等验证；同 generation 仅接受内容一致 heartbeat；invalid payload 保留最后有效事实但不刷新有效期。
- 规范化 catalog class、membership closure、去重、无环、resource/scope；禁止从 Link、Node 或 HyperNode 猜 hard Domain。
- Node-local facts 与 single-owner complete Fabric 分开验证；Fabric member 必须解析到当前 UID/generation/local Domain。
- Provider capability 只能引用固定 catalog 中已定义的 class；Provider 不可用不阻断有效 soft facts 发布。
- scheduler planner 拥有 DeviceKeys 的最终选择权；Provider 只验证/消费 selected DeviceKeys，不提供 chooser 或替换 plan。
- 提供可信 publisher 的最小输入规范、RBAC 与 admission 保护；拒绝 workload/scheduler 身份篡改 topology annotation。

**完成条件**：重复 ID 跨 Node 合法、同 Node 冲突非法；错误 class、循环成员、乱序更新、owner/member 变化、selected DeviceKey 校验与伪造发布全部有测试。
首期不额外开发通用硬件发现 DaemonSet；真实 facts 采集器是否需要由 XPU-01 确认，并据此调整实现范围。

### XPU-06：live cache、identity 与同次 snapshot

**落点**：现有 `pkg/scheduler/cache/{cache,event_handlers}.go`、`api/cluster_info.go`、`framework/session.go`；
新增 topology cache 与 immutable indexes。

- 协调 Node identity 变化、Provider publish 和主 cache：只出现“旧 UID + 旧 view”或“新 UID + Pending”。
- 保留 paired snapshot 与 immutable view；parse、closure、Provider RPC、持久 I/O 在锁外。具体锁序和内部并发实现不进入 Alpha contract。
- Snapshot 在同一临界区获取 Node/Job/Queue 与 immutable topology pointer；plugin 不再次 snapshot。
- 建立满足 planner 所需的最小 class/domain/device 索引，发布后 map/slice 不可变；其他派生索引按实现阶段需要再增加。
- Node/Fabric replacement 使旧候选失效；已绑定 Pod 使用旧 NodeUID 时标记 anchor 不可恢复，不能把旧 key 重新放入候选。
- 区分 facts、health、Session-local placement overlay；不把 Node aggregate 数量当单个 Domain 的可用数量。

**完成条件**：并发 Node replacement、旧 UID ClearFacts、catalog 缺失/非法、source 删除、旧 Session 不变、并发/race 测试通过；不得在持锁期间执行解析、RPC 或持久化 I/O。
Alpha 只验证 Pod-derived anchor 的失效与 Pending，不管理外部设备 owner 生命周期。

### XPU-07：Advisory MVP

**落点**：新增 `pkg/scheduler/plugins/xpu-topology-aware/`（建议），复用 JobValid/Predicate/BatchNodeOrder/EventHandler。

- compiler 使用 canonical spec、DomainClassKey 与 immutable view；分清 unknown、unsupported、Pending、stale、not enforceable。
- 实现只对有证据 topology 评分的 soft preference 与 deterministic Compact；稳定排序与 tie-break 可复现。
- Node/Fabric membership feasibility 做成可复用纯函数，供后续 hard planner 使用；soft 不因 topology 缺失过滤 Node。
- Group soft 使用既有成员/Session placement 的可解释偏好，不建立持久 hard anchor；不跨 Session 保留未经 Pod 事实确认的设备状态。
- 配置相同、workload 无 policy 时，不改变其他 plugin 的候选与评分；不注册第二个 HyperNode gradient 或 JobReadyFn。
- Provider identity 或 assignment contract 不满足时，相关 hard policy 明确失败，不静默降级为另一种设备身份。

**M2 验收演示**：安装 gate/plugin → 部署固定 catalog 并提交 facts → 用 direct PodGroup typed policy 调度 soft workload →
观察确定性 preference → 删除/过期 facts 后普通候选仍可调度 → 同一请求改 hard 后获得明确不可执行原因且无 Bind。

## 6. M3：Pod-derived Topology Alpha

这一阶段完成首个可用的 hard topology Alpha：Group anchor 从已绑定 Pod 的 `spec.nodeName` 与
`volcano.sh/xpu-assignment` annotation 恢复，复用现有 Statement 与逐 Pod Bind。它不承诺跨系统设备生命周期或多 Pod Bind 原子回滚。

> 实施状态：待开发。以下总体工作包已细化为
> [M3 Pod-derived Topology Alpha 开发计划](./106-generic-xpu-topology-aware-m3-pod-derived-alpha-development-plan-zh-v4.md)；在 XPU-01B
> 证明 selected DeviceKey 可被执行/确认、assignment 可持久化并恢复前，不得把 `AssignmentContractReady` 置为生产可用。

### XPU-08：allocate 内部 side-effect-free Group planner

**落点**：新增 plugin 私有 planner/model 包，在 `allocate` 的 winning Statement 路径调用；不扩展通用 framework 注册合同，不接管 gang readiness。

- 以 PodGroupUID/SubGroupID 标识稳定 Group，输入本轮待调度成员、普通候选交集、固定成员及 anchor。
- 分别实现 Pod+Node、Group+Node、Pod+Fabric、Group+Fabric；验证本地域与 Fabric policy 组合，以及 Job/SubGroup 双层约束。
- Group+Node 共享具体 LocalDomainKey/NodeUID；Group+Fabric 共享显式 FabricKey；Pod policy 允许各 Pod 选择不同实例。
- Fabric-only policy 使用显式成员设备并集，不隐含每 Pod 单一本地域；额外 Pod+Node policy 才增加该限制。
- 基于 plan-owned NodeInfo clone 重放 CPU/内存/Pod 资源与已有固定成员；DeviceKey 独立去重，重叠 Domain 不重复计数。
- 固定 Node 的 finalization 必须使用 AssignedNodes；不可局部换 Node/Device 后沿用旧 group plan。
- 实现 deterministic Compact；需要回溯时使用实现阶段确定的有限预算，具体参数不作为 plugin 配置暴露。
- 返回 Alpha 必需的 immutable placements、NodeUID、DeviceKeys 和 assignments；规划阶段不产生外部副作用。

**完成条件**：V4 §11.4 场景表全部变成表驱动测试；随机打乱输入仍得到相同 plan；必要的有限回溯案例有覆盖；
多 Pod 分别可放但合计 CPU/内存不足的候选被拒绝；调用 Fake 证明 planning 不访问外部 allocator。

### XPU-09：Statement/Session plan 与提交分支审计

**落点**：现有 `actions/allocate/{allocate,recorder}.go`、`framework/statement.go` 与对应测试。

- 审计普通、hard network topology、SubGroup、nomination fast path 的每个 Statement 提交分支，确保最终 Task placement 和
  `xpu-assignment` 都来自 winning Statement，而不是某个试算副本。
- 复用 `SaveOperations/RecoverOperations` 复制可试算的 Task/Node 状态，不把试算副本当作已提交事实。
- 对已 Ready Group 的新增 Task，先从已绑定 Pod 恢复 anchor，再与本次 Session 的 plan 求交集；不能为新 wave 选择第二个 Domain/Fabric。
- 普通 workload 继续使用现有 `Commit()`；Alpha 不新增新的提交入口或平行 Statement。

**完成条件**：试算多个 HyperNode、恢复最佳 Statement 后，只有 winning placement 进入 Bind；Already Ready 和多 SubGroup 不漏成员；
同一输入重复规划得到相同 DeviceKeys，且 planner 没有外部 allocator 副作用。

### XPU-11：Pod-derived anchor、assignment annotation 与恢复

**落点**：`pkg/scheduler/framework/session.go` 的 Session/HyperNode 恢复路径、Pod Bind annotation 传递路径、Pod/Node cache event 与
新增 topology model/provider 包；不新增 PodGroup status 字段作为唯一事实源。

- 定义 scheduler-owned `volcano.sh/xpu-assignment` canonical envelope：每条目标 resource 恰有一个 assignment，包含
  `ContainerRef(kind/name)`、`resourceName`、`provider` 和 canonical `deviceKeys`；不加入无消费者的
  Domain/Fabric/Group/plan 派生字段。
- 使用已绑定/运行 Pod 的 `spec.nodeName` 作为 Node placement；`deviceKeys` 必须包含 NodeUID-safe identity，不能只使用 GPU index。
- 从 `AllocatedStatus` 且 `NodeName` 非空的成员 Pod 解析 annotation，并用同一 immutable topology snapshot 映射 LocalDomain/Fabric。
- `Group + Node` 要求所有已绑定成员位于同一 LocalDomain；`Group + Fabric` 要求成员存在共同 Fabric；缺失、冲突或无法映射时 hard Pending。
- 首 wave 尚无已绑定成员时，anchor 仅为 Session-local plan；Bind 成功后由后续 Session 从 Pod cache 重建，不提前写 PodGroup。
- 不写入 PodGroup anchor 摘要；恢复直接扫描成员 Pod，避免引入可过期的重复状态源。
- 已绑定成员存在时拒绝 topology policy semantic mutation；复用现有 resourceVersion/generation 更新冲突，不新增 ActivityFence。
- 单 active scheduler leader 负责同一 Group；不实现 active-active owner、RecordRevision 或 SchedulerEpoch。

**完成条件**：`minAvailable=4, replicas=10` 首 wave 的已绑定 Pod 提供同一 anchor，后续 6 个继续使用该 anchor；Session 重启后仍能从 Pod
恢复；缺失/冲突 annotation、NodeUID 替换和 Fabric 映射失败都保持 Pending；annotation 在 Bind 后仍可读。

### XPU-13：Alpha 提交 guard 与所有 action 入口防绕过

**落点**：allocate、`Session.Allocate/dispatch`、cache bind submission；审计所有能进入 Bind 的 action/shortcut。

Alpha 主链固定为：

```text
Group/Pod policy
  -> Session-local Group plan
  -> Statement.Allocate 选择 Node + canonical DeviceKeys
  -> BindContext/Task Pod 保存 volcano.sh/xpu-assignment
  -> 现有逐 Pod PreBind/Bind
  -> 后续 Session 从已绑定 Pod 恢复 anchor
```

- core final guard 验证 hard topology Task 能生成合法 assignment annotation；不能生成时保持 Pending/返回明确 reason。
- backfill、nomination 等能产生新 assignment/Bind 的旁路必须经过相同的 policy/assignment guard；不能绕过 hard constraint。
- preempt/reclaim/eviction、gang*、shuffle 只审计不创建/改写 assignment，不把 eviction 当成外部设备已释放的证明。
- 每个成功绑定 Pod 只有一份 assignment annotation；同一 Group 不允许因旁路产生第二份 anchor。

**完成条件**：普通逐 Pod Bind 回归通过；所有 hard topology 入口要么写入可校验 annotation，要么明确 Pending；
`bound-pod-anchor-recovery`、`xpu-assignment-conflict-pending` 和旁路 fixture 通过；不出现双 scheduler/双 allocator owner。

### XPU-14：Alpha Pod/Node recovery 与旁路审计

**落点**：Session open/rebuild、Pod/Node cache event、preempt/reclaim/eviction 和 action bypass audit。

- Session 打开时只从已绑定/运行 Pod 重建 anchor；无已绑定成员不强行创建 anchor，未绑定 Pod 仍按本轮 plan 调度。
- Pod assignment annotation 缺失、格式非法、DeviceKey 不属于 Node 或 NodeUID 已替换时，相关 Group hard 调度保持 Pending。
- 同一 Group 的成员 anchor 冲突时不覆盖、不选择第二个实例，记录可重建 reason 并等待事实修复。
- 同一 active leader 负责调度路径；不增加 follower owner 或 fencing。
- Pod 删除、eviction、FutureIdle 和 pipeline 不被解释为外部设备已释放；可用性由现有 cache/provider 更新反映。
- 不实现 activation reload/drain 状态机；只检查 feature/plugin/catalog 失配时 fail closed、policy mutation 和 Pod 生命周期。

**M3 完成条件**：Session 重启、Pod/Node event、Node replacement、annotation 缺失/冲突和旁路场景均有测试；普通 workload 不受影响；
关闭 Alpha 的升级/drain 验收延期到 M4 发布阶段。

## 7. M4：E2E、指标与发布

### XPU-16：E2E 与故障矩阵

**落点**：现有 `test/e2e/`，新增 xPU suite/fixtures（具体目录评审后确定）；复用 kind/KWOK 和既有 E2E harness。

- Mock/kind：验证 webhook/schema/controller、API server Bind 次数、Node replacement、两阶段 wave、失败与恢复。
- KWOK/模拟规模：后续发布阶段再验证 Node/Domain/Fabric churn、候选索引及 cache 并发压力，不作为 Alpha 合同前置。
- 真实设备：选中 ID、Pod assignment annotation 和 runtime identity；真实 Fabric 作为独立后续环境项记录。
- Alpha 故障测试断言绑定 Pod 的 NodeName/annotation、anchor 数量、状态和 reason；不扩展为外部 owner 或原子回滚断言。
- 新增 CI job 的运行环境/资源标签与失败日志归档；硬件 job 没运行时报告“未验证”，不能由 Mock 结果替代。

### XPU-17：指标与后续性能基线

最小指标：provider/snapshot/plan/recovery 结果和低基数 outcome。原始 DeviceID/PodUID/token 不进入 metrics label；具体指标名、
reason taxonomy、budget 计数和日志长度由实现阶段确定。

plugin disabled、enabled-no-policy、soft、Mock hard 的性能对比，及 P50/P95/P99、吞吐、CPU/RSS、锁等待和 GC，全部降为后续发布验收。
Alpha 只要求 planner 有有限实现预算，超过预算保持 Pending，不冻结数值阈值。

### XPU-18：用户配置与发布操作

- 提供可执行的 gate/plugin、scheduler-side catalog、trusted publisher 和 soft/hard 示例，并说明 assignment annotation 是
  scheduler/provider 运行时字段，不是用户 policy 输入。
- 提供 direct PodGroup 示例；VCJob、Deployment PodTemplate、StatefulSet、bare Pod ergonomic authoring 延期，不把 topology annotation
  当成 gang 一次调度全部副本的保证。
- Helm render 核对 scheduler/admission 同名 gate 与 plugin；保持默认安装关闭。
- 升级、policy 变更、drain、关闭 plugin/gate、重启恢复和回滚属于后续发布验收；Alpha catalog 不允许运行期间修改；已有绑定 Pod 仍不能
  放行会改变 topology 语义的 policy 更新。
- V3 `tier/tierName` 只有在确认实际 served 过时才做显式 catalog 映射和 reject-only 兼容测试。
- 发布说明列出 Advisory、Pod-derived Topology Alpha、真实 Node-local/Fabric 环境的验证范围与不支持请求形状。

**M4 完成条件**：后续发布验收在干净环境验证安装、升级、关闭、回滚、性能和真实硬件范围；这些结果不回写为 Alpha 已实现功能。

## 8. 验收追踪：每个关键要求由谁交付

所有编号 Txx 是拟新增测试场景，当前文档不表示测试已存在或已经运行。

| 测试族 | 必须断言的反例/成功条件 | 工作包 | 最低验收层级 |
| --- | --- | --- | --- |
| T01 激活 | gate/plugin 四组合；参数解析的默认/告警/安全降级；catalog/Provider 不可用时相关 policy fail closed | 02 | 单元 + 安装集成 |
| T02 存量保护 | config/gate 失配时已有非空 policy、空 spec 的冲突 blocker 均不能走普通路径 | 02、04、13 | 调度集成 |
| T03 canonicalization | direct PodGroup typed spec canonicalize；重复/乱序不变；未知/冲突字段拒绝 | 03、04 | 单元 + API server |
| T04 schema pruning | 仅对实际 served V3 的 version 验证旧 tier/tierName reject-only；无 served V3 时不新增兼容字段 | 03 | 真实 API server（如适用） |
| T05 更新 | 无绑定成员时 quiescent 更新成功；已有绑定成员的 semantic mutation/删除失败 | 04、11、13 | controller + 调度集成 |
| T06 身份 | 两 Node 同 source ID 不冲突；Node 同名换 UID 后 Pending；旧 update 不污染新 UID | 05、06 | 单元 + race |
| T07 catalog | scheduler-side 固定 resource/scope/class 匹配；catalog 缺失/非法时 fail closed；不产生第二份 class 定义 | 03、05、06 | 单元 + 集成 |
| T08 Provider | invalid 不完成初次同步；freshness/generation 正确；ClearFacts 不伪造已绑定 Pod 的 assignment/anchor | 05、06、14 | 单元 + 集成 |
| T09 Fabric authority | 单 owner 完整声明；member/owner 变更立即失效；同 HyperNode 不推导 Fabric | 05、06 | 单元 + E2E |
| T10 Snapshot | 并发 publish 无新 Node/旧 UID 混合；旧 view 不变；RPC 不在锁内 | 06 | race + 压力 |
| T11 soft | 缺数据/Provider 只失去 preference；不产生 per-device owner；候选不因软约束缩小 | 07 | plugin + E2E |
| T12 Domain fit | 6+2 不能满足单 Domain 8；local-scale-up 4 不扩大为 pcie-root 8 | 08 | 单元 + Mock E2E |
| T13 四种量词 | Pod 可各选 Domain，Group 必须同实例；Fabric-only 与本地+Fabric 组合不同 | 08 | 单元 + Mock E2E |
| T14 Group 完整性 | Ready 后新增、多个 SubGroup、多 resource、固定 Running 成员均纳入约束 | 08、09、11、13 | 调度集成 |
| T15 规划边界 | 普通 Predicate 淘汰节点不复活；总 CPU/内存不超量；实现预算耗尽保持 Pending | 08 | 单元 |
| T16 试算回滚 | trial 无外部调用；Save/Recover/Merge 只复制可试算的 Task/Node 状态；winning placement 唯一 | 09 | 单元 + 故障注入 |
| T17 最终校验 | 无关 heartbeat 不使计划失效；引用的 Domain/Fabric membership 改变必须重算；NodeUID/DeviceKey 冲突保持 Pending | 08、11、13 | 并发 + 故障注入 |
| T18 绑定 annotation | 成功绑定 Pod 的 NodeName/assignment 可读；缺失/非法 annotation 不建立 anchor；普通逐 Pod Bind 不受影响 | 11、13 | 调用计数 + API server |
| T19 抢占/旁路 | backfill/nomination 不绕过 hard；eviction/pipeline/preempt/reclaim/shuffle 不创建 assignment 或伪造外部释放 | 13、14 | action 回归 |
| T20 Pod-derived 恢复 | Session restart、两阶段 wave、Node replacement、assignment 缺失/冲突；跨 wave anchor 不漂移 | 11、14、16 | API server + E2E |
| T21 观测与回归 | controller blocker 不被 gang 覆盖；resolved 清除；无 policy 决定不变；指标低基数 | 04、07、17 | 集成 + benchmark |
| T22 安装与关闭 | 双进程 gate；失配时 fail closed；受保护 topology Pod/policy activity 时拒绝语义变更 | 02、14 | 安装集成；升级/drain/回滚后续验收 |

XPU-16 将 T01～T22 中的系统级场景串成回归，不重复替代各工作包的单元测试。

### 8.1 每个 PR 的完成定义

1. 只交付其声明能力；feature 默认关闭，未支持 hard 路径显式阻塞。
2. 对应测试族通过，检查正常行为与失败后的 Pod annotation、anchor、状态和 reason；不扩展为 external owner 或多 Pod 原子回滚验收。
3. 修改公共接口时补齐所有实现/Fake；修改 API 时检查 deepcopy/conversion、CRD/CEL、client 及发布模板。
4. 修改 cache/Statement/bind 时跑相关并发和现有 gang/network-topology/device 回归。
5. 同时更新能力表、配置和 reason 文档；记录测试环境、命令、结果及 Mock/真实范围。

### 8.2 后续开发的验证入口

以下是根据现有 Makefile/CI 核对的入口，本次计划编写未运行这些代码测试。开发时按 PR 改动范围选择，
不在每个小 PR 重复跑所有硬件/规模测试。

```bash
# API 修改后，在 staging API module 生成并验证。
cd staging/src/volcano.sh/apis
bash hack/update-codegen.sh
bash hack/verify-codegen.sh
```

```bash
# 仓库根目录：按 API/生成文件影响选择。
make generate-code
make manifests
make verify

# scheduler/cache/transaction 改动的聚焦测试。
go test ./pkg/scheduler/framework ./pkg/scheduler/cache ./pkg/scheduler/actions/allocate
go test -race ./pkg/scheduler/framework ./pkg/scheduler/cache ./pkg/scheduler/actions/allocate

# release/旁路改动的回归入口。
go test ./pkg/scheduler/actions/backfill ./pkg/scheduler/actions/preempt ./pkg/scheduler/actions/reclaim

# controller/admission 改动的入口。
go test ./pkg/controllers/podgroup ./pkg/controllers/job ./pkg/webhooks/admission/...
```

现有 CI 还执行 `make unit-test`、带发行 TAG 的 `make generate-yaml`/`make verify-generated-yaml`；
现有 kind 入口为 `make e2e`/`hack/run-e2e-kind.sh`。新 xPU suite 的选择参数、fixtures、硬件标签与执行脚本在 XPU-16 明确，
不把尚未创建的测试名写成现成命令。`make unit-test` 在 macOS 与 Linux 的 race 行为不同，并发验收应显式执行 race 或使用 Linux CI。

## 9. 排期、并行方式与第一周

### 9.1 参考排期

以 **2 名熟悉 Kubernetes/Volcano 的全职工程师 + 兼职 reviewer/测试支持** 为假设，每人每周 5 工程日：

- Advisory MVP：约 39～59 人日，考虑集成余量后约 **5～8 周**。
- Pod-derived Topology Alpha 主线（不含发布验收）：约 61～94 人日；两人并行的参考时间约 **8～12 周**。
- 单人串行时间需在 XPU-00/XPU-01 后重估；不能把两人排期直接当单人的可交付日期。

评审等待、硬件可用性、backend 协议补建不在以上区间；关键依赖未落定时，应更新排期，不缩减恢复或 release 验收来追赶。
这些区间用于资源讨论，不是对当前人力与设备条件的假定事实。

### 9.2 可并行与应串行的边界

| 工作流 | 适合的并行安排 | 串行门槛 |
| --- | --- | --- |
| API/controller | 03/04 与 Provider 纯逻辑并行，基于冻结的 shared types/fixtures | 固定 catalog 与 mutation authority 先确定 |
| scheduler 基础 | 02 → 05/06 → 07；公开接口冻结后提前写 planner fixture | paired snapshot 验收完成才能发布 Advisory |
| framework | 09 的 Statement/Session 审计与 11 的 Pod-derived anchor 可在 planner 后并行 | 09/11/13/14 完成才能连接 Alpha |
| Provider | 01 提前做 Provider identity probe，并与 Alpha 集成 | Provider identity probe 完成后才能冻结相关能力声明 |
| 测试/运维 | 每包写单元/故障测试；17/18 在 Alpha 后准备发布验收 | M4 等待能力矩阵、性能和升级演练 |

只有两名工程师时，上表是可选分工，不是五条同时满负荷推进的承诺。建议一人偏 API/Provider，另一人偏 cache/framework；
`cache.go`、`statement.go`、`allocate.go` 的每次接口变更明确一个主责，避免多个 PR 同时重写同一核心路径。

### 9.3 第一周的具体产出

优先开始 XPU-00 和 XPU-01，不先交一个只有空回调的 plugin PR。

| 时间 | 工作 | 可评审产物 |
| --- | --- | --- |
| 第 1 天 | 把各项 Alpha 决策分派到负责人；核对 allocation/Bind 调用链与全部分支 | 决策记录初稿、源码触点表、action 支持矩阵草案 |
| 第 2～3 天 | 冻结 Public/canonical 类型、最小 gate/plugin/catalog、assignment annotation 与 Pod-derived anchor 规则；准备正反例 fixtures | 同义 policy、6+2、两个 class、双 policy Fabric、四种量词、NodeUID/annotation fixture 规范 |
| 第 2～4 天 | 并行做 Provider 选定 DeviceKey、annotation 保留和 Node replacement 探针 | 可重放脚本、日志与能力差距清单 |
| 第 4～5 天 | 评审最小 Alpha 提交路径、旁路 guard 与 quiescent 规则；确认不增加新的提交事务层 | 决策修订、恢复/冲突序列表、首批 PR 的明确边界 |
| 第一周结束 | 按探针和接口反馈重估 XPU-02～07、11、13、14；为未决项指定 owner | 更新后的依赖、硬件条件与排期；XPU-16～18 明确为后续发布验收 |

## 10. 主要风险与调整规则

| 风险 | 尽早获得的证据 | 调整方式 |
| --- | --- | --- |
| backend 不能指定/确认 DeviceKey | XPU-01 的 Provider 探针 | Alpha 对相关 hard workload 保持 Pending；不把未验证能力纳入本计划 |
| framework review 发现 Statement/Bind 路径边界问题 | XPU-00 的最小 Alpha 合同 | 保持现有 Statement/逐 Pod Bind；在现有路径内修正 guard 和 annotation 传递 |
| admission 只读内存判断 policy activity 导致竞态 | T05 semantic mutation/绑定成员场景 | 复用现有 resourceVersion/generation 和 cache 重读；不以不存在的 anchor 放行语义变更 |
| assignment annotation 未能随 Pod Bind 保留 | XPU-11/13 的 API server 与 binder 测试 | 修正 BindContext/Pod annotation 路径；无法保留时相关 hard workload Pending |
| 数据 churn/大型 Group 使规划耗时失控 | 后续发布基线、T10/T15 压测 | 保持 paired snapshot、局部 revalidation 和实现阶段有限预算，超过预算 Pending |
| API 生成 prune/reject 规则失效 | T04 的真实 API server 测试 | 修正 schema 生成/后处理链；单测 parser 通过不能替代 |
| 真实 Fabric 环境不足 | XPU-01 环境清单 | 分别标注本地真实与 Fabric Mock 验证，不宣传未测硬件能力 |
| 关闭配置/旧版本回滚存在受保护 topology Pod | 后续发布 T22 演练 | 在发布阶段定义 Pod/policy 迁移顺序；Alpha 不引入额外 owner 清理流程 |

## 11. 本次文档交付的验证边界

本次只修改三份设计/合同/开发计划文档，未实现 V4 功能。后续提交应核对三份文档的阶段依赖、源码链接、任务编号、
估算加总、Markdown fence 和空白。

未运行 Volcano 单元/E2E 或硬件测试。依赖图按 Mermaid skill 做静态语法检查；本机未发现 `mmdc`，
未将 fence/节点检查称为图形渲染验证。
