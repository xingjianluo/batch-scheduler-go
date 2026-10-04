# 本地批处理作业调度系统（Go）

一个**单机、零外部依赖**的多租户批处理作业调度器：用进程内资源池模拟集群计算
资源，用本地 JSON 文件 + 原子重命名模拟集群持久化存储。目标是让多租户作业在
**并发提交、优先级抢占、进程重启**三类场景下的调度结论**可预期、可解释、可复现**。

- 模块路径：`github.com/xingjianluo/batch-scheduler-go`
- 核心包：`pkg/scheduler`
- Go 版本：1.25

---

## 1. 快速开始

```go
import "github.com/xingjianluo/batch-scheduler-go/pkg/scheduler"

s, err := scheduler.New(scheduler.Config{
    Capacity:     scheduler.Resource{CPU: 8, Memory: 16384, GPU: 2},
    AgeIncrement: 1,                    // 每等待一个周期等效优先级 +1
    AgingPeriod:  10 * time.Second,
    StorePath:    "./data/state.json",  // 为空则不持久化（纯内存模式）
}, []scheduler.Tenant{
    {ID: "team-a", Quota: scheduler.Resource{CPU: 4, Memory: 8192, GPU: 2}, MaxConcurrent: 0},
    {ID: "team-b", Quota: scheduler.Resource{CPU: 4}, MaxConcurrent: 4},
})

res, err := s.Submit("job-1", "team-a", 5 /*优先级*/, scheduler.Resource{CPU: 2, Memory: 2048})
// res.Started 为 true 表示立即启动，否则进入排队
s.Complete("job-1") // 作业完成，释放资源并自动触发一轮调度
s.Cancel("job-x")   // 取消排队中或运行中的作业
s.Close()
```

每次 `Submit / Complete / Cancel` 都会自动触发调度，并在状态变更后**原子持久化**。

> 说明：`Complete`/`Cancel` 只返回“完成/取消”动作本身的裁决；若需要该操作
> 引发的**全部连锁裁决**（释放资源后排队作业的启动、抢占、重新入队），
> 请使用 `CompleteAll(jobID)` / `CancelAll(jobID)`，返回完整 `[]Decision`
> （第一个元素即动作本身）。推演（见第 7 节）每一步记录的正是这份完整列表。

---

## 2. 调度模型

### 2.1 资源与配额

| 约束 | 含义 | 不满足时 |
| --- | --- | --- |
| `Config.Capacity` | 集群物理总容量（CPU/内存/GPU 三维独立比较） | 需求超出则提交被拒 `ExceedsClusterCapacity` |
| `Tenant.Quota` | 租户**瞬时持有资源上限**（不是总配额额度） | 单次需求超出则提交被拒 `ExceedsTenantQuota` |
| `Tenant.MaxConcurrent` | 租户同时运行作业数上限（0 表示不限） | 提交仍受理，但无槽位时排队 |

- 集群容量与租户配额是**两道独立约束**，启动作业时同时检查，任一不满足都不得启动，
  因此**绝不发生资源超卖**。
- 租户配额允许配置为大于集群容量（策略上限可以激进），两类“超出”因此可以清晰区分。
- 配额被其他运行中作业**暂时占满**不是拒绝理由：作业正常受理排队，待资源释放后再准入。

### 2.2 作业生命周期

```
            Submit
              │
              ▼
         ┌────────┐  资源/配额/槽位满足（可含抢占）   ┌─────────┐
         │ Queued │ ───────────────────────────────▶│ Running │
         └────────┘                                   └─────────┘
           ▲    ▲                                   Complete│  Cancel
           │    └──────── 被更高优先级作业抢占 ──────┐        │
           └───────────────────────────────────────┘        ▼
                                                    ┌───────────────┐
                                                    │ Completed /   │
                                                    │ Canceled(终态)│
                                                    └───────────────┘
```

终态作业保留历史记录但**永远不会被再次调度**，其 ID 也不可被新提交复用
（重复提交得到 `DuplicateJob`）。

### 2.3 调度顺序（完全确定）

每个作业在时刻 `now` 的**等效优先级**为：

```
排队中：eff = Priority + floor(now - QueuedAt, AgingPeriod) * AgeIncrement
运行中：eff = Priority            // 运行即获得进展，不参与老化
```

每轮调度将队列按以下全序键排序后依次考察：

1. **等效优先级降序**；
2. `SubmitSeq` 升序（全局单调提交序号，即 FIFO）。

采用**严格队首（Head-of-Line）规则**：一旦队首作业当前既无法直接启动、抢占后也
无法满足，立即结束本轮；排在其后的作业即使能塞进资源空洞也**不回填**。这牺牲了
少量利用率，换取“同样输入必然同样结果”的强可解释性。

### 2.4 抢占规则

当高等效优先级排队作业无法直接启动时，允许抢占**严格更低等效优先级**的运行中作业：

- 相同等效优先级之间**永不抢占**（避免抖动）；
- 牺牲品按下列全序键确定性排序后，逐个模拟回收，直到满足需求为止（**最小抢占集合**）：
  1. 等效优先级升序；
  2. **跨租户优先**（公平性规则）——同等条件下优先回收其他租户的占用，
     避免同一租户内部连环抢占；
  3. 资源需求降序（GPU → 内存 → CPU 字典序），优先牺牲“大”作业以减少牺牲品数量；
  4. 启动时间降序（后启动者先被牺牲）；
  5. 作业 ID 升序（最终平局兜底）；
- 若抢占全部候选仍不满足，则本轮**不抢占**（不做无谓破坏）；
- **一个调度轮次最多发生一次抢占**，随后立即结束本轮，防止“抢占者与被抢占者同轮
  互相反超”的抖动；下一轮事件（提交/完成/取消/`Tick`）再继续推进。

### 2.5 被抢占作业的重新入队语义（确定性 + 防饿死 + 防抖动）

- `SubmitSeq` 保持不变：按**原始提交顺序**插回队列（先被抢占者在同优先级下先恢复）；
- `QueuedAt` **重置为被抢占时刻**：该作业已经获得过运行进展，等待年龄重新累积，
  避免它立即反超抢占者造成来回抢占；
- `PreemptedTimes` 累加，可在快照/日志中观察。

### 2.6 防饿死（No Starvation）

排队作业的等效优先级随等待时间**线性且无界增长**，而运行中作业的等效优先级固定。
因此任何排队作业在有限等待后必然超过所有运行者并获得运行窗口。系统不会出现
“低优先级作业无限期得不到资源”的情况。`AgingPeriod/AgeIncrement` 用于配置老化速度。

---

## 3. 并发模型

- 调度器内部使用单一 `sync.Mutex` 保护全部可变状态（作业表、队列、资源记账、序号）。
- `Submit / Complete / Cancel / Get / Snapshot / Tick / Close` 均为线程安全。
- 资源采用“先判定后记账、全程持锁”的方式：并发提交下不会超卖，同一作业不会被
  重复调度；重复 ID 并发提交恰好一个成功，其余得到 `DuplicateJob`。

---

## 4. 状态恢复语义（崩溃恢复）

### 4.1 持久化机制

- 每次状态变更（受理/启动/抢占/完成/取消）完成后，将完整状态序列化为 JSON 写入
  `state.json.tmp`，`fsync` 后 `rename` 为 `state.json`。
- 重命名在本地文件系统上是原子操作，因此崩溃后磁盘状态要么是变更前、要么是变更后，
  不会读到半成品。
- 纯内存模式（`StorePath: ""`）不做任何持久化。

### 4.2 重启后的状态

重新 `New` 同一 `StorePath` 时自动恢复：

| 崩溃前状态 | 重启后 |
| --- | --- |
| `Running` 及其资源占位 | 保持 `Running`，按作业需求**逐笔重建资源记账** |
| `Queued`（含被抢占重排队） | 保持 `Queued`，队列顺序按 `SubmitSeq` 还原 |
| `Completed` / `Canceled` | 保持终态，**不会再次调度**，ID 不可复用 |
| `NextSeq` 提交序号 | 延续，避免序号回绕冲突 |

恢复后正常受理新操作；释放资源时排队作业按相同规则继续推进。

> 说明：本系统为本地模拟器，没有真实任务执行器，因此“运行中”不依赖外部心跳；
> 重启后运行占位被原样保留（模拟计算节点不丢作业的语义）。

### 4.3 拒绝原因（可区分）

| 错误 | 判定 | 错误类型 |
| --- | --- | --- |
| 参数非法 | ID/租户为空、资源为负 | `RejectError(InvalidRequest)` |
| 租户不存在 | 未注册租户 | `RejectError(TenantNotFound)` |
| 重复提交 | 作业 ID 已存在（含终态） | `RejectError(DuplicateJob)` |
| 超集群容量 | 需求任一维度 > 物理容量（永不可满足） | `RejectError(ExceedsClusterCapacity)` |
| 超租户配额 | 需求任一维度 > 租户静态配额（永不可满足） | `RejectError(ExceedsTenantQuota)` |

用 `scheduler.IsReject(err, scheduler.RejectDuplicateJob)` 编程化区分；
其他操作错误（作业不存在、状态不符等）为普通哨兵错误，如 `ErrJobNotFound`、
`ErrJobAlreadyTerminal`、`ErrJobNotRunning`。

---

## 5. 可观测性与可解释性

- `Scheduler.Snapshot()` 返回确定性顺序的完整快照：容量/已用、每个作业（按
  `SubmitSeq`）、队列 ID、运行 ID、各租户用量。
- `Submit / Complete / Cancel / Tick` 返回 `Decision`（或其列表），包含时间、
  作业、动作（`admit-queued/start/preempt/complete/cancel/reject`）、原因、
  资源用量快照与抢占牺牲品 ID 列表。
- 测试日志会打印**关键输入、资源快照与判定依据**（`go test -v` 可见）。

---

## 6. 验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单测（竞态检测，强烈建议）
go test ./... -race -v

# 仅看覆盖场景
go test ./... -race -run 'TestSubmitRejectReasons|TestQuotaBoundary|TestClusterCapacityBoundary|TestMaxConcurrent'  # 配额/准入边界
go test ./... -race -run 'TestConcurrent'    # 并发提交、无超卖、重复占位
go test ./... -race -run 'TestPriority|TestEqual|TestFIFO|TestAging|TestMinimalVictim|TestPreempted' # 抢占/回退/防饿死/确定性
go test ./... -race -run 'TestCrash|TestRecovery' # 崩溃恢复
go test ./... -race -run 'TestRehearsal'          # 推演：逐项对齐/中断续跑/存档失败/逻辑时间/兼容
go test ./... -race -run 'TestRehearsalCascade|TestRehearsalCompletePreemption' -v  # 推演：级联记录/续跑一致/失败重试（日志可见各场景）
```

> 说明：`TestRehearsalResumeNoDrift` 与 `TestRehearsalSaveFailureAtomic`
> 都是表驱动子测试，分别枚举全部中断点（0..N）与在每一步注入存档失败，
> 可用 `-run 'TestRehearsalResumeNoDrift/cutoff-7'` 之类的方式只跑单个断点。

### 推演测试场景清单

| 测试 | 覆盖点 |
| --- | --- |
| `TestRehearsalParityFromLive` | 从运行中的真实调度器 fork；20 步混合事件的错误/裁决/快照与真跑逐项一致；双向状态隔离 |
| `TestRehearsalParityFromStateFile` | 从已保存状态点推演与真跑逐项对齐；源状态文件字节级只读 |
| `TestRehearsalResumeNoDrift` | 在每个中断点（含 0 步）重启续跑，最终状态与全部历史报告不漂移 |
| `TestRehearsalCascadeDecisionsRecorded` | 完成/取消引发的级联启动、抢占被逐步完整记录，且与真跑每步结论（裁决+快照）逐项一致 |
| `TestRehearsalCompletePreemptionCascadeDetail` | “完成只释放部分资源→排队高优作业抢占低优作业顶上”逐步核对：谁被抢、谁顶上、按原序号回队、资源账目 |
| `TestRehearsalCascadeResumeIdentical` | 级联密集事件序列在全部中断点杀掉续跑，每步历史（当时快照/受理或拒绝/失败原因/全部裁决）与基准逐项一致 |
| `TestRehearsalCascadeSaveFailureRetryOnce` | 级联步骤存档失败：连锁对外与重启后均不可见；原样重试只生效一次（被抢次数恰好 1、资源不多占）；续跑后语义不变 |
| `TestRehearsalSaveFailureAtomic` | 每一步注入存档失败：对外/重启均无痕迹；原样重试幂等且结论一致 |
| `TestRehearsalSaveFailureRepeated` | 同一步连续失败多次后重试只生效一次 |
| `TestRehearsalLogicalTimeDeterminism` | 瞬间跑完 vs 带真实停顿分批跑完结论一致；无 Advance 时真实等待不引发老化抢占 |
| `TestRehearsalTimeCannotGoBack` | 逻辑时间不可回退，拒绝不消耗步骤、重启后仍拒绝 |
| `TestRehearsalConcurrentApply` | 并发推演提交串行生效、无超卖/无重复、存档完整 |
| `TestRehearsalCheckpointGuards` | 已存在存档拒绝覆盖；普通状态文件不可当作推演存档打开 |
| `TestRehearsalCompatibilityWithSchedulerStore` | 旧状态文件可作种子；推演存档可被旧调度器加载器读出，格式向后兼容 |
| `TestRehearsalRejectedStepsSurviveRestart` | 含业务错误（五类拒绝/状态错误）的步骤随存档保存，重启后逐项一致 |

### 测试场景清单

| 测试 | 覆盖点 |
| --- | --- |
| `TestSubmitRejectReasons` | 五类拒绝原因逐一可区分，被拒提交不产生占位 |
| `TestDuplicateSubmit` | 重复 ID（含并发 32 路、终态复用）不产生重复占位 |
| `TestQuotaBoundary` | 恰等于配额可准入；静态超限拒绝；暂时占满则排队；租户间额度独立 |
| `TestClusterCapacityBoundary` | 物理容量不足排队、永不可满足拒绝 |
| `TestMaxConcurrent` | 并发槽位约束 |
| `TestConcurrentSubmitNoOversell` | 120 路并发下集群/租户双维度不超卖、账实一致、作业唯一调度 |
| `TestPriorityPreemption` | 高优先级抢占、跨租户牺牲品选择、释放后回退作业恢复运行 |
| `TestEqualPriorityNoPreemption` | 同级不抢占，严格更高才抢占 |
| `TestFIFOWithinPriority` | 同优先级 FIFO、无插队回填 |
| `TestAgingNoStarvation` | 老化使长期排队作业最终抢占成功（有界等待） |
| `TestDeterminism` | 相同初始状态 + 相同提交序列两次运行结果完全一致 |
| `TestMinimalVictimSetMultiDim` | 多维资源下只抢占最小必要集合 |
| `TestPreemptedJobRequeuePosition` | 被抢占者按原 SubmitSeq 回队并优先于后到作业恢复 |
| `TestCrashRecovery` | 运行/排队/终态/占位崩溃前后一致，终态不再调度 |
| `TestRecoverySubmitSeq` | 提交序号跨重启延续 |
| `TestRecoveryUnderConcurrentLoad` | 并发负载后崩溃，磁盘状态自洽、无超卖无重复 |

---

## 7. 推演（Rehearsal）：先推演，再决定

在容量规划与故障复盘时，光看“现在是什么状态”不够，还需要回答**“如果按某个
顺序发生这些事，最后会怎样、每一步为什么这样判”**。推演能力让你从**当前正在
跑的状态**或**一个已保存的状态点**出发，喂一串事件先跑一遍：

- 完全在**独立副本**上演算，**绝不触碰、也不影响**原来那台真实调度器；
- 复用同一套调度代码（准入、老化、抢占、固定点），推演结论与真按相同顺序
  操作一遍的结果**逐项一致**——不会“推演一套、真跑另一套”；
- 等待/老化等一切与时间有关的判断都按调用方推进的**逻辑时间**计算，
  **不读墙钟**：同一段事件瞬间跑完与隔几秒分几次跑，结论完全相同；
- 推演进度本身**原子持久化**，进程被干掉后可从最近一步**续跑**，最终结论
  与一口气跑完完全一致；每一步**要么彻底生效，要么对外等同于没发生**。

### 7.1 事件类型

| 构造函数 | 含义 |
| --- | --- |
| `NewSubmitEvent(jobID, tenantID, priority, req)` | 新作业提交（含五类拒绝路径） |
| `NewCompleteEvent(jobID)` | 作业结束，释放资源并触发调度 |
| `NewCancelEvent(jobID)` | 取消排队/运行中的作业 |
| `NewAdvanceEvent(at)` | 把逻辑时间推进到 `at`（不可向过去推进） |

> 只有 `Advance` 会改变逻辑时间；`Submit/Complete/Cancel` 使用当前逻辑时间，
> 不会因为真实世界过了多久而改变结论。需要“等一会儿再提交”时，请显式先
> `Advance` 再 `Submit`——这与真实调度器配合同步的逻辑时钟完全对应。

### 7.2 基本用法

```go
import "github.com/xingjianluo/batch-scheduler-go/pkg/scheduler"

// 方式一：从正在运行的真实调度器“分叉”出一轮推演（只读取一次快照副本）
rh, err := scheduler.NewRehearsalFromLive(liveScheduler, "./data/plan-a.json")

// 方式二：从一个已保存的状态点出发（源状态文件全程只读，绝不被改写）
rh, err := scheduler.NewRehearsalFromStateFile(
    "./data/state.json",            // 只读的种子状态
    "./data/plan-a.json",           // 推演自己的进度存档
    cfg,                            // 必须与产生种子状态时一致（容量/老化参数）
    seedTime,                       // 推演起始逻辑时间
)

events := []scheduler.RehearsalEvent{
    scheduler.NewAdvanceEvent(seedTime.Add(30 * time.Second)),
    scheduler.NewSubmitEvent("forecast-1", "team-a", 9, scheduler.Resource{CPU: 4}),
    scheduler.NewCompleteEvent("job-1"),
    scheduler.NewCancelEvent("job-2"),
}
reports, err := rh.ApplyAll(events)
// reports[i].Err        本步业务结果：nil 成功；*RejectError 被拒；
//                       ErrJobNotFound/ErrJobNotRunning/ErrJobAlreadyTerminal 等
// reports[i].Decisions  本步引发的全部调度裁决（admit/start/preempt/complete/
//                       cancel）：完成或取消一个作业时，由它连锁触发的启动、
//                       抢占（含牺牲品 ID）、重新入队都完整记录在内，
//                       每条裁决带资源用量快照与判定原因
// reports[i].Submit     提交事件的受理结果（被拒时为 nil，错误在 Err 中）
// reports[i].Snapshot   本步生效后的完整状态快照
_ = reports
final := rh.Snapshot()   // 最终哪些在运行、哪些排队、哪些被拒
rh.Close()
```

- `Apply(ev)` 一次只应用一个事件（便于在任意点暂停观察）；`ApplyAll` 是其顺序封装。
- **每一步记录的内容**：事件本身、执行时的逻辑时间、该步引发的**全部**调度
  裁决（对 `Complete`/`Cancel` 事件，`Decisions[0]` 是结束/取消动作本身，
  其后依次是释放资源连锁触发的启动/抢占裁决，可清楚看出有没有发生抢占、
  谁被抢了、谁顶上来）、提交受理结果、业务错误，以及该步生效后的状态快照。
  这些内容与同样顺序在真实调度器上跑一遍（`Submit`/`CompleteAll`/
  `CancelAll`/`Tick`）得到的结论**逐项一致**。
- **业务结果**（提交被拒绝、作业不存在、状态不符）属于事件的正常演算结果，
  记录在 `StepReport.Err` 里并继续后续事件；`Apply` 只为这类结果返回 `nil`。
  只有**基础设施错误**（存档 IO 失败）才以非 nil error 返回。

### 7.3 逻辑时间与真实时间的关系

- 推演内部是一台只接受 `Advance` 驱动的手动时钟（`manualClock`），
  **任何地方都不读取 `time.Now()`**；老化公式
  `Priority + floor(now-QueuedAt)/AgingPeriod*AgeIncrement` 中的 `now`
  永远是当前逻辑时间。
- 因此：同一段事件，无论调用方是在一个 for 循环里瞬间喂完，还是每步之间
  `time.Sleep` 几秒、甚至跨天分批喂，只要事件与其 `Advance` 时刻相同，
  推演结论（运行/排队/拒绝及原因、抢占、每步裁决）**逐项相同**。
- 逻辑时间**单调不回退**：`Advance` 到早于当前的时刻会得到
  `ErrRehearsalTimeGoesBack`，该调用既不演算也不存档，状态原地不动。
- 要让“推演与真跑逐项一致”，真实那侧也应使用由调用方同步推进的逻辑时钟
  （如测试中的假时钟；生产中即你的仿真/事件时间），在每个 `Advance(t)`
  处把真跑时钟也设到 `t`，其余一一对应执行 `Submit/Complete/Cancel`。

### 7.4 中断续跑语义

- 构造时与**每一步成功生效时**都原子写入进度存档（临时文件 + fsync + rename），
  因此崩溃后磁盘上只可能是“上一步”或“本步”，绝不会是写了一半的存档。
- 进程重启后调用 `OpenRehearsal(path)`：恢复作业表、队列顺序、资源账目、
  提交序号、容量/老化参数与逻辑时间，并重建历史步骤报告；随后继续 `Apply`
  剩余事件即可。`Applied()` 给出已生效步数，便于跳过已完成的事件前缀
  （也可以把完整事件列表原样再喂一遍——已存档的前缀在磁盘上不会重复应用）。
- **历史报告逐项一致**：每一步的完整记录都随存档持久化——包括该步当时的
  状态快照、全部连锁调度裁决、提交是被受理还是被拒绝（受理方含作业副本与
  Started 标志）、失败原因（拒绝类别/哨兵错误按类型还原）。因此在**每一个**
  中断点（含“刚创建、一步未跑”）杀掉续跑，恢复出的每一步历史报告与最终
  状态都与不中断连续跑完**逐项一致**，而不仅仅是最终作业状态一致
  （`TestRehearsalResumeNoDrift` 与 `TestRehearsalCascadeResumeIdentical`
  分别枚举了通用场景与级联密集场景的全部中断点）。
- 存档格式版本为 `rehearsalVersion=2`（v2 起每步记录完整快照与受理详情）；
  旧版本存档会得到 `ErrRehearsalUnsupportedVersion`，请重新发起推演。

### 7.5 存档失败时的对外一致性

每一步采用“**在候选副本上演算 → 原子存档 → 成功后才转正**”的顺序：

- 演算成功但存档失败：候选副本被丢弃，推演保留旧状态，
  调用方看到失败；重启后磁盘上也看不到这一步——**该步本身以及它引发的
  全部连锁动作（启动/抢占/重新入队）都对外不可见，重启后也不会冒出来**；
- 随后原样重试同一事件：只生效一次，**不会多占资源、不会让队列或资源
  账目错位**（例如被抢占次数恰好增加一次）；
- 同一步连续失败任意次后再成功，最终结论仍与无失败时逐项一致；
  杀掉重开后续跑，同样的失败/重试语义保持不变
  （`TestRehearsalSaveFailureAtomic` 在每一步上注入失败验证，
  `TestRehearsalCascadeSaveFailureRetryOnce` 专门验证级联步骤的失败与重试）。

### 7.6 与已有持久化数据的兼容

推演存档（`rehearsalVersion/rehearsalKind` 信封 + 状态字段）与调度器原有
状态文件**同构、互相兼容**：

- 调度器写出的 `state.json` 可**直接作为推演种子**（`NewRehearsalFromStateFile`
  只读打开，字节级不改动它）；
- 推演存档中的 `NextSeq/Tenants/Jobs/QueuedOrder` 使用与普通状态文件**完全
  相同的 JSON 键**，旧版 `Scheduler` 加载器会忽略新增的信封/进度字段，
  因而同一份推演存档也能被普通调度器当作状态点读出并继续运行；
- 新建推演时若目标路径已有文件（推演存档或普通状态文件）会得到
  `ErrRehearsalCheckpointExists`，防止覆盖另一轮推演；续跑请用
  `OpenRehearsal`；用 `OpenRehearsal` 打开普通状态文件会得到
  `ErrRehearsalNotACheckpoint`。

---

## 8. 包结构

```
pkg/scheduler/
  types.go       # 资源/作业/租户/状态/快照等核心类型
  errors.go      # 可区分的提交拒绝错误与哨兵错误
  resource.go    # 三维资源的加减与容量判定
  pool.go        # 进程内资源池（集群用量 + 各租户用量记账）
  store.go       # 持久化接口 + JSON 原子写文件存储
  scheduler.go   # 准入、老化排序、抢占、调度固定点与崩溃恢复
  rehearsal.go   # 推演器：隔离副本、逻辑时钟、事件演算、逐步原子提交、续跑
  rehearsal_store.go # 推演进度存档的原子读写与格式信封
  *_test.go      # 上述全部场景测试
```
