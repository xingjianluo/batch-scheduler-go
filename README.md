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

## 6. 调度推演（先推演再决定）

推演用于容量规划与故障复盘：从**当前正在运行的调度器**或**一个已保存的状态点**出发，
按顺序喂一段事件（提交/完成/取消/推进时间），在独立副本上先跑一遍，看清最终哪些作业
在运行、哪些在排队、哪些被拒绝以及为什么、中间是否发生抢占、每一步的判断依据——
全程**不打扰原调度器**。

### 6.1 用法

```go
// 出发点二选一（SimConfig 的容量/老化参数需与出发点调度器一致）：
sim, _ := scheduler.NewSimulatorFromScheduler(live, scheduler.SimConfig{
    Capacity:     scheduler.Resource{CPU: 8, Memory: 16384, GPU: 2},
    AgeIncrement: 1, AgingPeriod: 10 * time.Second,
    WorkDir:      "./sim-run-1",            // 检查点目录；为空则纯内存（不可续跑）
    StartTime:    time.Now(),               // 逻辑时钟起点
})
// 或者从已保存的状态文件出发（该文件只读，绝不回写）：
sim, _ = scheduler.NewSimulator(scheduler.SimConfig{...}, tenants, "./data/state.json")

rec, err := sim.Apply(scheduler.SubmitEvent("job-9", "team-a", 5, scheduler.Resource{CPU: 2}))
sim.Apply(scheduler.AdvanceEvent(start.Add(time.Hour))) // 推进逻辑时间并触发一轮调度
sim.Apply(scheduler.CompleteEvent("job-1"))
sim.Apply(scheduler.CancelEvent("job-2"))
```

每个 `Apply` 返回 `StepRecord`：事件序号、执行时的逻辑时间、提交拒绝类别
（`RejectKind`，与真实提交完全同构）、操作错误、本步全部调度裁决（含抢占级联与
牺牲品列表）、以及本步生效后的完整快照。`sim.Records()` 可取回全部步骤。

### 6.2 推演与真跑的一致性

推演器内部**直接使用同一套 `Scheduler` 实现**，只替换两个外部依赖：时钟换成逻辑
时钟、存储换成推演自己的检查点。系统中不存在第二份调度逻辑，因此同一段事件序列，
推演结论与真实按同样顺序操作的结论**逐项一致**（拒绝类别、裁决链、每步后的作业
状态/队列/资源账）。这一性质由 `TestSimulationMatchesRealRun` 逐步比对保证。

### 6.3 逻辑时间与真实时间

- 所有与时间有关的判断（排队等待、老化提升）只按**调用方推进的逻辑时间**计算：
  `AdvanceEvent(t)` 把逻辑时钟拨到 `t`（不允许倒退），提交/完成/取消都发生在当前
  逻辑时刻。
- 真实过了多久**不影响**结论：同一段事件，瞬间跑完或隔几秒分几次跑，结果完全一致
  （`TestSimulationLogicalTimeOnly`）。
- 提交、结束、推进时间任意穿插时，资源账始终不超集群容量、不超租户配额，同一作业
  不会被排进去两次（每个测试步后都有不变量校验）。

### 6.4 中断续跑与失败语义

- 设置 `WorkDir` 后，每一步生效都会把「调度器状态 + 已生效步骤 + 逻辑时间」原子
  写入 `WorkDir/sim-checkpoint.json`（与调度器状态存储同款 tmp+fsync+rename）。
- 进程被杀后用 `ResumeSimulator(cfg)` 从最近一次落盘的检查点接着跑，最终结论与
  一口气跑完**完全一致**（`TestSimulationResumeNoDrift` 在每个可能的中断点验证）。
- 每一步**要么彻底生效，要么对外就当没发生过**：检查点落盘失败时 `Apply` 返回错误，
  内存状态回滚到上一步，重启后也看不到这步的痕迹；原样重试同一步不会多占资源、
  不会让队列或状态错位（`TestSimulationSaveFailureSemantics`）。
- 提交被拒、作业不存在等属于**正常推演结论**（记录在 `StepRecord` 中并正常生效），
  只有落盘失败才会让 `Apply` 返回错误。

### 6.5 与已有保存数据的兼容

- 调度器状态文件（`state.json`）的格式**没有任何变化**：既有保存数据可直接作为
  推演的出发点（`NewSimulator` 的 `seedStatePath`），原调度器的崩溃恢复行为不受影响。
- 推演对种子文件**只读**；推演的检查点是独立文件（`sim-checkpoint.json`），与调度器
  状态文件互不干扰；纯内存模式（`WorkDir: ""`）不落任何文件。

---

## 7. 验证方法

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
go test ./... -race -run 'TestSimulat'            # 推演：真跑对齐/逻辑时间/续跑/失败语义
```

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
| `TestSimulationMatchesRealRun` | 推演与真实运行逐步逐项一致（拒绝类别/裁决链/每步快照） |
| `TestSimulationLogicalTimeOnly` | 结论只依赖逻辑时间，与真实流逝时间无关 |
| `TestSimulationResumeNoDrift` | 在每个中断点杀掉续跑，逐步记录与基准完全一致 |
| `TestSimulationResumeMultipleKills` | 反复多次中断续跑结论不漂移 |
| `TestSimulationSaveFailureSemantics` | 落盘失败=步骤未发生（内存回滚/重启无痕迹/重试不多占资源） |
| `TestSimulationSaveFailureOnMutationStep` | 完成+级联调度步骤落盘失败可完整回滚 |
| `TestSimulationInitCommitFailure` / `TestSimulatorEdgeCases` | 初始提交失败、时间倒退、重复创建、关闭后使用 |
| `TestSimulationFromSavedStateFile` | 从既有保存状态出发=真实重启续跑，种子文件只读 |
| `TestSimulationFromLiveScheduler` | 从运行中调度器出发不打扰原调度器，同序结论一致 |

---

## 8. 包结构

```
pkg/scheduler/
  types.go       # 资源/作业/租户/状态/快照等核心类型
  errors.go      # 可区分的提交拒绝错误与哨兵错误
  resource.go    # 三维资源的加减与容量判定
  pool.go        # 进程内资源池（集群用量 + 各租户用量记账）
  store.go       # 持久化接口 + JSON 原子写文件存储 + 内存存储
  scheduler.go   # 准入、老化排序、抢占、调度固定点与崩溃恢复
  simulator.go   # 调度推演：事件重放、逻辑时钟、检查点与中断续跑
  *_test.go      # 上述全部场景测试
```
