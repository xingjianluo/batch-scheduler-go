# 本地批处理作业调度系统（batch-scheduler-go）

用**进程内资源池**模拟集群计算资源、用**本地 WAL 文件持久化**模拟分布式存储，
在单进程内实现一套多租户批处理作业调度系统。目标是让多租户作业在
**并发提交、优先级抢占、进程重启**三类场景下的调度结论**可预期、可解释、可重放**。

- 纯 Go 标准库实现，无外部依赖。
- 所有状态变更串行化（单互斥锁），结构上杜绝资源超卖与重复调度。
- 所有状态变迁先写 append-only WAL（JSONL + fsync）再改内存，崩溃后重放恢复。
- 调度规则完全确定：相同初始状态 + 相同提交序列 + 相同 tick 序列 ⇒ 完全一致的结果。

## 1. 快速开始

```go
import "github.com/xingjianluo/batch-scheduler-go/scheduler"

s, err := scheduler.New(scheduler.Config{
    Capacity: scheduler.Resources{CPUMillis: 2000, MemoryMB: 2048}, // 集群总量
    Tenants: []scheduler.Tenant{
        {Name: "alpha", Quota: scheduler.Resources{CPUMillis: 1000, MemoryMB: 1024}},
        {Name: "beta",  Quota: scheduler.Resources{CPUMillis: 2000, MemoryMB: 2048}},
    },
    DataDir: "./data", // 非空则启用文件持久化；为空则纯内存（测试用）
})
if err != nil { panic(err) }

job, created, err := s.Submit(scheduler.JobSpec{
    ID: "job-001", Tenant: "alpha", Priority: 3,
    Request:       scheduler.Resources{CPUMillis: 500, MemoryMB: 512},
    DurationTicks: 4,
})
result := s.Tick() // 推进一个时间单位：完成到期作业 → 一轮确定性调度（含抢占）
snap := s.Snapshot()
```

## 2. 调度模型

### 2.1 资源与时间

- 资源为两类标量：`CPUMillis`（CPU 毫核，1000 = 1 vCPU）、`MemoryMB`。
  作业运行期间**全程独占**其请求量；任一维度放不下即不可调度。
- 时间是离散的 tick。作业在 tick `N` 被派发、剩余 `R` 时，占用时间片
  `N, N+1, …, N+R-1`（共 `R` 个），在 tick `N+R` 开始时被判定完成。
- `Tick()` 内顺序固定（保证可重放）：
  1. 持久化并进入新 tick；
  2. 运行中作业到达完成点的，标记完成、释放资源（按作业 ID 排序处理）；
  3. 调度轮：对本 tick 可参与的排队作业逐个做准入/抢占决策。

### 2.2 作业状态机

```
            Submit(受理)
   (不存在) ───────────▶ PENDING ──dispatch──▶ RUNNING
                            ▲                    │
                            │                    ├── finish ──▶ COMPLETED (终态)
                            │                    ├── cancel ──▶ CANCELED  (终态)
                            └──── preempt ───────┘
```

- `PENDING`：排队中。被抢占的作业下一 tick 才能重新参与调度
  （`EnqueueTick = 抢占tick + 1`），避免同一 tick 内抢占抖动。
- `RUNNING`：已分配资源。被抢占时按已运行的 tick 数扣减 `Remaining`，
  恢复调度后从剩余时长继续，而不是重跑整段。
- `COMPLETED / CANCELED`：终态。终态作业**永远不会被再次调度**，
  重启后亦保持终态。

### 2.3 优先级与确定性排序

排队作业每个 tick 计算**有效优先级**：

```
effPriority(等待作业) = basePriority + floor(已等待tick数 / 10) * 1
effPriority(运行作业) = 派发那一刻冻结的值（StartEffPrio）
```

调度轮中候选作业按以下键**全序**排序（无随机、无 map 迭代）：

1. 有效优先级降序；
2. 基础优先级降序；
3. 入队 tick 升序（先到先得）；
4. 作业 ID 字典序。

### 2.4 防饿死（aging）

排队作业的有效优先级随等待时间无界增长，而运行作业的有效优先级在派发时
冻结。因此任何排队作业在有界时间内必然超过所有运行者并获得资源
（前提：系统作业负载有限、资源请求不超过总量/配额——这两类在提交期已拒绝）。
老化只依赖 tick 计数，不引入任何非确定性。

## 3. 配额与准入规则

提交 `Submit` 按以下顺序判定，拒绝原因**可区分**：

| 顺序 | 判定 | 拒绝原因 (`SubmitError.Reason`) |
|---|---|---|
| 1 | ID/租户名为空、优先级 < 0、资源 ≤ 0 或为负、时长 ≤ 0 | `INVALID_REQUEST` |
| 2 | 同一 ID 重复提交 | **不是错误**：幂等返回已有作业（`created=false`），不重复占位 |
| 3 | 租户未注册 | `UNKNOWN_TENANT` |
| 4 | 需求超过集群总容量（任何配额空闲都放不下） | `CAPACITY_FULL` |
| 5 | 需求超过该租户配额上限 | `QUOTA_EXCEEDED` |
| 6 | 以上均通过 | 受理：先写 WAL 再入队（`PENDING`） |

注意第 4/5 条是**静态可行性**判定（"永远无法满足"才拒绝）；
若只是"当前资源被占满但将来可能空闲"，作业正常受理并排队。

运行期有两层资源约束，任一维度都不得突破：

- 集群：所有租户占用之和 ≤ `Capacity`；
- 租户：该租户运行作业占用之和 ≤ 其 `Quota`。

## 4. 抢占规则

当候选作业无法直接放入时，在**运行中**作业里寻找抢占候选：

1. 候选条件：`victim.StartEffPrio < waiter.effPriority`
   （只抢占冻结有效优先级**严格更低**者；含因老化而反超的情形）。
2. 牺牲者排序（确定、可解释）：冻结有效优先级升序 → 开始 tick 升序 → 作业 ID。
3. 贪心累加牺牲者，同时补足两个维度的缺口：
   - 同租户牺牲者同时释放**集群容量**与**租户配额**；
   - 跨租户牺牲者只释放**集群容量**；
   - 牺牲者对现存缺口无任何贡献时跳过（不无意义抢占）。
4. **全有或全无**：即使抢占所有合格候选仍放不下，则本轮一个都不抢
   （避免抢了却用不上造成抖动），作业继续等待。
5. 抢占事件先持久化：牺牲者按 `ran = 当前tick - StartTick` 扣减已运行时长、
   释放资源、回到 `PENDING`（下一 tick 重新入队）；随后等待者在同一 tick 派发。

## 5. 持久化与状态恢复语义

### 5.1 WAL

- 文件：`<DataDir>/wal.jsonl`，每行一个 JSON 事件，append-only，每次写入后
  `fsync`。`Submit/Cancel/Tick/调度/抢占/完成` 的每一步状态变迁都是一条事件。
- 事件类型：`TENANT_REGISTERED, SUBMITTED, TICK, DISPATCHED, PREEMPTED,
  COMPLETED, CANCELED`，单调递增 `Seq`，并带发生时的 `Tick`。
- 事件是状态的**唯一真相来源**；`DataDir` 为空时使用 `MemoryStore`（不落盘）。

### 5.2 恢复（重放）

`New(Config{DataDir: ...})` 打开 WAL 并按 Seq 顺序重放：

- 重放是幂等的：重复的 `SUBMITTED` 不产生重复占位；终态事件不重复释放资源。
- tick 计数、作业状态、租户配额占用、集群占用全部由事件重建。
- **崩溃于运行片段中途**的作业：最后一条相关事件是 `DISPATCHED`，
  重放结束后统一折算（`reconcileAfterReplay`）——
  已消耗 `tick - StartTick + 1` 个时间片（崩溃所在 tick 的时间片视为已跑完），
  剩余时长与预计完成点相应平移，保证重启后不重复运行、也不少跑。

恢复语义保证：**已受理作业的排队、占位与终态与崩溃前保持一致**；
已完成/已取消作业不会被再次调度；恢复后继续推进的调度结论与"从未崩溃"的
连续执行在相同提交与 tick 序列下逐字段一致（见 `TestCrashRecoveryContinues`）。

### 5.3 并发

`Submit / Cancel / Tick / Snapshot / Job` 共用一把互斥锁，状态变更完全串行。
因此：并发提交不会超卖、同一作业不会被重复调度、重复 ID 恰好受理一次；
所有不变量（占用之和=运行作业请求之和、占用 ≤ 配额/容量）在任何时刻成立。

## 6. 可解释性

- 每次提交的关键输入、每个 tick 的资源快照（集群已用/总量、租户已用/配额）、
  每次调度/抢占/等待/拒绝的**判定依据**（有效优先级、容量空闲、配额空闲、
  抢占受害者及原因）都通过 `log.Logger` 打印；测试中这些日志汇入
  `go test -v` 输出。
- `Snapshot()` 返回排序后的全量状态（作业明细、各租户 running/pending 列表、
  两层资源占用），便于断言与排查。

## 7. 验证方法

```bash
go build ./...
go vet ./...
go test ./... -race -count=1 -v
```

测试覆盖（`scheduler/*_test.go`）：

| 场景 | 测试 |
|---|---|
| 非法请求 / 未知租户 / 超容量 / 超配额，原因可区分 | `TestSubmitValidation` `TestSubmitUnknownTenant` `TestSubmitCapacityFull` `TestSubmitQuotaExceeded` `TestRejectReasonsDistinct` |
| 配额边界（恰好填满、再超则排队、不超卖） | `TestQuotaBoundaryRunning` |
| 重复提交幂等、不重复占位 | `TestSubmitIdempotent` `TestConcurrentDuplicateSubmit` |
| 优先级顺序、抢占回退、剩余时长续跑、终态不再调度 | `TestPriorityOrder` `TestPreemptAndResume` `TestNoPreemptIfInfeasible` |
| 老化防饿死、调度确定性（两次独立运行逐字段一致） | `TestAgingNoStarvation` `TestDeterminism` |
| 并发提交 + 并发 tick，过程中无超卖/无重复调度 | `TestConcurrentSubmitNoOversell`（-race） |
| 取消（运行中立即释放/排队取消/终态与不存在原因区分/重启保持） | `TestCancelSemantics` |
| 崩溃恢复：排队/占位/终态一致、中途崩溃时长折算、终态不再调度、WAL 落盘 | `TestCrashRecoveryPendingAndRunning` `TestCrashRecoveryContinues` `TestTerminalJobsNotRescheduled` `TestWALFileDurable` |

## 8. 目录结构

```
scheduler/
  types.go       # Resources / Job / 状态 / 有效优先级与老化
  errors.go      # 提交与取消的可区分错误原因
  store.go       # Store 接口、FileStore(JSONL+fsync WAL)、MemoryStore
  scheduler.go   # 准入、tick 推进、确定性调度、抢占、取消、WAL 重放恢复
  *_test.go      # 配额边界/并发/抢占回退/崩溃恢复等测试
```
