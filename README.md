# data-qd-225 · 数据库迁移事务执行器

服务部署时提交完整、有序的 SQLite 迁移计划。执行器对照已登记的版本和校验值，拒绝历史漂移；将本次待执行的 schema/data SQL 与版本登记在同一事务提交。批次失败回退新增变更，原版本仍可用。只接受受信任的项目 SQL，每个元素为一条语句，应避开执行器保留的 `_migration_ledger`；不提供回滚脚本或多数据库协调。

Python 3.9+，仅标准库。示例使用本仓合成迁移和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

错误批次输出 `after rollback: ['id']`，改用有效计划则显示 `upgrade: [2]` 和 `retry: []`。

## 查询等价闸门与预演

`CheckedMigration(version, statements, checks)` 是可放入完整连续计划的普通原子
阶段。它先在**当前事务状态**中按声明顺序执行 1–8 个 `QueryParity` 的前查询，
再执行有序迁移 SQL，然后按同一顺序执行后查询并比较整行多重集；第一个失败检查
会中止批次。迁移 SQL、等价判定和账本登记仍在同一个 `BEGIN IMMEDIATE` 事务中，
任何错误都回滚本批全部改动。

```python
from migrations import (
    CheckedMigration, QueryParity, apply_plan, preview_plan,
    QueryParityShape, QueryParityMismatch,
)

name_parity = QueryParity(
    "event-names",          # 1–32 个 [A-Za-z0-9_-]，阶段内唯一
    1,                      # 非布尔 int，1–8
    "SELECT name FROM old_events",
    "SELECT name FROM new_events",
)
checked = CheckedMigration(2, (
    "CREATE TABLE new_events(name TEXT NOT NULL)",
    "INSERT INTO new_events SELECT name FROM old_events WHERE eligible = 1",
), [name_parity])

preview_plan(path, plan)    # 返回若提交会登记的版本，但整体回滚
apply_plan(path, plan)      # 每次都在新写事务中重新执行查询和判定
```

比较忽略列名和行顺序，但不忽略每行的出现次数或 SQLite 存储类型：`1` 与 `1.0`、
`'x'` 与 `x'78'`、不同大小写文本均不等价；NULL 只等于 NULL，`-0.0` 与 `0.0`
相等。列数错误抛 `QueryParityShape(version, check_name, side,
expected_columns, actual_columns)`；多重集不等抛
`QueryParityMismatch(version, check_name, row, before_count, after_count)`。
checksum 绑定阶段类型、版本、有序 SQL，以及检查顺序、名称、列数和两段原始 SQL；
已登记阶段重放为 no-op，改动任一项会按原规则抛 `HistoryMismatch`。

大结果集每侧只执行一次，Python 侧只流式取小批量行；精确计数、类型键和连接索引
都放在 SQLite TEMP 表中，关闭连接即释放，不把查询结果写入应用库。副本波次仍只
主动安排剩余阶段全部是精确 `Migration` 类型的节点；已登记的 `CheckedMigration`
前缀可正常通过账本校验。

## 在线唯一性发布

计划中可包含一个 `OnlineUnique(version, table)`，用于给已有表的
`route_key TEXT COLLATE BINARY` 列（`id INTEGER PRIMARY KEY`）在**不停写**、
**不重扫全表**的前提下发布唯一性。它不能由 `apply_plan` 直接执行，而是走三步：

```python
from migrations import (Migration, OnlineUnique, apply_plan,
                        prepare_online_unique, step_online_unique,
                        publish_online_unique)

plan = [Migration(1, (...)), OnlineUnique(2, "routes")]

prepare_online_unique(path, plan)          # 装触发器/进度表，不登记版本
step_online_unique(path, plan, 256)        # 每次至多 limit(1..256) 个 id，返回处理数
# … 反复 step 直到返回 0（发布前的新写入也会被计入） …
publish_online_unique(path, plan)          # ("published", None) 或冲突/未就绪
```

- `prepare_online_unique` 首次返回 `True`，同计划重做返回 `False` 且不清空进度。
  它在目标表上安装增量捕获触发器（普通连接无需任何改动），进度记录在
  `_online_unique_state` 中，尚不改变写入规则。
- `step_online_unique` 按 id 升序回填 capture 表，每步一个短事务，配额为
  `limit`。捕获触发器会同步合并期间的旧节点已提交 INSERT/UPDATE/DELETE，
  包括回填游标之前新插入的更小 id；回滚的写入不会留下痕迹。
- `publish_online_unique` 在同一事务中：校验无待处理工作、按 BINARY 顺序
  取最小重键见证、写入账本 checksum、开启写入期唯一性触发器。返回
  `("not_ready", None)` / `("conflict", (key, id1, id2))` /
  `("published", None)` / `("already_published", None)`。阻塞结果不会登记版本。

发布成功后，既有旧连接再插入或改键到已被占用的非 NULL 键会抛
`sqlite3.IntegrityError` 且不留变更；改成自身原键、删除后复用键、多行 NULL
正常。`apply_plan` 仍按原口径检查前缀：发布前不能越过待发布在线版本，也不能
在 prepare 与 publish 之间用别的普通迁移推进账本；发布后完整计划可重放，并
可继续原子应用之后不触碰该唯一性的普通迁移。所有入口只依赖数据库内可索引的
工作量，单行旧写入的额外成本与表大小、重复组大小无关。

## 维护窗口内回收发布制品

计划中可以加入普通阶段
`ReleaseArtifactReclaim(version, candidate_ids, max_releases)`。候选 ID 为
1–20 个不同的正 SQLite 整数（拒绝布尔值），`max_releases` 为 1–8；版本仍须
满足计划从 1 连续编号的规则。候选顺序不参与 checksum，checksum 绑定类型、
版本、排序后的候选集合和额度。

```python
from migrations import Migration, ReleaseArtifactReclaim, apply_plan

plan = [
    Migration(1, ("CREATE TABLE releases(...)", ...)),
    ReleaseArtifactReclaim(2, [5, 2, 4, 3], max_releases=2),
]
apply_plan(path, plan)
```

执行时只选择候选中实际存在且 `retired=1` 的发布；活跃或不存在的 ID 不占用
删除额度。一个制品只有在本阶段开始时至少有一个持有者，并且全部持有者都属于
最终选中的发布集合时才删除。未列入候选、仍活跃或未选中的持有者都会阻止释放；
历史无主制品不计收益也不删除。

选择算法把每个可由候选解释的制品持有者集合编码成位掩码，在有界临时表中完成
子集 zeta 汇总，然后精确比较所有不超过额度的集合：先最大化登记字节数，收益
相同则减少发布数，仍相同则选择数值 ID 升序序列中字典序最小者。目录扫描量只
与目录/关联行数相关，不随候选组合数倍增；Python 侧只流式读取评分行，最大
候选规模下新增 Python 峰值远低于 32 MiB，主要临时空间由 SQLite 管理。

该阶段与普通 SQL、账本写入处于同一个 `apply_plan` 事务；后续阶段失败会连同
回收删除和账本记录一起回滚。准确计划重放为 no-op；改变候选集合或额度会因
checksum 不同抛出 `HistoryMismatch`。它也可以位于已发布的 `OnlineUnique`
之后；未发布的在线版本仍按原规则阻止账本推进。

## worker 执行配置的发布与撤回

计划中可以放入两个相邻的原子阶段：

```python
from migrations import (
    Migration, RestoreWorkerProfiles, WorkerProfileRollout,
    ProfileRestoreBlocked, apply_plan,
)

rollout = WorkerProfileRollout(2, (
    "UPDATE worker_profiles SET config=x'11' WHERE id=10",
    "DELETE FROM worker_profiles WHERE id=20",
    "INSERT INTO worker_profiles(id, config) VALUES (40, x'dd')",
))
restore = RestoreWorkerProfiles(3, 2)      # 必须是紧邻的上一个版本
plan = [Migration(1, (...)), rollout, restore]

apply_plan(path, plan[:2])                 # 上线，登记 version 2
apply_plan(path, plan)                     # 撤回，登记 version 3
```

`WorkerProfileRollout` 携带 1–64 条有序、只写 `worker_profiles` 的普通
`INSERT`/`UPDATE`/`DELETE`（单条、无 REPLACE/冲突策略/事务控制/DDL）。执行时
按“阶段前 vs 最后一条语句后”比较，每个净变化 id 只写一行到只读证据表
`_worker_profile_rollout_delta(rollout_version, profile_id, before_config,
after_config)`；`NULL` 表示该时点无此行，`x''` 是合法空配置，与不存在不同。
改回原值或出现后又消失的 id 不留记录，空净变化的上线仍登记版本。

`RestoreWorkerProfiles(version, version - 1)` 只处理该上线的净变化 id：现场仍
等于上线后状态时才还原到上线前状态，否则原样保留；未被上线改变的 profile 绝不
触碰。若还原需要删除的 profile 仍被 `job_profile_pins` 引用，整次撤回抛
`ProfileRestoreBlocked(profile_id, job_id)`（取数值字典序最小的 `(profile,
job)` 见证），不删 pin、不改数据、不登记版本；删除 pin 后同一计划可重试。
撤回是一个新版本，不回滚 schema，也不删除证据。上线与其撤回不能在同一份未提交
批次里：撤回引用的上线必须在此次调用开始时已登记，否则抛 `ValueError`。

结构或参数错误抛 `ValueError`；滚动/语句顺序或撤回参数漂移抛
`HistoryMismatch`。两阶段都有稳定 `checksum()`，绑定类型、版本及有序语句或被
撤回版本。大目录下的比较与写入全部在 SQLite 内以集合方式完成，Python 侧不
持有目录，单次调用新增峰值分配（`tracemalloc`）远低于 20 MiB。

## 路由缓存的可恢复变更投递（route feed）

针对部署表 `route_catalog(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)`，
执行器在迁移库内提供单库、单消费方的可恢复交付：先装载一个不可变的分页
快照，再按真实提交顺序追更完整的净变化批次，不另起服务、不持有跨调用连接。

```python
from migrations import (
    Migration, RouteFeedFull, RouteFeedState,
    open_route_feed, read_route_snapshot, finish_route_snapshot,
    read_route_changes, ack_route_changes, apply_plan, preview_plan,
)

base = open_route_feed(path, max_pending=256)   # 返回原子确定的切点版本
after = 0
while True:
    rows, after = read_route_snapshot(path, after, limit=256)
    cache_update(rows)                          # 同一切点，可跨进程重读
    if after is None:
        break
finish_route_snapshot(path, base)               # 快照可靠落地后确认（首次 True）

while True:
    batch = read_route_changes(path)            # 最早未确认批次，None 表示积压为空
    if batch is None:
        break
    version, entries = batch                    # entries 按 id 升序，删除项 payload 为 None
    cache_apply(entries)
    ack_route_changes(path, version)            # 只能确认最早未确认批次
```

- `open_route_feed(path, max_pending)` 在一个写事务内同时读取已登记的最高版本
  并全量扫描目录写入私有快照表，两者来自同一数据库状态；此后任何分页都不受
  现表增删改影响。同额度重调返回原切点且不清进度，换额度抛 `RouteFeedState`。
  `max_pending`（1–16384，拒绝布尔值）限制**未确认变更条目总数**。
- `read_route_snapshot(path, after_id, limit)` 只在快照确认前可用，按 id 升序
  返回严格大于游标的前 `limit`（1–256）行；空页的 `next_after` 为 `None`，
  同一游标可重复读取，读页不改变任何状态。
- `finish_route_snapshot(path, base_version)` 首次确认返回 `True`、重试 `False`；
  确认后释放快照保留行且不能再读旧快照，但装载期间积累的增量全部保留。
- `read_route_changes(path)` 只在快照确认后可用，返回最早未确认的完整批次
  `(version, entries)`，空积压返回 `None`；批次不可拆分，`version` 是该次
  `apply_plan` 实际登记的最高版本（可不连续）。重复读取不推进进度，重开
  进程后仍是同一批。
- `ack_route_changes(path, version)` 仅接受最早未确认批次：删除该批并返还
  对应额度，返回 `True`；重复确认最近已确认批次返回 `False`；其他版本抛
  `RouteFeedState`。确认快照本身不消耗增量额度。

### 提交语义、额度与资源边界

通道开启后，执行器在每次 `apply_plan` 执行待登记阶段**之前**安装仅存在于
该连接的 TEMP 捕获触发器（不在目录上建持久触发器），只记录本批实际触及的
id 及其计划前取值；全部阶段成功后在 SQLite 内做集合式净变化比较，中间状态
（包括同一 id 被多个阶段反复改写、改回原值、出现后又删除）一律不外发。净
变化、批次行、额度更新与迁移账本在**同一次提交**落盘：净变化条数超过当前
可用额度时抛 `RouteFeedFull(needed, available)`，本批全部 SQL、账本与通道
状态一起回退，绝不发部分批次、不跳过积压、不自行确认。无净变化、准确重放
或失败的调用不占额度、不生成批次；额度为零时净零批次仍可提交。

`preview_plan` 按同一份事务状态重新判定增量与额度：会登记的版本列表正常
返回且不留任何改动；若同样超额则同样抛 `RouteFeedFull`。额度只在确认事务
提交后返还，读取、预演、重开进程、确认快照、计划重放都不返还。应用 SQL
无法读写任何 `_route_feed_*` 内部对象（执行期授权器拦截）。

持久通道数据至多为一份切点快照（确认后即删）加未确认批次与单行进度元数据；
发布只对实际触及的 id 和有界通道状态做额外工作，不扫描未触及目录。各调用
新增 Python 峰值分配（`tracemalloc`，按调用快照差值计）远低于 12 MiB；
60,000 行、每行 320 字节目录下的开通、分页（页大小 7/256）、发布与预演均有
测试覆盖。未开通通道的库保持原有行为，不创建任何通道对象。

## SQLite 副本群的维护批次

`replica_waves.py` 在同一套迁移计划之上安排维护批次：同批节点从开批到收批
都不接读流量，读席位由留在批次外、版本兼容且互不相同的节点独享承接。批次
选择把"席位匹配 + 全批/故障域额度"一次性建模成内存最小费用流，精确求出
节点最多、同规模下节点名升序字典序最小的批次，不枚举候选幂集；开批只对每个
节点读取一次账本，不写任何数据库。

```python
from replica_waves import (
    Replica, ReadSlot, WaveUnavailable, open_replica_wave,
)

replicas = [
    Replica("a", "/srv/a.db", "east"),
    Replica("c", "/srv/c.db", "west"),
]
slots = [
    # 席位 X 只能访问 a/c，且只接受 schema v1..v2
    ReadSlot("X", {"a", "c"}, min_version=1, max_version=2,
             sql="SELECT value FROM state WHERE id=1"),
]
try:
    with open_replica_wave(
        replicas,
        plan,                         # 从 1 连续编号的完整计划，目标为末版本
        slots,
        candidates=["a", "c"],       # 允许进本批的节点（可空）
        limit=2,                     # 全批下线上限
        zone_limits={"east": 1, "west": 1},  # 必须恰好覆盖所有故障域
    ) as wave:
        print(wave.selected)         # 升序节点名元组；开批本身不迁移
        node, value = wave.read("X")  # 每次在实际库上执行，读最新已提交数据
        while (name := wave.advance()) is not None:
            print("migrated", name)  # 按 selected 顺序逐节点 apply_plan 到目标
except WaveUnavailable:
    ...  # 连空批都无法让全部席位被互不相同的在线节点承接
```

- 开批前逐个校验所有节点（含未选中、非候选节点）的账本：缺失或为空抛
  `HistoryMismatch`，账本必须是计划的精确版本/ checksum 前缀；只调度剩余阶段
  全是普通 `Migration` 的节点，在线发布等专用阶段不代劳。参数/计划形状非法
  抛 `ValueError`；即使空批也无法满足全部席位时抛 `WaveUnavailable` 且不改任何
  节点；可满足但无节点可迁时正常开空批，席位仍可读。
- 入批节点即使提前迁完也不参与本批读取；`advance()` 失败时已提交节点保留、
  失败节点按原事务规则整体回退、后续节点不启动，同批仍可读但不能再推进
  （`RuntimeError`）。关闭上下文不启动任何工作，关闭后读写均抛
  `RuntimeError`；重开按当时账本重新选择，已达目标的节点不重复执行。
- 合法规模（最多 256 节点、128 席位、8192 条访问关系）下开批与退出合计新增
  Python 峰值分配（`tracemalloc`，不含 SQLite 页缓存与输入造库）低于 24 MiB。
